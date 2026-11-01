// Package bustest holds the shared conformance suite for messagebus.Bus
// implementations.
package bustest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	messagebus "github.com/manovaspace/orbit-message-bus"
)

// waitTimeout bounds every receive in the conformance suite. Network-backed
// transports share dev servers, so be generous.
const waitTimeout = 10 * time.Second

// UniqueDomain returns a random lowercase domain such as "conf1a2b3c4d".
// Tests use it so concurrent runs never collide on a shared NATS or Redis.
func UniqueDomain() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("conf%08x", time.Now().UnixNano()&0xffffffff)
	}
	return "conf" + hex.EncodeToString(b[:])
}

// Conformance runs the shared Bus behaviour suite against any implementation.
//
// newBus must return a connected Bus that uses the default retry cap
// (messagebus.DefaultMaxAttempts); the suite asserts the exact call count.
// The suite closes every bus it creates.
func Conformance(t *testing.T, newBus func(t *testing.T) messagebus.Bus) {
	t.Helper()

	t.Run("publish subscribe round trip", func(t *testing.T) {
		bus := newBus(t)
		t.Cleanup(func() { _ = bus.Close() })

		ctx := context.Background()
		subject := UniqueDomain() + ".thing.happened"
		received := make(chan messagebus.Envelope, 1)
		if err := bus.Subscribe(ctx, subject, func(_ context.Context, env messagebus.Envelope) error {
			received <- env
			return nil
		}); err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		want := mustEnvelope(t, subject, `{"n":1}`)
		if err := bus.Publish(ctx, subject, want); err != nil {
			t.Fatalf("publish: %v", err)
		}
		got := receive(t, received)
		assertSameEnvelope(t, want, got)
	})

	t.Run("publish rejects invalid envelope and subject", func(t *testing.T) {
		bus := newBus(t)
		t.Cleanup(func() { _ = bus.Close() })

		ctx := context.Background()
		subject := UniqueDomain() + ".thing.happened"
		received := make(chan messagebus.Envelope, 4)
		if err := bus.Subscribe(ctx, subject, func(_ context.Context, env messagebus.Envelope) error {
			received <- env
			return nil
		}); err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		invalid := mustEnvelope(t, subject, `{"n":1}`)
		invalid.ID = ""
		if err := bus.Publish(ctx, subject, invalid); err == nil {
			t.Fatal("publish accepted an invalid envelope")
		} else if !messagebus.IsValidationError(err) {
			t.Fatalf("invalid envelope error is not a validation error: %v", err)
		}

		valid := mustEnvelope(t, subject, `{"n":2}`)
		if err := bus.Publish(ctx, "bad", valid); err == nil {
			t.Fatal("publish accepted an invalid subject")
		} else if !messagebus.IsValidationError(err) {
			t.Fatalf("invalid subject error is not a validation error: %v", err)
		}

		probe := mustEnvelope(t, subject, `{"n":3}`)
		if err := bus.Publish(ctx, subject, probe); err != nil {
			t.Fatalf("publish probe: %v", err)
		}
		got := receive(t, received)
		if got.ID != probe.ID {
			t.Fatalf("delivered envelope %q before the probe %q: invalid publish was not rejected", got.ID, probe.ID)
		}
	})

	t.Run("handler errors retry then dead letter", func(t *testing.T) {
		bus := newBus(t)
		t.Cleanup(func() { _ = bus.Close() })

		ctx := context.Background()
		subject := UniqueDomain() + ".work.failed"
		dlq := messagebus.DLQSubject(subject)
		dead := make(chan messagebus.Envelope, 1)
		if err := bus.Subscribe(ctx, dlq, func(_ context.Context, env messagebus.Envelope) error {
			dead <- env
			return nil
		}); err != nil {
			t.Fatalf("subscribe dlq: %v", err)
		}

		var calls atomic.Int32
		if err := bus.Subscribe(ctx, subject, func(_ context.Context, _ messagebus.Envelope) error {
			calls.Add(1)
			return errors.New("boom")
		}); err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		want := mustEnvelope(t, subject, `{"n":1}`)
		if err := bus.Publish(ctx, subject, want); err != nil {
			t.Fatalf("publish: %v", err)
		}

		got := receive(t, dead)
		if n := calls.Load(); n != int32(messagebus.DefaultMaxAttempts) {
			t.Fatalf("handler calls = %d, want %d", n, messagebus.DefaultMaxAttempts)
		}
		assertSameEnvelope(t, want, got)
		if got.Metadata["dlq_reason"] == "" {
			t.Fatal("dead-letter envelope has no dlq_reason metadata")
		}
	})

	t.Run("validation error skips retries", func(t *testing.T) {
		bus := newBus(t)
		t.Cleanup(func() { _ = bus.Close() })

		ctx := context.Background()
		subject := UniqueDomain() + ".work.invalid"
		dlq := messagebus.DLQSubject(subject)
		dead := make(chan messagebus.Envelope, 1)
		if err := bus.Subscribe(ctx, dlq, func(_ context.Context, env messagebus.Envelope) error {
			dead <- env
			return nil
		}); err != nil {
			t.Fatalf("subscribe dlq: %v", err)
		}

		var calls atomic.Int32
		if err := bus.Subscribe(ctx, subject, func(_ context.Context, _ messagebus.Envelope) error {
			calls.Add(1)
			return fmt.Errorf("%w: rejected", messagebus.ErrInvalidEnvelope)
		}); err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		want := mustEnvelope(t, subject, `{"n":1}`)
		if err := bus.Publish(ctx, subject, want); err != nil {
			t.Fatalf("publish: %v", err)
		}

		got := receive(t, dead)
		if n := calls.Load(); n != 1 {
			t.Fatalf("handler calls = %d, want 1", n)
		}
		assertSameEnvelope(t, want, got)
		if got.Metadata["dlq_reason"] == "" {
			t.Fatal("dead-letter envelope has no dlq_reason metadata")
		}
	})

	t.Run("subscribers on different subjects are isolated", func(t *testing.T) {
		bus := newBus(t)
		t.Cleanup(func() { _ = bus.Close() })

		ctx := context.Background()
		domain := UniqueDomain()
		subjectA := domain + ".alpha.sent"
		subjectB := domain + ".beta.sent"
		receivedA := make(chan messagebus.Envelope, 1)
		receivedB := make(chan messagebus.Envelope, 1)
		if err := bus.Subscribe(ctx, subjectA, func(_ context.Context, env messagebus.Envelope) error {
			receivedA <- env
			return nil
		}); err != nil {
			t.Fatalf("subscribe a: %v", err)
		}
		if err := bus.Subscribe(ctx, subjectB, func(_ context.Context, env messagebus.Envelope) error {
			receivedB <- env
			return nil
		}); err != nil {
			t.Fatalf("subscribe b: %v", err)
		}

		want := mustEnvelope(t, subjectA, `{"n":1}`)
		if err := bus.Publish(ctx, subjectA, want); err != nil {
			t.Fatalf("publish: %v", err)
		}
		got := receive(t, receivedA)
		assertSameEnvelope(t, want, got)

		select {
		case env := <-receivedB:
			t.Fatalf("subscriber b received %q from subject a", env.Type)
		case <-time.After(300 * time.Millisecond):
		}
	})

	t.Run("close stops delivery and is idempotent", func(t *testing.T) {
		bus := newBus(t)

		ctx := context.Background()
		subject := UniqueDomain() + ".thing.happened"
		received := make(chan messagebus.Envelope, 1)
		if err := bus.Subscribe(ctx, subject, func(_ context.Context, env messagebus.Envelope) error {
			received <- env
			return nil
		}); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		if err := bus.Publish(ctx, subject, mustEnvelope(t, subject, `{"n":1}`)); err != nil {
			t.Fatalf("publish: %v", err)
		}
		receive(t, received)

		if err := bus.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if err := bus.Close(); err != nil {
			t.Fatalf("second close: %v", err)
		}
		if err := bus.Publish(ctx, subject, mustEnvelope(t, subject, `{"n":2}`)); err == nil {
			t.Fatal("publish after close succeeded")
		}
	})

	t.Run("delivery preserves publish order", func(t *testing.T) {
		bus := newBus(t)
		t.Cleanup(func() { _ = bus.Close() })

		ctx := context.Background()
		subject := UniqueDomain() + ".thing.happened"
		received := make(chan messagebus.Envelope, 8)
		if err := bus.Subscribe(ctx, subject, func(_ context.Context, env messagebus.Envelope) error {
			received <- env
			return nil
		}); err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		const total = 5
		sent := make([]messagebus.Envelope, total)
		for i := 0; i < total; i++ {
			sent[i] = mustEnvelope(t, subject, fmt.Sprintf(`{"i":%d}`, i))
			if err := bus.Publish(ctx, subject, sent[i]); err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
		}
		for i := 0; i < total; i++ {
			got := receive(t, received)
			if got.ID != sent[i].ID {
				t.Fatalf("message %d out of order: got %q want %q", i, got.ID, sent[i].ID)
			}
		}
	})
}

func mustEnvelope(t *testing.T, subject, payload string) messagebus.Envelope {
	t.Helper()
	env, err := messagebus.NewEnvelope(subject, json.RawMessage(payload), "bustest", "1")
	if err != nil {
		t.Fatalf("new envelope: %v", err)
	}
	return env
}

func receive(t *testing.T, ch <-chan messagebus.Envelope) messagebus.Envelope {
	t.Helper()
	select {
	case env := <-ch:
		return env
	case <-time.After(waitTimeout):
		t.Fatalf("timed out after %s waiting for a message", waitTimeout)
		return messagebus.Envelope{}
	}
}

func assertSameEnvelope(t *testing.T, want, got messagebus.Envelope) {
	t.Helper()
	if got.ID != want.ID {
		t.Fatalf("id = %q, want %q", got.ID, want.ID)
	}
	if got.Type != want.Type {
		t.Fatalf("type = %q, want %q", got.Type, want.Type)
	}
	if got.CorrelationID != want.CorrelationID {
		t.Fatalf("correlation id = %q, want %q", got.CorrelationID, want.CorrelationID)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Fatalf("payload = %s, want %s", got.Payload, want.Payload)
	}
	if !got.Timestamp.Equal(want.Timestamp) {
		t.Fatalf("timestamp = %s, want %s", got.Timestamp, want.Timestamp)
	}
	// A dead-letter envelope may add dlq_reason; every other key must match.
	for k, v := range want.Metadata {
		if got.Metadata[k] != v {
			t.Fatalf("metadata[%q] = %q, want %q", k, got.Metadata[k], v)
		}
	}
	for k := range got.Metadata {
		if _, ok := want.Metadata[k]; !ok && k != "dlq_reason" {
			t.Fatalf("unexpected metadata key %q", k)
		}
	}
}
