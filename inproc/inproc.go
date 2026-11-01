// Package inproc is an in-memory messagebus.Bus for single-host projects.
//
// It needs no server: each subscription owns a dispatcher goroutine and a
// queue, so messages for one subject reach a subscriber in publish order.
// Delivery, retry and dead-letter semantics match the messagebus.Bus contract.
package inproc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	messagebus "github.com/manovaspace/orbit-message-bus"
)

// queueSize bounds each subscription queue. Publishing blocks while a queue is
// full, which also applies backpressure to in-process producers.
const queueSize = 1024

var errClosed = errors.New("messagebus inproc: bus is closed")

// Bus is the in-process Bus implementation.
type Bus struct {
	cfg messagebus.Config

	mu     sync.RWMutex
	closed bool
	done   chan struct{}
	subs   map[string][]*subscription
}

type subscription struct {
	subject string
	handler messagebus.Handler
	queue   chan []byte
}

// New returns an in-memory Bus. It never touches the network.
func New(cfg messagebus.Config) messagebus.Bus {
	return &Bus{
		cfg:  cfg,
		done: make(chan struct{}),
		subs: make(map[string][]*subscription),
	}
}

func (b *Bus) Publish(ctx context.Context, subject string, env messagebus.Envelope) error {
	if err := messagebus.ValidateSubject(subject); err != nil {
		return fmt.Errorf("%w: %s", messagebus.ErrInvalidSubject, err)
	}
	if env.Type != subject {
		return fmt.Errorf("messagebus: envelope type %q must match subject %q", env.Type, subject)
	}
	data, err := env.Marshal()
	if err != nil {
		return err
	}
	return b.deliver(ctx, subject, data)
}

func (b *Bus) Subscribe(ctx context.Context, subject string, handler messagebus.Handler) error {
	if err := messagebus.ValidateSubscriptionSubject(subject); err != nil {
		return err
	}
	sub := &subscription{
		subject: subject,
		handler: handler,
		queue:   make(chan []byte, queueSize),
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return errClosed
	}
	b.subs[subject] = append(b.subs[subject], sub)
	done := b.done
	b.mu.Unlock()

	go b.dispatch(ctx, sub, done)
	return nil
}

func (b *Bus) Request(context.Context, string, messagebus.Envelope) (*messagebus.Envelope, error) {
	return nil, errors.New("messagebus: request not supported")
}

func (b *Bus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	close(b.done)
	b.subs = nil
	return nil
}

func (b *Bus) deliver(ctx context.Context, subject string, data []byte) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return errClosed
	}
	subs := b.subs[subject]
	done := b.done
	for _, sub := range subs {
		payload := append([]byte(nil), data...)
		select {
		case sub.queue <- payload:
		case <-done:
			b.mu.RUnlock()
			return errClosed
		case <-ctx.Done():
			b.mu.RUnlock()
			return ctx.Err()
		}
	}
	b.mu.RUnlock()
	return nil
}

func (b *Bus) dispatch(ctx context.Context, sub *subscription, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case data := <-sub.queue:
			select {
			case <-done:
				return
			default:
			}
			b.handle(ctx, sub, data, done)
		}
	}
}

func (b *Bus) handle(ctx context.Context, sub *subscription, data []byte, done <-chan struct{}) {
	env, err := messagebus.UnmarshalEnvelope(data)
	if err != nil {
		b.deadLetter(ctx, sub.subject, data, err.Error(), done)
		return
	}

	max := b.cfg.MaxAttemptsOrDefault()
	var lastErr error
	for attempt := 1; attempt <= max; attempt++ {
		lastErr = sub.handler(ctx, env)
		if lastErr == nil {
			return
		}
		if messagebus.IsValidationError(lastErr) {
			break
		}
		if attempt < max {
			timer := time.NewTimer(messagebus.RetryDelay(attempt))
			select {
			case <-timer.C:
			case <-done:
				timer.Stop()
				return
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
	}
	b.deadLetter(ctx, sub.subject, data, lastErr.Error(), done)
}

func (b *Bus) deadLetter(ctx context.Context, subject string, data []byte, reason string, done <-chan struct{}) {
	env, err := messagebus.UnmarshalEnvelope(data)
	if err != nil {
		// Unreadable bytes cannot be represented as an envelope, so there is
		// nothing a DLQ subscriber could parse. Drop them.
		return
	}
	metadata := make(map[string]string, len(env.Metadata)+1)
	for k, v := range env.Metadata {
		metadata[k] = v
	}
	metadata["dlq_reason"] = reason
	env.Metadata = metadata
	out, err := env.Marshal()
	if err != nil {
		return
	}
	_ = b.deliver(ctx, messagebus.DLQSubject(subject), out)
}

var _ messagebus.Bus = (*Bus)(nil)
