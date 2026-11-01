package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	messagebus "github.com/manovaspace/orbit-message-bus"
	"github.com/manovaspace/orbit-message-bus/bustest"
	"github.com/manovaspace/orbit-message-bus/inproc"
	"github.com/manovaspace/orbit-message-bus/outbox"
)

const receiveTimeout = 5 * time.Second

// TestMigrationSQLHasUpAndDown runs without a database so the migration shape
// is still checked when the SQL tests skip.
func TestMigrationSQLHasUpAndDown(t *testing.T) {
	for _, marker := range []string{"-- +goose Up", "-- +goose Down"} {
		if !strings.Contains(outbox.MigrationSQL, marker) {
			t.Fatalf("MigrationSQL is missing %q", marker)
		}
	}
	if len(upStatements(t)) == 0 {
		t.Fatal("MigrationSQL has no Up statements")
	}
}

func TestInsertCommittedTxIsRelayedOnce(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	subject := bustest.UniqueDomain() + ".thing.happened"
	bus, received := newRecordingBus(t, subject, 4)

	env := newEnvelope(t, subject)
	tx := begin(t, pool)
	if err := outbox.Insert(ctx, tx, subject, env); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	relay := &outbox.Relay{Pool: pool, Bus: bus}
	n, err := relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if n != 1 {
		t.Fatalf("published = %d, want 1", n)
	}
	if got := receive(t, received); got.ID != env.ID {
		t.Fatalf("delivered id = %q, want %q", got.ID, env.ID)
	}

	state := onlyRow(t, pool)
	if !state.published {
		t.Fatal("row is still pending after a successful relay")
	}
	if state.attempts != 0 {
		t.Fatalf("attempts = %d, want 0", state.attempts)
	}

	n, err = relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("second run once: %v", err)
	}
	if n != 0 {
		t.Fatalf("second run published = %d, want 0 (at most once per row)", n)
	}
	expectNoDelivery(t, received)
}

func TestInsertRolledBackTxPublishesNothing(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	subject := bustest.UniqueDomain() + ".thing.happened"
	bus, received := newRecordingBus(t, subject, 1)

	tx := begin(t, pool)
	if err := outbox.Insert(ctx, tx, subject, newEnvelope(t, subject)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	relay := &outbox.Relay{Pool: pool, Bus: bus}
	n, err := relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if n != 0 {
		t.Fatalf("published = %d, want 0: a rolled-back outbox row must not exist", n)
	}
	if got := pendingCount(t, pool); got != 0 {
		t.Fatalf("pending rows = %d, want 0", got)
	}
	expectNoDelivery(t, received)
}

func TestRelayPreservesOrderWithinBatch(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	subject := bustest.UniqueDomain() + ".thing.happened"
	bus, received := newRecordingBus(t, subject, 8)

	const total = 5
	sent := make([]messagebus.Envelope, total)
	tx := begin(t, pool)
	for i := range sent {
		sent[i] = newEnvelope(t, subject)
		if err := outbox.Insert(ctx, tx, subject, sent[i]); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	relay := &outbox.Relay{Pool: pool, Bus: bus, BatchSize: total}
	n, err := relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if n != total {
		t.Fatalf("published = %d, want %d", n, total)
	}
	for i := range sent {
		if got := receive(t, received); got.ID != sent[i].ID {
			t.Fatalf("message %d out of order: got %q, want %q", i, got.ID, sent[i].ID)
		}
	}
}

func TestRelayRecordsPublishFailureAndRetries(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	subject := bustest.UniqueDomain() + ".work.failed"
	env := newEnvelope(t, subject)

	tx := begin(t, pool)
	if err := outbox.Insert(ctx, tx, subject, env); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	relay := &outbox.Relay{Pool: pool, Bus: failingBus{err: errors.New("bus is down")}}
	n, err := relay.RunOnce(ctx)
	if err == nil {
		t.Fatal("run once succeeded with a failing bus")
	}
	if n != 0 {
		t.Fatalf("published = %d, want 0", n)
	}
	if !strings.Contains(err.Error(), "bus is down") {
		t.Fatalf("error does not carry the publish failure: %v", err)
	}

	state := onlyRow(t, pool)
	if state.published {
		t.Fatal("row marked published after a failed publish")
	}
	if state.attempts != 1 {
		t.Fatalf("attempts = %d, want 1", state.attempts)
	}
	if state.lastError == nil || !strings.Contains(*state.lastError, "bus is down") {
		t.Fatalf("last_error = %v, want it to mention the publish failure", state.lastError)
	}

	bus, received := newRecordingBus(t, subject, 2)
	relay.Bus = bus
	n, err = relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("retry run once: %v", err)
	}
	if n != 1 {
		t.Fatalf("retry published = %d, want 1", n)
	}
	if got := receive(t, received); got.ID != env.ID {
		t.Fatalf("delivered id = %q, want %q", got.ID, env.ID)
	}
	state = onlyRow(t, pool)
	if !state.published {
		t.Fatal("row is still pending after a successful retry")
	}
	if state.attempts != 1 {
		t.Fatalf("attempts = %d, want the failed attempt to remain recorded", state.attempts)
	}
}

func TestRelayStopsBatchAtFirstFailure(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	subject := bustest.UniqueDomain() + ".thing.happened"
	first := newEnvelope(t, subject)
	second := newEnvelope(t, subject)
	third := newEnvelope(t, subject)

	tx := begin(t, pool)
	for _, env := range []messagebus.Envelope{first, second, third} {
		if err := outbox.Insert(ctx, tx, subject, env); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Fail on the second publish so the relay must stop before the third and
	// leave the first published.
	bus := &scriptedBus{failAt: 2, err: errors.New("second publish failed")}
	relay := &outbox.Relay{Pool: pool, Bus: bus}
	n, err := relay.RunOnce(ctx)
	if err == nil {
		t.Fatal("run once succeeded despite a mid-batch failure")
	}
	if n != 1 {
		t.Fatalf("published = %d, want 1 before the failure", n)
	}
	if got := pendingCount(t, pool); got != 2 {
		t.Fatalf("pending rows = %d, want 2", got)
	}
	states := allRows(t, pool)
	if !states[0].published {
		t.Fatal("first row was not marked published")
	}
	if states[1].published || states[1].attempts != 1 {
		t.Fatalf("failed row published=%v attempts=%d, want false/1", states[1].published, states[1].attempts)
	}
	if states[2].published || states[2].attempts != 0 {
		t.Fatalf("row after the failure published=%v attempts=%d, want false/0", states[2].published, states[2].attempts)
	}
}

func TestConcurrentRelaysPublishEachRowOnce(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	subject := bustest.UniqueDomain() + ".thing.happened"
	const total = 20
	bus, received := newRecordingBus(t, subject, total)

	sent := make([]messagebus.Envelope, total)
	tx := begin(t, pool)
	for i := range sent {
		sent[i] = newEnvelope(t, subject)
		if err := outbox.Insert(ctx, tx, subject, sent[i]); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	relayA := &outbox.Relay{Pool: pool, Bus: bus, BatchSize: 5}
	relayB := &outbox.Relay{Pool: pool, Bus: bus, BatchSize: 5}

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]int, 2)
	errs := make([]error, 2)
	for i, relay := range []*outbox.Relay{relayA, relayB} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = relay.RunOnce(ctx)
		}()
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("relay %d: %v", i, err)
		}
	}
	if results[0]+results[1] == 0 {
		t.Fatal("neither concurrent relay selected a row")
	}

	for {
		n, err := relayA.RunOnce(ctx)
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
		if n == 0 {
			break
		}
	}

	seen := make(map[string]int, total)
	for i := 0; i < total; i++ {
		got := receive(t, received)
		seen[got.ID]++
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("envelope %q delivered %d times, want 1", id, count)
		}
	}
	if len(seen) != total {
		t.Fatalf("distinct envelopes delivered = %d, want %d", len(seen), total)
	}
	if got := pendingCount(t, pool); got != 0 {
		t.Fatalf("pending rows = %d, want 0", got)
	}
	expectNoDelivery(t, received)
}

func TestInsertRejectsInvalidSubjectAndEnvelope(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	subject := bustest.UniqueDomain() + ".thing.happened"
	tx := begin(t, pool)
	t.Cleanup(func() { _ = tx.Rollback(ctx) })

	err := outbox.Insert(ctx, tx, "not-a-subject", newEnvelope(t, subject))
	if err == nil {
		t.Fatal("insert accepted an invalid subject")
	}
	if !errors.Is(err, messagebus.ErrInvalidSubject) || !messagebus.IsValidationError(err) {
		t.Fatalf("invalid subject error = %v, want ErrInvalidSubject", err)
	}

	err = outbox.Insert(ctx, tx, subject, messagebus.Envelope{})
	if err == nil {
		t.Fatal("insert accepted an invalid envelope")
	}
	if !errors.Is(err, messagebus.ErrInvalidEnvelope) || !messagebus.IsValidationError(err) {
		t.Fatalf("invalid envelope error = %v, want ErrInvalidEnvelope", err)
	}
}

func TestRelayRejectsInvalidTable(t *testing.T) {
	pool := newTestPool(t)
	bus := inproc.New(messagebus.Config{})
	t.Cleanup(func() { _ = bus.Close() })

	for _, table := range []string{"outbox_events; drop table outbox_events", "out box", "1outbox", `"outbox"`} {
		relay := &outbox.Relay{Pool: pool, Bus: bus, Table: table}
		_, err := relay.RunOnce(context.Background())
		if err == nil {
			t.Fatalf("RunOnce accepted table %q", table)
		}
		if !strings.Contains(err.Error(), "invalid table") {
			t.Fatalf("error for table %q = %v, want an invalid table error", table, err)
		}
	}
}

// --- helpers ---------------------------------------------------------------

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("needs TEST_DATABASE_URL")
	}
	ctx := context.Background()
	schema := "outbox_" + bustest.UniqueDomain()

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	// Every pooled connection works inside the test's own schema.
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		pool.Close()
	})
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	for _, stmt := range upStatements(t) {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("migrate: %v\n%s", err, stmt)
		}
	}
	return pool
}

// upStatements extracts the Up section of MigrationSQL and splits it into
// statements, which pgx sends one at a time.
func upStatements(t *testing.T) []string {
	t.Helper()
	up := outbox.MigrationSQL
	if i := strings.Index(up, "-- +goose Down"); i >= 0 {
		up = up[:i]
	}
	var b strings.Builder
	for _, line := range strings.Split(up, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "-- +goose") {
			continue
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	var stmts []string
	for _, stmt := range strings.Split(b.String(), ";") {
		if s := strings.TrimSpace(stmt); s != "" {
			stmts = append(stmts, s)
		}
	}
	return stmts
}

func newEnvelope(t *testing.T, subject string) messagebus.Envelope {
	t.Helper()
	env, err := messagebus.NewEnvelope(subject, json.RawMessage(`{"ok":true}`), "outbox-test", "1")
	if err != nil {
		t.Fatalf("new envelope: %v", err)
	}
	return env
}

func newRecordingBus(t *testing.T, subject string, capacity int) (messagebus.Bus, <-chan messagebus.Envelope) {
	t.Helper()
	bus := inproc.New(messagebus.Config{})
	t.Cleanup(func() { _ = bus.Close() })
	received := make(chan messagebus.Envelope, capacity)
	if err := bus.Subscribe(context.Background(), subject, func(_ context.Context, env messagebus.Envelope) error {
		received <- env
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	return bus, received
}

func receive(t *testing.T, ch <-chan messagebus.Envelope) messagebus.Envelope {
	t.Helper()
	select {
	case env := <-ch:
		return env
	case <-time.After(receiveTimeout):
		t.Fatalf("timed out after %s waiting for a message", receiveTimeout)
		return messagebus.Envelope{}
	}
}

func expectNoDelivery(t *testing.T, ch <-chan messagebus.Envelope) {
	t.Helper()
	select {
	case env := <-ch:
		t.Fatalf("unexpected delivery of %q", env.ID)
	case <-time.After(300 * time.Millisecond):
	}
}

func begin(t *testing.T, pool *pgxpool.Pool) pgx.Tx {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	return tx
}

func pendingCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	return n
}

type rowState struct {
	published bool
	attempts  int
	lastError *string
}

func onlyRow(t *testing.T, pool *pgxpool.Pool) rowState {
	t.Helper()
	states := allRows(t, pool)
	if len(states) != 1 {
		t.Fatalf("outbox rows = %d, want 1", len(states))
	}
	return states[0]
}

func allRows(t *testing.T, pool *pgxpool.Pool) []rowState {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT published_at IS NOT NULL, attempts, last_error FROM outbox_events ORDER BY id`)
	if err != nil {
		t.Fatalf("select outbox: %v", err)
	}
	defer rows.Close()
	var states []rowState
	for rows.Next() {
		var s rowState
		if err := rows.Scan(&s.published, &s.attempts, &s.lastError); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		states = append(states, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("select outbox: %v", err)
	}
	return states
}

// failingBus fails every publish.
type failingBus struct {
	err error
}

func (b failingBus) Publish(context.Context, string, messagebus.Envelope) error { return b.err }

func (failingBus) Subscribe(context.Context, string, messagebus.Handler) error { return nil }

func (failingBus) Request(context.Context, string, messagebus.Envelope) (*messagebus.Envelope, error) {
	return nil, errors.New("not supported")
}

func (failingBus) Close() error { return nil }

// scriptedBus fails on the nth publish (1-based).
type scriptedBus struct {
	mu     sync.Mutex
	calls  int
	failAt int
	err    error
}

func (b *scriptedBus) Publish(context.Context, string, messagebus.Envelope) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if b.calls == b.failAt {
		return b.err
	}
	return nil
}

func (b *scriptedBus) Subscribe(context.Context, string, messagebus.Handler) error { return nil }

func (b *scriptedBus) Request(context.Context, string, messagebus.Envelope) (*messagebus.Envelope, error) {
	return nil, errors.New("not supported")
}

func (b *scriptedBus) Close() error { return nil }
