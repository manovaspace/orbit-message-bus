// Package outbox is a Postgres transactional outbox for messagebus.Bus.
//
// A service writes the event in the same transaction as the change that caused
// it:
//
//	tx, err := pool.Begin(ctx)
//	// ... write the domain change with tx ...
//	err = outbox.Insert(ctx, tx, "auth.user.created", env)
//	err = tx.Commit(ctx)
//
// A Relay then publishes committed rows to whatever Bus the service runs with.
// Only committed rows are visible, so a rolled-back change never publishes an
// event.
//
// Delivery is at-least-once: a relay publishes a row and marks it published in
// the same transaction, but a crash between Publish and commit re-publishes the
// row on the next run, and a relay may retry after a network error the broker
// already accepted. Handlers must therefore be idempotent; the envelope id is
// the idempotency key.
//
// MigrationSQL is a goose migration (Up and Down). A service copies it into its
// own embedded migrations and keeps one schema per service.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	messagebus "github.com/manovaspace/orbit-message-bus"
)

const (
	defaultTable     = "outbox_events"
	defaultBatchSize = 50
	defaultInterval  = 500 * time.Millisecond
	maxBackoff       = 30 * time.Second
)

// MigrationSQL creates the outbox table and its pending index. It is a goose
// migration: copy the whole constant into the service's embedded migrations.
// The package tests run the Up section against a throwaway schema.
const MigrationSQL = `-- +goose Up
CREATE TABLE outbox_events (
    id bigserial PRIMARY KEY,
    subject text NOT NULL,
    envelope jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    attempts int NOT NULL DEFAULT 0,
    last_error text
);

CREATE INDEX outbox_events_pending_idx ON outbox_events (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox_events;
`

// Insert writes one outbox row inside the caller's transaction. The subject and
// envelope are validated before the write and produce the same error types as
// Bus.Publish (ErrInvalidSubject, ErrInvalidEnvelope).
func Insert(ctx context.Context, tx pgx.Tx, subject string, env messagebus.Envelope) error {
	if err := messagebus.ValidateSubject(subject); err != nil {
		return fmt.Errorf("%w: %v", messagebus.ErrInvalidSubject, err)
	}
	if err := env.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("%w: %v", messagebus.ErrInvalidEnvelope, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO `+defaultTable+` (subject, envelope) VALUES ($1, $2)`,
		subject, data,
	); err != nil {
		return fmt.Errorf("outbox: insert: %w", err)
	}
	return nil
}

// Relay publishes committed outbox rows through Bus.
//
// Several relays may run at once: each transaction locks its rows with FOR
// UPDATE SKIP LOCKED, so a pending row is relayed by one relay at a time.
type Relay struct {
	Pool      *pgxpool.Pool
	Bus       messagebus.Bus
	BatchSize int           // default 50
	Interval  time.Duration // default 500ms between empty polls
	Table     string        // default "outbox_events"; a plain SQL identifier
}

// RunOnce relays up to BatchSize pending rows in id order and returns how many
// it published. A publish failure stops the batch so order is preserved; that
// row's attempts and last_error are recorded, the transaction commits the
// successes, and the error is returned. RunOnce never rewrites a published row,
// so a retry after such an error does not re-publish the rows already committed.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	table, err := r.tableName()
	if err != nil {
		return 0, err
	}
	if r.Pool == nil {
		return 0, errors.New("outbox: relay needs a Pool")
	}
	if r.Bus == nil {
		return 0, errors.New("outbox: relay needs a Bus")
	}
	limit := r.BatchSize
	if limit <= 0 {
		limit = defaultBatchSize
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("outbox: begin: %w", err)
	}
	// Rollback after a successful commit is a no-op that reports ErrTxClosed.
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx,
		`SELECT id, subject, envelope FROM `+table+
			` WHERE published_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED LIMIT $1`,
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf("outbox: select pending: %w", err)
	}
	type pendingRow struct {
		id       int64
		subject  string
		envelope []byte
	}
	var pending []pendingRow
	for rows.Next() {
		var p pendingRow
		if err := rows.Scan(&p.id, &p.subject, &p.envelope); err != nil {
			rows.Close()
			return 0, fmt.Errorf("outbox: scan pending: %w", err)
		}
		pending = append(pending, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, fmt.Errorf("outbox: select pending: %w", err)
	}

	published := make([]int64, 0, len(pending))
	var (
		failedID   int64
		publishErr error
	)
	for _, p := range pending {
		env, err := messagebus.UnmarshalEnvelope(p.envelope)
		if err == nil {
			err = r.Bus.Publish(ctx, p.subject, env)
		}
		if err != nil {
			failedID = p.id
			publishErr = err
			break
		}
		published = append(published, p.id)
	}

	if len(published) > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE `+table+` SET published_at = now() WHERE id = ANY($1)`,
			published,
		); err != nil {
			return 0, fmt.Errorf("outbox: mark published: %w", err)
		}
	}
	if publishErr != nil {
		if _, err := tx.Exec(ctx,
			`UPDATE `+table+` SET attempts = attempts + 1, last_error = $2 WHERE id = $1`,
			failedID, publishErr.Error(),
		); err != nil {
			return 0, fmt.Errorf("outbox: record failure for event %d: %w", failedID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("outbox: commit: %w", err)
	}
	if publishErr != nil {
		return len(published), fmt.Errorf("outbox: publish event %d: %w", failedID, publishErr)
	}
	return len(published), nil
}

// Run calls RunOnce until ctx is cancelled. It sleeps Interval when nothing was
// pending and backs off (doubling from Interval up to 30s) after errors. Run
// returns nil when ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	var backoff time.Duration
	for {
		if ctx.Err() != nil {
			return nil
		}
		n, err := r.RunOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}

		var wait time.Duration
		switch {
		case err != nil:
			backoff = nextBackoff(backoff, r.interval())
			wait = backoff
		case n == 0:
			backoff = 0
			wait = r.interval()
		default:
			backoff = 0
			continue
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func nextBackoff(previous, base time.Duration) time.Duration {
	if previous < base {
		return base
	}
	next := previous * 2
	if next > maxBackoff {
		return maxBackoff
	}
	return next
}

func (r *Relay) interval() time.Duration {
	if r.Interval <= 0 {
		return defaultInterval
	}
	return r.Interval
}

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (r *Relay) tableName() (string, error) {
	table := r.Table
	if table == "" {
		table = defaultTable
	}
	if !identifierPattern.MatchString(table) {
		return "", fmt.Errorf("outbox: invalid table %q: want a plain SQL identifier", table)
	}
	return table, nil
}
