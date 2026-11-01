// Package redisbus is a Redis Streams implementation of messagebus.Bus.
//
// One stream per subject ("orbit:<subject>") with one consumer group per
// subject ("orbit-<sanitized subject>"). Publishing is XADD; subscribing is
// XREADGROUP with explicit acks. Unacknowledged entries are reclaimed with
// XAUTOCLAIM after a visibility timeout, and entries that exhaust the retry
// cap are copied to "orbit:<subject>.dlq".
package redisbus

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	messagebus "github.com/manovaspace/orbit-message-bus"
)

const (
	// dataField is the stream entry field holding the marshalled envelope.
	dataField = "data"
	// reasonField is the stream entry field carrying a dead-letter reason.
	reasonField = "dlq_reason"
	// visibilityTimeout is how long an unacked entry waits before another
	// consumer (or the claim loop) may take it over.
	visibilityTimeout = 30 * time.Second
	// readBlock bounds a blocking XREADGROUP so Close can stop promptly.
	readBlock = 200 * time.Millisecond
)

var errClosed = errors.New("messagebus redis: bus is closed")

// Bus is the Redis Streams Bus implementation.
type Bus struct {
	rdb redis.UniversalClient
	cfg messagebus.Config

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	closed bool
}

type subscription struct {
	ctx      context.Context
	stream   string
	group    string
	consumer string
	handler  messagebus.Handler
}

// New connects to url and returns a Redis Streams-backed Bus.
func New(url string, cfg messagebus.Config) (messagebus.Bus, error) {
	if url == "" {
		return nil, errors.New("messagebus redis: empty redis url")
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("messagebus redis: parse url: %w", err)
	}
	rdb := redis.NewClient(opt)

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("messagebus redis: ping: %w", err)
	}

	busCtx, busCancel := context.WithCancel(context.Background())
	return &Bus{rdb: rdb, cfg: cfg, ctx: busCtx, cancel: busCancel}, nil
}

func (b *Bus) Publish(ctx context.Context, subject string, env messagebus.Envelope) error {
	if b.isClosed() {
		return errClosed
	}
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
	return b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName(subject),
		Values: []any{dataField, string(data)},
	}).Err()
}

func (b *Bus) Subscribe(ctx context.Context, subject string, handler messagebus.Handler) error {
	if b.isClosed() {
		return errClosed
	}
	if err := messagebus.ValidateSubscriptionSubject(subject); err != nil {
		return err
	}
	subCtx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		ctx:      subCtx,
		stream:   streamName(subject),
		group:    groupName(subject),
		consumer: consumerName(),
		handler:  handler,
	}

	if err := b.ensureGroup(subCtx, sub.stream, sub.group); err != nil {
		cancel()
		return err
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		cancel()
		return errClosed
	}
	b.wg.Add(2)
	b.mu.Unlock()

	go func() {
		defer b.wg.Done()
		defer cancel()
		b.readLoop(sub)
	}()
	go func() {
		defer b.wg.Done()
		b.claimLoop(sub)
	}()
	return nil
}

func (b *Bus) Request(context.Context, string, messagebus.Envelope) (*messagebus.Envelope, error) {
	return nil, errors.New("messagebus: request not supported")
}

func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()

	b.cancel()
	b.wg.Wait()
	return b.rdb.Close()
}

func (b *Bus) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

func (b *Bus) ensureGroup(ctx context.Context, stream, group string) error {
	err := b.rdb.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil && !isBusyGroup(err) {
		return fmt.Errorf("messagebus redis: create group: %w", err)
	}
	return nil
}

func (b *Bus) readLoop(sub *subscription) {
	for {
		if b.ctx.Err() != nil || sub.ctx.Err() != nil {
			return
		}
		res, err := b.rdb.XReadGroup(b.ctx, &redis.XReadGroupArgs{
			Group:    sub.group,
			Consumer: sub.consumer,
			Streams:  []string{sub.stream, ">"},
			Count:    1,
			Block:    readBlock,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || b.ctx.Err() != nil || sub.ctx.Err() != nil {
				continue
			}
			if isNoGroup(err) {
				if err := b.ensureGroup(b.ctx, sub.stream, sub.group); err != nil {
					return
				}
				continue
			}
			// Transient network errors: back off briefly, then keep reading.
			select {
			case <-time.After(100 * time.Millisecond):
			case <-b.ctx.Done():
				return
			case <-sub.ctx.Done():
				return
			}
			continue
		}
		for _, stream := range res {
			for _, msg := range stream.Messages {
				b.process(sub, msg)
			}
		}
	}
}

func (b *Bus) claimLoop(sub *subscription) {
	ticker := time.NewTicker(visibilityTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-sub.ctx.Done():
			return
		case <-ticker.C:
		}

		msgs, _, err := b.rdb.XAutoClaim(b.ctx, &redis.XAutoClaimArgs{
			Stream:   sub.stream,
			Group:    sub.group,
			Consumer: sub.consumer,
			MinIdle:  visibilityTimeout,
			Start:    "0-0",
			Count:    10,
		}).Result()
		if err != nil {
			if isNoGroup(err) {
				if err := b.ensureGroup(b.ctx, sub.stream, sub.group); err != nil {
					return
				}
			}
			continue
		}
		for _, msg := range msgs {
			b.process(sub, msg)
		}
	}
}

func (b *Bus) process(sub *subscription, msg redis.XMessage) {
	data, ok := stringField(msg.Values, dataField)
	if !ok {
		b.ack(sub, msg)
		return
	}
	env, err := messagebus.UnmarshalEnvelope([]byte(data))
	if err != nil {
		b.deadLetter(sub, msg, data, err.Error())
		return
	}

	max := b.cfg.MaxAttemptsOrDefault()
	var lastErr error
	for attempt := 1; attempt <= max; attempt++ {
		if b.ctx.Err() != nil {
			return // Close: leave the entry pending for the next process.
		}
		lastErr = sub.handler(sub.ctx, env)
		if lastErr == nil {
			b.ack(sub, msg)
			return
		}
		if messagebus.IsValidationError(lastErr) {
			break
		}
		if attempt < max {
			timer := time.NewTimer(messagebus.RetryDelay(attempt))
			select {
			case <-timer.C:
			case <-b.ctx.Done():
				timer.Stop()
				return
			case <-sub.ctx.Done():
				timer.Stop()
				return
			}
		}
	}
	b.deadLetter(sub, msg, data, lastErr.Error())
}

func (b *Bus) ack(sub *subscription, msg redis.XMessage) {
	_ = b.rdb.XAck(b.ctx, sub.stream, sub.group, msg.ID).Err()
}

func (b *Bus) deadLetter(sub *subscription, msg redis.XMessage, data, reason string) {
	if out, ok := dlqEnvelope(data, reason); ok {
		err := b.rdb.XAdd(b.ctx, &redis.XAddArgs{
			Stream: streamName(messagebus.DLQSubject(topicFromStream(sub.stream))),
			Values: []any{dataField, out, reasonField, reason},
		}).Err()
		if err != nil {
			// Leave the entry pending so it is reclaimed and retried.
			return
		}
	}
	b.ack(sub, msg)
}

// dlqEnvelope adds the failure reason to the original envelope metadata. The
// envelope keeps its id, type, payload and every original metadata key.
func dlqEnvelope(data, reason string) (string, bool) {
	env, err := messagebus.UnmarshalEnvelope([]byte(data))
	if err != nil {
		return "", false
	}
	metadata := make(map[string]string, len(env.Metadata)+1)
	for k, v := range env.Metadata {
		metadata[k] = v
	}
	metadata["dlq_reason"] = reason
	env.Metadata = metadata
	out, err := env.Marshal()
	if err != nil {
		return "", false
	}
	return string(out), true
}

func streamName(subject string) string {
	return "orbit:" + subject
}

func topicFromStream(stream string) string {
	return strings.TrimPrefix(stream, "orbit:")
}

// groupName mirrors messagebus's NATS durable naming: dots are not portable
// across Redis clients, so sanitize them.
func groupName(subject string) string {
	return "orbit-" + strings.ReplaceAll(subject, ".", "_")
}

func consumerName() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("orbit-%d-%d", os.Getpid(), time.Now().UnixNano())
	}
	return fmt.Sprintf("orbit-%d-%s", os.Getpid(), hex.EncodeToString(b[:]))
}

func stringField(values map[string]any, key string) (string, bool) {
	v, ok := values[key]
	if !ok {
		return "", false
	}
	switch typed := v.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return fmt.Sprint(typed), true
	}
}

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

func isNoGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}

var _ messagebus.Bus = (*Bus)(nil)
