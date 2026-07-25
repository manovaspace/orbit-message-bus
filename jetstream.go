package messagebus

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
)

type natsBus struct {
	nc            *nats.Conn
	js            nats.JetStreamContext
	cfg           Config
	subscriptions []*nats.Subscription
	mu            sync.Mutex
}

// New returns a NATS JetStream-backed Bus.
func New(cfg Config) (Bus, error) {
	if cfg.URL == "" {
		cfg.URL = nats.DefaultURL
	}
	nc, err := nats.Connect(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("messagebus nats: connect: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("messagebus nats: jetstream: %w", err)
	}
	return &natsBus{nc: nc, js: js, cfg: cfg}, nil
}

func (b *natsBus) Publish(ctx context.Context, subject string, env Envelope) error {
	if err := ValidateSubject(subject); err != nil {
		return err
	}
	if env.Type != subject {
		return fmt.Errorf("messagebus: envelope type %q must match subject %q", env.Type, subject)
	}
	data, err := env.Marshal()
	if err != nil {
		return err
	}
	msg := &nats.Msg{Subject: subject, Data: data, Header: nats.Header{}}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier(msg.Header))
	_, err = b.js.PublishMsg(msg, nats.Context(ctx))
	return err
}

func (b *natsBus) Subscribe(ctx context.Context, subject string, handler Handler) error {
	if err := ValidateSubject(subject); err != nil {
		return err
	}
	if err := b.ensureStream(subject); err != nil {
		return err
	}
	sub, err := b.js.Subscribe(subject, func(msg *nats.Msg) {
		b.handleMessage(ctx, subject, msg, handler)
	}, nats.ManualAck(), nats.AckExplicit(), nats.Durable(durableName(subject)))
	if err != nil {
		return fmt.Errorf("messagebus nats: subscribe: %w", err)
	}
	b.mu.Lock()
	b.subscriptions = append(b.subscriptions, sub)
	b.mu.Unlock()
	return nil
}

func (b *natsBus) handleMessage(ctx context.Context, subject string, msg *nats.Msg, handler Handler) {
	env, err := UnmarshalEnvelope(msg.Data)
	if err != nil {
		_ = b.publishDLQ(subject, msg.Data, err.Error())
		_ = msg.Ack()
		return
	}

	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier(msg.Header))

	var lastErr error
	max := b.cfg.MaxAttemptsOrDefault()
	for attempt := 1; attempt <= max; attempt++ {
		lastErr = handler(ctx, env)
		if lastErr == nil {
			_ = msg.Ack()
			return
		}
		if IsValidationError(lastErr) {
			break
		}
		if attempt < max {
			delay := RetryDelay(attempt)
			if err := msg.NakWithDelay(delay); err != nil {
				time.Sleep(delay)
			} else {
				return
			}
		}
	}

	_ = b.publishDLQ(subject, msg.Data, lastErr.Error())
	_ = msg.Ack()
}

func (b *natsBus) publishDLQ(subject string, data []byte, reason string) error {
	hdr := nats.Header{}
	hdr.Set("dlq-reason", reason)
	_, err := b.js.PublishMsg(&nats.Msg{Subject: DLQSubject(subject), Data: data, Header: hdr})
	return err
}

func (b *natsBus) Request(ctx context.Context, subject string, env Envelope) (*Envelope, error) {
	return nil, fmt.Errorf("messagebus nats: Request not implemented — use gRPC for sync RPC")
}

func (b *natsBus) Close() error {
	b.mu.Lock()
	subs := b.subscriptions
	b.subscriptions = nil
	b.mu.Unlock()
	for _, sub := range subs {
		_ = sub.Unsubscribe()
	}
	b.nc.Close()
	return nil
}

func (b *natsBus) ensureStream(subject string) error {
	domain, err := DomainFromSubject(subject)
	if err != nil {
		return err
	}
	stream := domain
	if _, err := b.js.StreamInfo(stream); err == nil {
		return nil
	}
	_, err = b.js.AddStream(&nats.StreamConfig{
		Name:      stream,
		Subjects:  []string{domain + ".>"},
		Retention: nats.LimitsPolicy,
		MaxAge:    7 * 24 * time.Hour,
		MaxBytes:  1 << 30,
	})
	return err
}

func durableName(subject string) string {
	// NATS durable names cannot contain '.'.
	return "orbit-" + strings.ReplaceAll(subject, ".", "_")
}

type headerCarrier nats.Header

func (h headerCarrier) Get(key string) string {
	return nats.Header(h).Get(key)
}

func (h headerCarrier) Set(key, value string) {
	nats.Header(h).Set(key, value)
}

func (h headerCarrier) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	return keys
}

var _ Bus = (*natsBus)(nil)
