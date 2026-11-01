// Package transport selects a messagebus.Bus implementation from configuration.
//
// It is the only package that imports every transport, so services depend on
// messagebus.Bus and pick the backend at start-up through FromEnv.
package transport

import (
	"context"
	"errors"
	"fmt"
	"os"

	messagebus "github.com/manovaspace/orbit-message-bus"
	"github.com/manovaspace/orbit-message-bus/inproc"
	"github.com/manovaspace/orbit-message-bus/redisbus"
)

// Urls holds transport endpoints that do not live in messagebus.Config.
// NATS keeps its URL in Config.URL.
type Urls struct {
	Redis string
}

// New returns the Bus for name: "inproc", "redis" or "nats".
func New(ctx context.Context, name string, cfg messagebus.Config, urls Urls) (messagebus.Bus, error) {
	_ = ctx // reserved for transports that need connection cancellation
	switch name {
	case "inproc":
		return inproc.New(cfg), nil
	case "redis":
		return redisbus.New(urls.Redis, cfg)
	case "nats":
		return messagebus.New(cfg)
	default:
		return nil, fmt.Errorf("transport: unknown BUS_TRANSPORT %q (want inproc, redis or nats)", name)
	}
}

// FromEnv builds the Bus selected by BUS_TRANSPORT (default "inproc").
// Redis reads BUS_REDIS_URL; NATS reads BUS_NATS_URL and falls back to
// cfg.URL when it is unset.
func FromEnv(ctx context.Context, cfg messagebus.Config) (messagebus.Bus, error) {
	name := os.Getenv("BUS_TRANSPORT")
	if name == "" {
		name = "inproc"
	}
	urls := Urls{Redis: os.Getenv("BUS_REDIS_URL")}
	if name == "redis" && urls.Redis == "" {
		return nil, errors.New("transport: BUS_REDIS_URL is required when BUS_TRANSPORT=redis")
	}
	if name == "nats" {
		if url := os.Getenv("BUS_NATS_URL"); url != "" {
			cfg.URL = url
		}
	}
	return New(ctx, name, cfg, urls)
}
