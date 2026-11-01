package transport_test

import (
	"context"
	"os"
	"reflect"
	"testing"

	messagebus "github.com/manovaspace/orbit-message-bus"
	"github.com/manovaspace/orbit-message-bus/transport"
)

func TestNewInproc(t *testing.T) {
	bus, err := transport.New(context.Background(), "inproc", messagebus.Config{}, transport.Urls{})
	if err != nil {
		t.Fatalf("new inproc: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	assertType(t, bus, "*inproc.Bus")
}

func TestNewUnknown(t *testing.T) {
	_, err := transport.New(context.Background(), "kafka", messagebus.Config{}, transport.Urls{})
	if err == nil {
		t.Fatal("unknown transport accepted")
	}
	want := `transport: unknown BUS_TRANSPORT "kafka" (want inproc, redis or nats)`
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err, want)
	}
}

func TestFromEnvDefaultsToInproc(t *testing.T) {
	t.Setenv("BUS_TRANSPORT", "")
	t.Setenv("BUS_REDIS_URL", "")
	t.Setenv("BUS_NATS_URL", "")

	bus, err := transport.FromEnv(context.Background(), messagebus.Config{})
	if err != nil {
		t.Fatalf("from env: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	assertType(t, bus, "*inproc.Bus")
}

func TestNewRedis(t *testing.T) {
	url := redisURL(t)
	bus, err := transport.New(context.Background(), "redis", messagebus.Config{}, transport.Urls{Redis: url})
	if err != nil {
		t.Fatalf("new redis: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	assertType(t, bus, "*redisbus.Bus")
}

func TestFromEnvRedis(t *testing.T) {
	url := redisURL(t)
	t.Setenv("BUS_TRANSPORT", "redis")
	t.Setenv("BUS_REDIS_URL", url)

	bus, err := transport.FromEnv(context.Background(), messagebus.Config{})
	if err != nil {
		t.Fatalf("from env: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	assertType(t, bus, "*redisbus.Bus")
}

func TestFromEnvRedisNeedsURL(t *testing.T) {
	t.Setenv("BUS_TRANSPORT", "redis")
	t.Setenv("BUS_REDIS_URL", "")

	if _, err := transport.FromEnv(context.Background(), messagebus.Config{}); err == nil {
		t.Fatal("redis transport without BUS_REDIS_URL succeeded")
	}
}

func TestNewNATS(t *testing.T) {
	url := natsURL(t)
	bus, err := transport.New(context.Background(), "nats", messagebus.Config{URL: url}, transport.Urls{})
	if err != nil {
		t.Fatalf("new nats: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	assertType(t, bus, "*messagebus.natsBus")
}

func TestFromEnvNATS(t *testing.T) {
	url := natsURL(t)
	t.Setenv("BUS_TRANSPORT", "nats")
	t.Setenv("BUS_NATS_URL", url)

	bus, err := transport.FromEnv(context.Background(), messagebus.Config{})
	if err != nil {
		t.Fatalf("from env: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	assertType(t, bus, "*messagebus.natsBus")
}

func redisURL(t *testing.T) string {
	t.Helper()
	url := firstNonEmpty(os.Getenv("BUS_REDIS_URL"), os.Getenv("TEST_REDIS_URL"))
	if url == "" {
		t.Skip("needs BUS_REDIS_URL")
	}
	return url
}

func natsURL(t *testing.T) string {
	t.Helper()
	url := firstNonEmpty(os.Getenv("BUS_NATS_URL"), os.Getenv("TEST_NATS_URL"))
	if url == "" {
		t.Skip("needs BUS_NATS_URL")
	}
	return url
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func assertType(t *testing.T, bus messagebus.Bus, want string) {
	t.Helper()
	if got := reflect.TypeOf(bus).String(); got != want {
		t.Fatalf("bus type = %q, want %q", got, want)
	}
}
