package messagebus_test

import (
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	messagebus "github.com/manovaspace/orbit-message-bus"
	"github.com/manovaspace/orbit-message-bus/bustest"
)

func TestConformanceNATS(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("needs TEST_NATS_URL")
	}

	bustest.Conformance(t, func(t *testing.T) messagebus.Bus {
		before := natsStreams(t, url)
		bus, err := messagebus.New(messagebus.Config{URL: url, Source: "conformance", Version: "1"})
		if err != nil {
			t.Fatalf("connect nats: %v", err)
		}
		t.Cleanup(func() {
			_ = bus.Close()
			for name := range natsStreams(t, url) {
				if before[name] {
					continue
				}
				deleteNATSStream(t, url, name)
			}
		})
		return bus
	})
}

func natsStreams(t *testing.T, url string) map[string]bool {
	t.Helper()
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("nats jetstream: %v", err)
	}
	names := make(map[string]bool)
	for name := range js.StreamNames() {
		names[name] = true
	}
	return names
}

func deleteNATSStream(t *testing.T, url, name string) {
	t.Helper()
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("nats jetstream: %v", err)
	}
	if err := js.DeleteStream(name); err != nil {
		t.Fatalf("delete stream %q: %v", name, err)
	}
}
