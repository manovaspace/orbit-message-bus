package redisbus_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	messagebus "github.com/manovaspace/orbit-message-bus"
	"github.com/manovaspace/orbit-message-bus/bustest"
	"github.com/manovaspace/orbit-message-bus/redisbus"
)

func TestConformance(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("needs TEST_REDIS_URL")
	}

	bustest.Conformance(t, func(t *testing.T) messagebus.Bus {
		before := orbitKeys(t, url)
		bus, err := redisbus.New(url, messagebus.Config{Source: "conformance", Version: "1"})
		if err != nil {
			t.Fatalf("connect redis: %v", err)
		}
		t.Cleanup(func() {
			_ = bus.Close()
			for key := range orbitKeys(t, url) {
				if !before[key] {
					deleteKeys(t, url, key)
				}
			}
		})
		return bus
	})
}

func orbitKeys(t *testing.T, url string) map[string]bool {
	t.Helper()
	client := redisClient(t, url)
	defer func() { _ = client.Close() }()

	keys := make(map[string]bool)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	iter := client.Scan(ctx, 0, "orbit:*", 1000).Iterator()
	for iter.Next(ctx) {
		keys[iter.Val()] = true
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("scan redis keys: %v", err)
	}
	return keys
}

func deleteKeys(t *testing.T, url string, keys ...string) {
	t.Helper()
	client := redisClient(t, url)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Del(ctx, keys...).Err(); err != nil {
		t.Fatalf("delete redis keys %v: %v", keys, err)
	}
}

func redisClient(t *testing.T, url string) *redis.Client {
	t.Helper()
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	return redis.NewClient(opt)
}
