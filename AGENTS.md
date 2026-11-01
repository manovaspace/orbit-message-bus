# orbit-message-bus

Message-bus port and transports — subject naming, envelope enforcement, retry (max 3), DLQ. Status: `beta`. MIT — `github.com/manovaspace/orbit-message-bus`.

Services depend only on `messagebus.Bus`; `transport.FromEnv` picks the transport at start-up from configuration.

## Transports

| Transport | `BUS_TRANSPORT` | Endpoint |
| --- | --- | --- |
| root package (NATS JetStream) | `nats` | `BUS_NATS_URL`, else `Config.URL` |
| `redisbus/` (Redis Streams) | `redis` | `BUS_REDIS_URL` |
| `inproc/` (in-memory) | `inproc` (default) | none |

`transport.New(ctx, name, cfg, transport.Urls{Redis: ...})` selects explicitly; `transport.FromEnv(ctx, cfg)` reads the three environment variables.

## Layout

- root `messagebus` — `Bus`, `Envelope`, `Config`, validation, retry and DLQ; NATS implementation in `jetstream.go`. It does not import the transport packages.
- `inproc/` — in-memory `Bus` (`inproc.New(cfg)`), one dispatcher per subscription, no network.
- `redisbus/` — Redis Streams `Bus` (`redisbus.New(url, cfg)`); stream `orbit:<subject>`, group `orbit-<sanitized subject>`, acks, `XAUTOCLAIM` redelivery, DLQ stream `orbit:<subject>.dlq`.
- `transport/` — factory for `inproc`, `redis` and `nats`; the only package importing all three.
- `bustest/` — shared conformance suite (`bustest.Conformance`) and `bustest.UniqueDomain`.

## Commands

```bash
gofmt -l .
go vet ./...
go test ./... -race -count=1
```

Network conformance tests skip unless their URL is set:

```bash
TEST_NATS_URL=nats://localhost:10422 \
TEST_REDIS_URL=redis://localhost:6379/15 \
go test ./... -race -count=1 -v
```

Each run uses `bustest.UniqueDomain()` and deletes the streams, consumers, groups and Redis keys it created.

Local NATS: `nats://localhost:10422` (ADR-006).
