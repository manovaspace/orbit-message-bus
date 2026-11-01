# orbit-message-bus

[![CI](https://github.com/manovaspace/orbit-message-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/manovaspace/orbit-message-bus/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

Go library: one `messagebus.Bus` port with three transports — in-memory, Redis Streams and NATS JetStream — plus subject naming, envelope enforcement, retry (max 3) and DLQ.

Part of the [Manova / Orbit](https://github.com/manovaspace) open toolkit.

## Install

```bash
go get github.com/manovaspace/orbit-message-bus@latest
```

## Transports

| `BUS_TRANSPORT` | Package | Endpoint | Use |
| --- | --- | --- | --- |
| `inproc` (default) | `inproc` | none | single-host projects |
| `redis` | `redisbus` | `BUS_REDIS_URL` | light multi-process setups |
| `nats` | root `messagebus` | `BUS_NATS_URL`, else `Config.URL` | large deployments |

A service holds only the `messagebus.Bus` port and picks the transport at start-up through `transport.FromEnv`. `transport.New(ctx, name, cfg, transport.Urls{Redis: url})` selects one explicitly.

## Usage

```go
import (
    messagebus "github.com/manovaspace/orbit-message-bus"
    "github.com/manovaspace/orbit-message-bus/transport"
)

bus, err := transport.FromEnv(ctx, messagebus.Config{
    URL:     "nats://localhost:10422", // used when BUS_NATS_URL is unset
    Source:  "orbit-auth",
    Version: "1",
})
env, _ := messagebus.NewEnvelope("auth.otp.requested", payload, "orbit-auth", "1")
_ = bus.Publish(ctx, "auth.otp.requested", env)
```

NATS keeps its direct constructor:

```go
bus, err := messagebus.New(messagebus.Config{URL: "nats://localhost:10422"})
```

## Postgres outbox

Events that must not be lost are written in the same transaction as the change that caused them, then relayed to the configured `messagebus.Bus`:

```go
import "github.com/manovaspace/orbit-message-bus/outbox"

tx, err := pool.Begin(ctx)
// ... write the domain change with tx ...
env, _ := messagebus.NewEnvelope("auth.user.created", payload, "orbit-auth", "1")
err = outbox.Insert(ctx, tx, "auth.user.created", env)
err = tx.Commit(ctx)

relay := &outbox.Relay{Pool: pool, Bus: bus} // BatchSize 50, Interval 500ms by default
go relay.Run(ctx)
```

`outbox.MigrationSQL` is a goose migration (Up and Down). Copy it into the service's embedded migrations; each service keeps the outbox table in its own schema.

Delivery is **at-least-once**. A relay publishes a row and marks it published in one transaction, but a crash between the publish and the commit re-publishes the row, and a retry after a broker error may duplicate a message the broker already accepted. Handlers must be idempotent; the envelope `id` is the idempotency key.

`Relay.RunOnce` publishes pending rows in id order and returns how many it published. It stops the batch at the first publish failure, records `attempts` and `last_error` on that row and leaves it pending for the next poll, so order is preserved. `Relay.Run` polls until its context is cancelled and backs off after errors. Several relays may run at once: pending rows are locked with `FOR UPDATE SKIP LOCKED`, so scaling out does not duplicate a published row.

## Event schema contract

`bustest.CheckEventSchema` pins the JSON shape of the events a service publishes. It compares the struct's JSON tags with a committed file:

```go
func TestUserCreatedSchema(t *testing.T) {
    bustest.CheckEventSchema(t, auth.UserCreated{}, "testdata/user.created.schema.json")
}
```

The file is `{"fields": {"<json name>": "<kind>"}}`, indented with sorted keys. Kinds are `string`, `number`, `bool`, `object` and `array`; `time.Time` counts as `string`, `json.RawMessage` as `object`, nested structs expand to `"parent.child"` keys, `json:"-"` fields are skipped and `omitempty` fields carry a trailing `?`. Removing a field or changing its kind fails as a breaking change; adding a field fails as an additive change until the file is refreshed:

```bash
UPDATE_EVENT_SCHEMAS=1 go test ./...
```

That rewrites the committed schema files (and creates a missing one) and passes. The library's own envelope is pinned by `testdata/envelope.schema.json`.

## Package layout

- root `messagebus` — port, envelope, configuration, validation, retry, DLQ, NATS JetStream implementation.
- `inproc/` — in-memory bus for a single host.
- `redisbus/` — Redis Streams bus: stream `orbit:<subject>`, consumer group `orbit-<sanitized subject>`, `XAUTOCLAIM` redelivery, DLQ stream `orbit:<subject>.dlq`.
- `transport/` — factory and environment configuration.
- `outbox/` — Postgres transactional outbox: `Insert`, `MigrationSQL`, `Relay`.
- `bustest/` — shared conformance suite, `UniqueDomain` and `CheckEventSchema`.

## Development

```bash
gofmt -l .
go vet ./...
go test ./... -race -count=1
```

The NATS and Redis conformance tests skip when their URL is unset. To run the full suite against local dev servers:

```bash
TEST_NATS_URL=nats://localhost:10422 \
TEST_REDIS_URL=redis://localhost:6379/15 \
go test ./... -race -count=1 -v
```

Each conformance run uses a random domain and removes the streams, consumers, groups and keys it created.

The outbox tests skip unless `TEST_DATABASE_URL` is set; each test creates and drops its own random schema:

```bash
TEST_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/test?sslmode=disable' \
go test ./outbox/ -race -count=1 -v
```

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md). Security reports: [SECURITY.md](./SECURITY.md).

## License

MIT — see [LICENSE](./LICENSE).
