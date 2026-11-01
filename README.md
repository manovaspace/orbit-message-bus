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

## Package layout

- root `messagebus` — port, envelope, configuration, validation, retry, DLQ, NATS JetStream implementation.
- `inproc/` — in-memory bus for a single host.
- `redisbus/` — Redis Streams bus: stream `orbit:<subject>`, consumer group `orbit-<sanitized subject>`, `XAUTOCLAIM` redelivery, DLQ stream `orbit:<subject>.dlq`.
- `transport/` — factory and environment configuration.
- `bustest/` — shared conformance suite used by every implementation.

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

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md). Security reports: [SECURITY.md](./SECURITY.md).

## License

MIT — see [LICENSE](./LICENSE).
