# orbit-message-bus

[![CI](https://github.com/manovaspace/orbit-message-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/manovaspace/orbit-message-bus/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

Go library: NATS JetStream wrapper — subject naming, envelope enforcement, retry (max 3), and DLQ.

Part of the [Manova / Orbit](https://github.com/manovaspace) open toolkit.

## Install

```bash
go get github.com/manovaspace/orbit-message-bus@latest
```

## Usage

```go
import messagebus "github.com/manovaspace/orbit-message-bus"

bus, err := messagebus.New(messagebus.Config{
    URL:     "nats://localhost:10422",
    Source:  "orbit-auth",
    Version: "1",
})
env, _ := messagebus.NewEnvelope("auth.otp.requested", payload, "orbit-auth", "1")
_ = bus.Publish(ctx, "auth.otp.requested", env)
```

## Development

```bash
go test ./...
```

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md). Security reports: [SECURITY.md](./SECURITY.md).

## License

MIT — see [LICENSE](./LICENSE).
