# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `inproc` transport: in-memory `Bus` with per-subscription dispatch, retry, DLQ and no network.
- `redisbus` transport: Redis Streams `Bus` with consumer groups, acks, `XAUTOCLAIM` redelivery and a per-subject DLQ stream.
- `transport` package: `transport.New` and `transport.FromEnv` select `inproc`, `redis` or `nats` from `BUS_TRANSPORT`, `BUS_REDIS_URL` and `BUS_NATS_URL`.
- `bustest` package: shared `Conformance` suite and `UniqueDomain` for all implementations.
- Dead-letter subjects (`{domain}.{entity}.{action}.dlq`) are accepted by `Subscribe` on every transport; `Publish` still requires the three-part shape.
- `github.com/redis/go-redis/v9` dependency for the Redis Streams transport.
- Initial public MIT release.

### Changed

- `Publish` wraps invalid-subject errors with `ErrInvalidSubject`, so `IsValidationError` recognises them.
- NATS retries run inside the handler loop, so the handler is called exactly `MaxAttempts` times before the DLQ as the port contract requires.
- NATS dead-letter messages expose the `dlq-reason` header as `Metadata["dlq_reason"]` on the delivered envelope.
