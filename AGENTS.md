# Development Guide

## Prerequisites

All build and test commands run inside a Docker dev container. Start the environment first:

```bash
docker compose up -d
```

This brings up four services: `oracle`, `olr`, `yugabytedb`, and `dev`.

## Build

```bash
docker compose exec dev go build ./cmd/dblog
```

## Running Tests

Unit tests:

```bash
docker compose exec dev go test ./internal/... -v -count=1
```

Integration tests (require all services running):

```bash
docker compose exec dev go test ./integration/... -v -count=1 -timeout=600s -tags=integration
```

Run a specific integration test:

```bash
docker compose exec dev go test ./integration/... -v -count=1 -timeout=120s -tags=integration -run TestName
```

Makefile shortcuts: `make test-unit`, `make test-e2e`.

## Important Notes

- **Never run `go test` or `go build` on the host.** The Oracle Instant Client is only available inside the dev container, so `godror` (Oracle driver) will fail to link outside of it.
- Integration tests use build tag `integration` — always pass `-tags=integration`.
- When filtering test output, redirect to a file first, then grep the file. Do not pipe directly through grep.
