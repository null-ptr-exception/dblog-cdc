.PHONY: help build test-unit test-e2e proto clean

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

build: ## Build the dev Docker image
	docker compose build dev

test-unit: ## Run unit tests inside dev container
	docker compose exec dev go test ./internal/... -v -count=1

test-e2e: ## Run integration tests inside dev container
	docker compose exec dev go test ./integration/... -v -count=1 -timeout=600s -tags=integration

proto: ## Generate protobuf Go code
	mkdir -p pb
	protoc -I proto --go_out=pb --go_opt=paths=source_relative \
		OraProtoBuf.proto

clean: ## Remove build artifacts
	rm -rf pb/ dblog-cdc
