.PHONY: proto build test test-unit test-integration clean

proto:
	mkdir -p pb
	protoc -I proto --go_out=pb --go_opt=paths=source_relative \
		--go-grpc_out=pb --go-grpc_opt=paths=source_relative \
		OraProtoBuf.proto

build: proto
	go build -o dblog-cdc ./cmd/dblog

test-unit:
	go test ./internal/... -v -count=1

test-integration:
	go test ./integration/... -v -count=1 -timeout=300s -tags=integration

test: test-unit

clean:
	rm -rf pb/ dblog-cdc

testenv-up:
	docker compose -f testenv/docker-compose.yaml up -d

testenv-down:
	docker compose -f testenv/docker-compose.yaml down -v
