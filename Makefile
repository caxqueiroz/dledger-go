GO          ?= go
GOLANGCI    ?= golangci-lint
BIN_DIR     := bin

.PHONY: tools proto sqlc generate test test-integration lint serve build migrate-up migrate-down

tools:
	$(GO) install github.com/bufbuild/buf/cmd/buf@latest
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest
	$(GO) install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
	$(GO) install github.com/pressly/goose/v3/cmd/goose@latest

proto:
	buf generate

sqlc:
	sqlc generate

generate: proto sqlc

test:
	$(GO) test ./...

test-integration:
	$(GO) test -tags=integration ./...

lint:
	$(GOLANGCI) run ./...

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/server ./cmd/server
	$(GO) build -o $(BIN_DIR)/migrate ./cmd/migrate

serve: build
	./$(BIN_DIR)/server

migrate-up: build
	./$(BIN_DIR)/migrate --backend=$${BACKEND:-sqlite} up

migrate-down: build
	./$(BIN_DIR)/migrate --backend=$${BACKEND:-sqlite} down
