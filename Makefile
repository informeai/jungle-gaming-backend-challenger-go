.PHONY: up deps down build test test-race vet fmt integration integration-race migrate-up migrate-down demo

up:            ## full stack: postgres, keycloak, localstack, migrations and 3 API instances
	docker compose up --build -d

deps:          ## only the dependencies used by the integration tests
	docker compose up -d --wait postgres keycloak localstack

down:
	docker compose down -v

build:
	go build -o bin/wallet-service ./cmd/wallet-service

test:          ## unit tests (no infrastructure required)
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./... && go vet -tags integration ./...

fmt:
	gofmt -l -w .

integration:   ## requires `make deps`
	go test -tags integration -count=1 -v ./test/integration/...

integration-race:
	go test -race -tags integration -count=1 ./test/integration/...

migrate-up:
	MIGRATIONS_DATABASE_URL=postgres://postgres:postgres@localhost:5432/wallet?sslmode=disable go run ./cmd/wallet-service migrate up

migrate-down:
	MIGRATIONS_DATABASE_URL=postgres://postgres:postgres@localhost:5432/wallet?sslmode=disable go run ./cmd/wallet-service migrate down 1

demo:          ## end-to-end calls against http://localhost:8081
	scripts/demo.sh
