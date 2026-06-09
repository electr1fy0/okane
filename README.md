# okane

An asynchronous, queue-based payment processing pipeline in Go using PostgreSQL and Redis.

## Features

- **Asynchronous Queueing**: Decouples payment submission from processing using [Asynq](https://github.com/hibiken/asynq) (a Redis-backed task queue).
- **Dynamic Priority Queue Routing**: Routes payments automatically to different queues based on the payment amount:
  - `critical` queue (high concurrency priority) for payments &ge; 10,000.
  - `low` queue for payments &lt; 1,000.
  - `default` queue for all other amounts.
- **Idempotency Guard**: Backed by a Postgres unique constraint on `idempotency_key`, preventing duplicate payment creation or processing.
- **Automatic Retries & Exponential Backoff**: Uses Asynq's built-in retry mechanism (max 8 retries) with exponential backoff for transient provider failures (e.g., HTTP `503 Service Unavailable`).
- **Terminal Error Termination**: Immediately aborts further retries and marks the payment as `failed_final` for validation/unprocessable errors (e.g., HTTP `422 Unprocessable Entity`).
- **Manual Task Retry / Replay**: Exposes an API endpoint (`POST /v1/payments/{id}/retry`) that utilizes Asynq's `Inspector` to replay archived failed payment tasks instantly.
- **Sliding-Window Rate Limiting**: Redis sorted sets-based sliding-window rate limiter middleware guarding the HTTP API.
- **Graceful Shutdown**: Utilizes `errgroup` and signal handling to drain HTTP connections and safely shut down the Asynq worker server on SIGINT/SIGTERM.
- **HTTP Request Logger Middleware**: Traces and logs incoming HTTP requests (at `DEBUG` level) and their processing outcomes including response status code, execution duration, and payload size (at `INFO` level).
- **Comprehensive Debug Logging**: Core pipeline transitions, provider client requests/responses, and Asynq task enqueuing/rescheduling operations are fully traceable via configurable structured log levels (`LOG_LEVEL=DEBUG`).
- **CI Pipeline**: GitHub Actions workflow that builds, sets up a live PostgreSQL container, and runs the entire test suite.

---

## Architecture

```mermaid
flowchart TD
    Client -->|POST /v1/payments| API["API Server"]
    Client -->|"GET /v1/payments/{id}"| API
    Client -->|"POST /v1/payments/{id}/retry"| API

    API -->|insert/read payment| PG[("Postgres")]
    
    API -->|enqueue task| ClientQ["Asynq Client"]
    ClientQ -->|route by amount| Redis[("Redis")]

    subgraph Redis Queues
        critical["critical (weight 6)"]
        default["default (weight 3)"]
        low["low (weight 1)"]
    end
    
    Redis -.-> critical
    Redis -.-> default
    Redis -.-> low

    WorkerPool["Worker Pool (Asynq Server)"] -->|poll by weight| Redis
    WorkerPool -->|update status| PG
    WorkerPool -->|execute payment| Provider["Mock Provider"]
    
    %% Retry paths
    WorkerPool -->|transient error / retryable| Redis
    WorkerPool -->|max retries exceeded| Archived["Archived Tasks"]
    
    API -->|manual retry via Inspector| Archived
    Archived -.->|RunTask| Redis
```

### Queue Workflow

1. **API Server**: Validates the payload. Inserts a new payment as `pending` to Postgres. If a unique key conflict occurs on `idempotency_key`, it retrieves the existing record and returns it (without enqueuing again).
2. **Task Enqueuing**: Routes and enqueues a `payment:process` task in the appropriate Redis queue (via Asynq client) depending on the amount:
   - `amount >= 10000` &rarr; `critical`
   - `amount < 1000` &rarr; `low`
   - `1000 <= amount < 10000` &rarr; `default`
3. **Worker Server**: Concurrently polls tasks according to configured queue weights (`critical: 6`, `default: 3`, `low: 1`) to ensure higher-value payments are processed first.
4. **Provider Call**: The handler requests the external payment provider.
   - **Success (200 OK)**: Updates Postgres status to `success`, records the provider reference, and successfully completes the task.
   - **Transient Error (503 Service Unavailable / Network failure)**: Records the failure, transitions status to `failed_retryable`, and returns an error. Asynq automatically schedules a retry with exponential backoff (up to 8 attempts).
   - **Terminal Error (422 Unprocessable Entity)**: Marks status as `failed_final` in Postgres and returns `nil` to complete the task, preventing unnecessary retries.
5. **Archived / Failed Tasks**: If a task fails all 8 retry attempts, Asynq automatically moves it to the archived state. The client can trigger a manual retry via `POST /v1/payments/{id}/retry` which uses `asynq.Inspector` to move it back to the pending queue immediately.

---

## Getting Started

### Prerequisites

- Go 1.26
- Docker & Docker Compose
- Redis & PostgreSQL (if running locally without Docker)

### Run via Docker Compose

To start the database, Redis, Mock Provider, and the API server:
```bash
make up
```

To stop the stack and clean up volumes:
```bash
make down
```

### Run Locally

Create/update your local `.env` with the following variables:
```env
PORT=8080
POSTGRES_USER=okanedbuser
POSTGRES_PASSWORD=okanedbpass
POSTGRES_DB=okanedb
DATABASE_URL=postgresql://okanedbuser:okanedbpass@localhost:5432/okanedb
REDIS_ADDR=localhost:6379
PROVIDER_BASE_URL=http://localhost:3000
MOCK_PROVIDER_PORT=3000
```

Start the mock provider:
```bash
go run ./cmd/mockprovider
```

Start the API server (includes both HTTP endpoints and the Asynq worker server in a unified lifecycle):
```bash
go run ./cmd/okane
```

---

## API Documentation

### `POST /v1/payments`
Creates and enqueues a payment. Requires validation: `amount > 0` and a non-empty `idempotency_key`.
A `requestly.json` is also provided.

```bash
curl -i -X POST http://localhost:8080/v1/payments \
  -H "Content-Type: application/json" \
  -d '{"amount": 12500, "idempotency_key": "unique-payment-key-1"}'
```

**Response (`202 Accepted` - New Payment)**
```json
{
  "payment": {
    "id": "2d1b827e-85b4-4e3f-a631-f542289c4b7b",
    "amount": 12500,
    "status": "pending",
    "idempotency_key": "unique-payment-key-1",
    "attempts": 0,
    "created_at": "2026-06-09T22:45:00.123456+05:30",
    "updated_at": "2026-06-09T22:45:00.123456+05:30"
  },
  "created": true,
  "enqueued": true
}
```

**Response (`200 OK` - Duplicate / Idempotent Request)**
```json
{
  "payment": {
    "id": "2d1b827e-85b4-4e3f-a631-f542289c4b7b",
    "amount": 12500,
    "status": "success",
    "idempotency_key": "unique-payment-key-1",
    "provider_ref": "8ba1239c-44b2-4cd8-b0a3-d731bc99de3c",
    "attempts": 1,
    "created_at": "2026-06-09T22:45:00.123456+05:30",
    "updated_at": "2026-06-09T22:45:01.789123+05:30"
  },
  "created": false,
  "enqueued": false
}
```

### `GET /v1/payments/{id}`
Retrieves current payment status.

```bash
curl -i http://localhost:8080/v1/payments/2d1b827e-85b4-4e3f-a631-f542289c4b7b
```

**Response (`200 OK`)**
```json
{
  "payment": {
    "id": "2d1b827e-85b4-4e3f-a631-f542289c4b7b",
    "amount": 12500,
    "status": "success",
    "idempotency_key": "unique-payment-key-1",
    "provider_ref": "8ba1239c-44b2-4cd8-b0a3-d731bc99de3c",
    "attempts": 1,
    "created_at": "2026-06-09T22:45:00.123456+05:30",
    "updated_at": "2026-06-09T22:45:01.789123+05:30"
  }
}
```

### `POST /v1/payments/{id}/retry`
Manually triggers processing retry for a failed/archived payment task.

```bash
curl -i -X POST http://localhost:8080/v1/payments/2d1b827e-85b4-4e3f-a631-f542289c4b7b/retry
```

**Response (`200 OK`)**
```json
{
  "status": "queued"
}
```

### `GET /v1/health`
Health-check endpoint.

```bash
curl -i http://localhost:8080/v1/health
```

**Response (`200 OK`)**
```json
{
  "message": "don't worry about me, mate"
}
```

---

## Key Components

- [cmd/okane/main.go](file:///Users/ayush/Developer/okane/cmd/okane/main.go): Application entrypoint and dependency injection wiring.
- [cmd/mockprovider/main.go](file:///Users/ayush/Developer/okane/cmd/mockprovider/main.go): Mock payment provider server.
- [internal/payment/payment.go](file:///Users/ayush/Developer/okane/internal/payment/payment.go): Core payment types, params, and status constants.
- [internal/handler/handler.go](file:///Users/ayush/Developer/okane/internal/handler/handler.go): HTTP API handlers, validation, custom error wrapping, and routes.
- [internal/ratelimit/ratelimit.go](file:///Users/ayush/Developer/okane/internal/ratelimit/ratelimit.go): Redis sliding-window rate limiter middleware.
- [internal/service/service.go](file:///Users/ayush/Developer/okane/internal/service/service.go): Core business service enqueuing tasks, invoking the provider, and handling retries/replay.
- [internal/store/postgres.go](file:///Users/ayush/Developer/okane/internal/store/postgres.go): Postgres-backed storage implementation for payment records.
- [internal/worker/worker.go](file:///Users/ayush/Developer/okane/internal/worker/worker.go): Asynq worker server configuration and task processing handlers.

---

## Testing & Mocking

Run the test suite (uses `miniredis` for queue/limiter unit tests and a local/CI Postgres database for integration tests):
```bash
go test -v ./...
```

Mocks are generated using [mockery](https://github.com/vektra/mockery) based on [.mockery.yml](file:///Users/ayush/Developer/okane/.mockery.yml):
```bash
mockery
```

---

## Project Status

- [x] Redis-backed asynchronous queueing ([Asynq](https://github.com/hibiken/asynq))
- [x] Dynamic queue routing based on payment amount (`critical`/`default`/`low`)
- [x] Exponential backoff & automatic retries
- [x] Manual retry/replay endpoint for failed/archived tasks via Inspector
- [x] Dockerized environment
- [x] Test suite (unit & integration)
- [x] Redis sliding-window rate limiting
- [x] Request payload validation
- [x] CI pipeline (GitHub Actions)
- [ ] Benchmarking
