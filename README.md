# okane

An asynchronous payment pipeline in Go backed by PostgreSQL and Redis.

## Features

- **Asynchronous Processing**: Decouples payment creation from client requests via Redis lists.
- **Reliable Queuing**: Uses atomic operations to move jobs safely from pending to processing.
- **Idempotency Guard**: Checks for existing idempotency keys in Postgres to prevent double-processing or duplicate enqueuing.
- **Retries & Backoff**: Supports immediate retries for transient provider errors and transitions to an exponential backoff delayed queue.
- **Stale Job Reclamation**: A background reaper worker monitors the processing queue and requeues stuck jobs.
- **Rate Limiting**: Sliding-window rate limiter using Redis sorted sets.
- **Validation & Standardized Errors**: Struct validation for payment payloads (amount and idempotency keys) and global error handling.
- **Graceful Shutdown**: Uses `errgroup` and signal context to drain HTTP connections and finish in-flight worker queue tasks on SIGINT/SIGTERM.
- **CI Pipeline**: GitHub Actions workflow running tests against a live PostgreSQL container.

---

## Architecture

```mermaid
flowchart TD
    Client -->|POST /payments| API["API Server"]
    Client -->|"GET /payments/:id"| API

    API -->|insert payment| PG[("Postgres")]
    API -->|read payment| PG
    API -->|enqueue| Queue["Queue"]
    Queue --> Pending["payments:pending"]

    Pending -->|BLMOVE| WorkerPool["Worker Pool"]
    WorkerPool -->|move to| Processing["payments:processing"]
    WorkerPool -->|provider call| Provider["Mock Provider"]
    WorkerPool -->|success/failure| PG
    WorkerPool -->|retryable failure| Delayed["payments:delayed"]
    WorkerPool -->|attempts >= n| Dead["payments:dead"]

    Delayed -->|poll due jobs| Retry["Retry Worker"]
    Retry -->|LPUSH| Pending

    Processing -->|scan stale jobs| Reaper["Reaper Worker"]
    Reaper -->|LPUSH| Pending
```

### Queue Workflow

1. **API Server**: Validates payload, inserts payment as `pending` to Postgres (handling idempotency keys), and pushes the payment ID to Redis `payments:pending`.
2. **Worker Pool**: Atomically shifts jobs from pending to `payments:processing` (via `BLMOVE`).
3. **Provider Call**: Requests external mock provider.
   - **Success (200)**: Updates Postgres status to `success` and drops the job from processing.
   - **Transient Error (503)**: Performs an immediate retry. If it fails again, marks as `failed_retryable` and shifts to `payments:delayed` with exponential backoff.
   - **Terminal Error (422)**: Marks status as `failed_final` and drops the job from processing.
4. **Retry Worker**: Polls the delayed queue and pushes ready jobs back to pending.
5. **Reaper Worker**: Runs every 10s to requeue jobs stuck in `payments:processing` for longer than 1 minute.

---

## Getting Started

### Prerequisites

- Go 1.26
- Docker & Docker Compose

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

Start the API server:
```bash
go run ./cmd/okane
```

---

## API

### `POST /payments`
Enqueues a payment. Requires validation: `amount > 0` and a non-empty `idempotency_key`.

```bash
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{"amount": 440, "idempotency_key": "demo-key-1"}'
```

**Response (`202 Accepted`)**
```json
{
  "payment": {
    "id": "7d6adb1e-6627-44ed-a544-0e75d21ef09d",
    "amount": 440,
    "status": "pending",
    "idempotency_key": "demo-key-1",
    "attempts": 0,
    "created_at": "2026-03-28T18:40:57.436474+05:30",
    "updated_at": "2026-03-28T18:40:57.436474+05:30"
  },
  "created": true,
  "enqueued": true
}
```

### `GET /payments/{id}`
Retrieves payment status.

```bash
curl -i http://localhost:8080/payments/7d6adb1e-6627-44ed-a544-0e75d21ef09d
```

**Response (`200 OK`)**
```json
{
  "payment": {
    "id": "7d6adb1e-6627-44ed-a544-0e75d21ef09d",
    "amount": 440,
    "status": "success",
    "idempotency_key": "demo-key-1",
    "provider_ref": "4ea2641a-7b3b-4835-adcd-f8b8098c4b7b",
    "attempts": 1,
    "created_at": "2026-03-28T18:40:57.436474+05:30",
    "updated_at": "2026-03-28T18:40:57.454445+05:30"
  }
}
```

### `GET /health`
API health-check.

```bash
curl -i http://localhost:8080/health
```

**Response (`200 OK`)**
```
don't worry about me, mate
```

---

## Key Components

- [cmd/okane/main.go](file:///Users/ayush/Developer/okane/cmd/okane/main.go): Application entrypoint and dependency injection wiring.
- [internal/handler/handler.go](file:///Users/ayush/Developer/okane/internal/handler/handler.go): HTTP API handlers, custom error wrapper, and request validator.
- [internal/ratelimit/ratelimit.go](file:///Users/ayush/Developer/okane/internal/ratelimit/ratelimit.go): Redis sliding-window rate limiter middleware.
- [internal/service/service.go](file:///Users/ayush/Developer/okane/internal/service/service.go): Payment retry state, background workers, and reaper logic.
- [internal/store/postgres.go](file:///Users/ayush/Developer/okane/internal/store/postgres.go): Postgres-backed storage implementation.
- [internal/queue/redis.go](file:///Users/ayush/Developer/okane/internal/queue/redis.go): Redis-backed queue implementation.

---

## Testing & Mocking

Run the test suite (uses `miniredis` for queues, and falls back to a test container for database integration tests):
```bash
go test -v ./...
```

Mocks are generated using [mockery](https://github.com/vektra/mockery) based on [.mockery.yml](file:///Users/ayush/Developer/okane/.mockery.yml):
```bash
mockery
```

---

## Project Status

- [x] Exponential backoff & retry
- [x] Dockerized environment
- [x] Test suite (unit & integration)
- [x] Redis sliding-window rate limiting
- [x] Request payload validation
- [x] CI pipeline (GitHub Actions)
- [ ] Benchmarking
