# okane

Asynchronous payment pipeline in Go with Postgres state, a pluggable queue layer backed by Redis, and a mock provider.

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

## Package layout

- [cmd/okane/main.go](/Users/ayush/Developer/okane/cmd/okane/main.go): application bootstrap and wiring
- [cmd/mockprovider/main.go](/Users/ayush/Developer/okane/cmd/mockprovider/main.go): mock payment provider
- [internal/handler/handler.go](/Users/ayush/Developer/okane/internal/handler/handler.go): HTTP handlers
- [internal/service/service.go](/Users/ayush/Developer/okane/internal/service/service.go): payment workflow and workers
- [internal/store/store.go](/Users/ayush/Developer/okane/internal/store/store.go): payment store contract
- [internal/store/db/db.go](/Users/ayush/Developer/okane/internal/store/db/db.go): Postgres-backed store
- [internal/queue/queue.go](/Users/ayush/Developer/okane/internal/queue/queue.go): queue contract
- [internal/queue/redis.go](/Users/ayush/Developer/okane/internal/queue/redis.go): Redis-backed queue

## How it works

1. `POST /payments` inserts a payment row and enqueues the payment ID
2. The queue implementation moves jobs from pending to processing and records processing start times
3. Worker calls the mock provider and handles responses:
   - 200 OK: updates status to `success`, stores provider ref, removes from processing queue
   - 503 unavailable: increments attempts, continues immediate retry loop (up to 1 retry)
   - 422 unprocessable: updates status to `failed_final`, removes from processing queue
4. After immediate retries exhausted without success: status changes to `failed_retryable` and the job moves to the delayed queue with exponential backoff
5. On max attempts: the payment moves to the dead queue and status becomes `failed_final`
6. Retry worker polls delayed jobs and requeues payments whose retry time has arrived
7. Reaper polls in-flight jobs every 10s and requeues anything stuck longer than 1 minute
8. Graceful shutdown

## Data model
 [schema.sql](/Users/ayush/Developer/okane/schema.sql)

## Queue layout

- `payments:pending`
- `payments:processing`
- `payments:processing:times`
- `payments:delayed`
- `payments:dead`

## Payment state

- `pending`
- `processing`
- `success`
- `failed_retryable`
- `failed_final`

## API

### `POST /payments`

```json
{
  "amount": 440,
  "idempotency_key": "demo-key-1"
}
```

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

```json
{
  "payment": {
    "id": "7d6adb1e-6627-44ed-a544-0e75d21ef09d",
    "amount": 440,
    "status": "success",
    "idempotency_key": "demo-key-1",
    "provider_ref": "provider-ref-value",
    "attempts": 1,
    "created_at": "2026-03-28T18:40:57.436474+05:30",
    "updated_at": "2026-03-28T18:40:57.454445+05:30"
  }
}
```

## Configuration

- `DATABASE_URL`
- `REDIS_ADDR`
- `PROVIDER_BASE_URL`
- `PORT`
- `MOCK_PROVIDER_PORT`

```env
PORT=8080
DATABASE_URL=postgresql://ayush:ayush@localhost:5432/okanedb
REDIS_ADDR=localhost:6379
PROVIDER_BASE_URL=http://localhost:3000
MOCK_PROVIDER_PORT=3000
```

## Execution

```bash
go run ./cmd/mockprovider
```

```bash
go run ./cmd/okane
```

```bash
curl http://localhost:8080/payments \
  -d '{"amount": 440, "idempotency_key":"demo-key-1"}' | jq
```

```bash
curl "http://localhost:8080/payments/<payment-id>" | jq
```

## Roadmap
- [x] exponential backoff
- [ ] tests
- [ ] rate limiting
- [ ] benchmarking
- [ ] request validation
