# okane

Asynchronous payment pipeline in Go with Postgres state, Redis queues, and a mock provider.

## Architecture

```mermaid
flowchart TD
    Client -->|POST /payments| API["API Server"]
    Client -->|"GET /payments/:id"| API

    API -->|insert payment| PG[("Postgres")]
    API -->|read payment| PG
    API -->|LPUSH| Pending["payments:pending"]

    Pending -->|BLMOVE| WorkerPool["Worker Pool"]
    WorkerPool -->|move to| Processing["payments:processing"]
    WorkerPool -->|provider call| Provider["Mock Provider"]
    WorkerPool -->|success/failed| PG
    WorkerPool -->|retryable failure| Delayed["payments:delayed"]
    WorkerPool -->|attempts >= 8| Dead["payments:dead"]

    Delayed -->|poll due jobs| Retry["Retry Worker"]
    Retry -->|LPUSH| Pending

    Processing -->|scan stale jobs| Reaper["Reaper Worker"]
    Reaper -->|LPUSH| Pending
  ```

## How it works

1. POST /payments inserts a payment row and pushes the ID to payments:pending
2. Workers atomically move jobs from pending to processing (BLMOVE) and record timestamp in payments:processing:times hash
3. Worker calls the mock provider and handles responses:
   - 200 OK: updates status to "success", stores provider ref, removes from processing queue
   - 503 unavailable: increments attempts, continues immediate retry loop (up to 1 retry)
   - 422 unprocessable: updates status to "failed", removes from processing queue
4. After immediate retries exhausted without success: job moved to payments:delayed with 10s backoff
5. On max attempts (8): moved to payments:dead, status set to "failed"
6. Retry worker polls payments:delayed sorted set and requeues jobs whose score (timestamp) is in the past
7. Reaper polls payments:processing every 10s, requeues anything stuck longer than 1 minute
8. Graceful shutdown with 10s timeout

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
- `failed`

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
go run ./mockprovider
```

```bash
go run .
```

```bash
curl http://localhost:8080/payments \
  -d '{"amount": 440, "idempotency_key":"demo-key-1"}' | jq
```

```bash
curl "http://localhost:8080/payments/<payment-id>" | jq
```

## Roadmap
- [ ] tests
- [ ] rate limiting
- [ ] exponential backoff
- [ ] benchmarking
- [ ] request validation
