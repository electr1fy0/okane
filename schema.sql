create table payments (
    id  uuid primary key default gen_random_uuid(),
    amount BIGINT NOT NULL,
    status text not null check (status in ('pending', 'processing', 'success', 'failed_retryable', 'failed_final')),
    idempotency_key text unique not null,
    provider_ref text,
    attempts int default 0,
    last_error text,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);
