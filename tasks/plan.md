# Retry Handling Plan

## Overview

Replace unbounded retry behavior in the Kafka consumers and define bounded failure behavior for the synchronous Order API. Keep CDC as MariaDB binlog → Debezium/Kafka Connect → Kafka; this plan does not introduce an application Kafka producer or an outbox.

## Current behavior

- Order API writes with a client-provided `Idempotency-Key`; replay with the same key and payload returns the existing order.
- The browser preserves a pending key in `sessionStorage` and does not automatically retry `POST` requests.
- Inventory and Notification consumers use `DefaultErrorHandler` with a fixed one-second backoff and unlimited attempts for uncaught errors.
- Invalid CDC data is quarantined and acknowledged when the quarantine DB write succeeds.
- Consumers disable auto-commit and acknowledge after application processing. Inventory reservation and Notification persistence are idempotent by `order_id`.
- Kafka Connect REST is published locally on port 8083. Compose startup checks connector status, but there is no runtime monitoring or recovery runbook.

## Proposed decisions

1. **Order API / Order DB:** Do not add generic automatic DB retries or a circuit-breaker dependency in this Kafka-focused demo. Keep one bounded request attempt, use a bounded JDBC connection-acquisition timeout, map classified transient database availability failures to HTTP 503 with `Retry-After`, and preserve the idempotency key for client retry. Do not map every `DataAccessException` to 503: validation and constraint failures remain non-retryable. The UI will not auto-retry; it will keep the pending key and explain when the user can retry. Production traffic protection can later add a circuit breaker/rate limit if needed. Do not retry an unknown commit outcome with a new key.
2. **Inventory and Notification DB failures:** Replace infinite blocking retries with Spring Kafka non-blocking retry topics and a DLT. Use configurable lab defaults of four total attempts (including the original delivery), with exponential delays beginning at one second (1s, 2s, 4s before DLT). These values demonstrate bounded retry; they are not production sizing recommendations.
3. **Error classification:** Invalid CDC payloads continue to use the existing quarantine path and are acknowledged only after quarantine persistence succeeds. Transient database failures use retry topics. Records that exhaust attempts go to a Kafka DLT with the original key/value, topic/partition/offset, attempt count, exception and timestamps; DLT records need an explicit replay procedure.
4. **Idempotency and offsets:** Preserve reservation and notification uniqueness by `order_id`. Use container-managed record acknowledgement: successful business listener return follows DB commit, while uncaught DB failures enter retry handling. The flow observer continues to catch/log its own errors and return normally, so visualization failures do not retry or block business processing. A DB commit followed by a process crash before offset commit may cause redelivery; the existing idempotency must make that harmless. Verify the retry/DLT publish completes before advancing the source offset.
5. **Debezium/Kafka operations:** Keep the CDC design. Add a concise runbook using Connect connector/task status, Connect logs, source-offset/lag checks, Kafka topic progress and MariaDB binlog retention. Do not add a full metrics stack in this demo.
6. **Scope:** Do not change the accepted low-stock reservation rule or add retries around invalid business outcomes. Do not add an outbox pattern.

## Ordering trade-off requiring confirmation

Non-blocking retry topics let the main partition continue, but a failed Inventory record can be processed after later orders. The database still prevents negative stock and reservations remain idempotent, but this does not guarantee FIFO allocation for orders competing for the same SKU. The current design does not promise FIFO by SKU. Recommended demo choice: accept this trade-off and use non-blocking retry topics for Inventory so the partition/retry behavior is visible. If FIFO must be preserved, pause the affected partition during the DB outage instead; later records on that partition will wait.

## Implementation tasks

### Phase 1: Synchronous Order API

- Map transient database access failures to a stable 503 response and `Retry-After` without returning stack traces.
- Keep idempotency behavior unchanged and document that retries must reuse the same key and payload.
- Configure a bounded DB connection-acquisition timeout so requests do not wait indefinitely for an unavailable DB.

### Phase 2: Inventory consumer

- Replace the infinite error handler with finite non-blocking retry stages and a DLT.
- Ensure parser/business-invalid records use quarantine, while database exceptions use retry handling.
- Move this listener to container-managed record acknowledgement; remove direct manual acknowledgement from the listener.
- Ensure acknowledgement occurs only after the retry/DLT handoff is durable or the reservation transaction succeeds.

### Phase 3: Notification consumer

- Apply the same finite retry/DLT policy independently to the Notification consumer group.
- Preserve notification idempotency and publish to SSE only after the notification is persisted.
- Move this listener and flow observer to container-managed record acknowledgement; remove manual ack calls. Keep the observer best-effort by catching/logging visualization errors and returning normally.

### Phase 4: Kafka topics and operations notes

- Provision retry/DLT topics for each consumer flow with local-demo partition/replication settings. Match the source partition counts (order topic: 3; inventory CDC topic: 1) and use replication factor 1 for the single broker.
- Document Connect status/log/offset checks, binlog-retention risk, recovery steps and DLT replay steps.
- Keep the existing flow observer best-effort; its visualization errors must not block business processing.

## Acceptance criteria

- No application consumer is configured for unlimited retry.
- A transient DB failure produces a finite retry schedule, then a durable DLT record with structured logs; the source offset is not silently discarded.
- Replaying the same order event cannot decrement stock twice or create duplicate notifications.
- Order API DB failures return a retryable 503 response; the same idempotency key safely resolves an ambiguous prior outcome.
- Connect failure investigation has documented status, log, offset, binlog-retention and recovery steps.
- The selected Inventory ordering behavior is documented and reflected in the UI/logs where useful.

## Risks

| Risk | Mitigation |
|---|---|
| Retry topics change event processing order | Confirm the Inventory FIFO trade-off before implementation; preserve idempotency and stock constraints |
| DLT accumulates during a prolonged DB outage | Document how to inspect DLT count/age and retry-topic lag; provide controlled replay steps |
| Manual acknowledgement conflicts with retry-topic forwarding | Use container-managed record acknowledgement for the two business listeners and verify source offset behavior |
| Retry handoff succeeds but source offset commit fails | Allow duplicate handoff and rely on idempotent DB processing; use Kafka transactions only if needed and compatible with the chosen Spring Kafka configuration |
| MariaDB binlogs expire while Debezium is down | Monitor connector lag and binlog retention; document snapshot/reconciliation recovery |

## Verification approach

After implementation, manually exercise duplicate HTTP requests, an Order DB outage, consumer DB outages and recovery, crash/redelivery after DB commit, retry exhaustion/DLT, invalid CDC quarantine, and Connect task failure/restart. No code or runtime changes are part of this planning step.
