# Retry Handling Tasks

- [ ] **Confirm Inventory ordering policy.** Choose non-blocking retry topics and accept possible reordering (recommended for this demo), or pause the affected partition to preserve its order.
- [ ] **Order API failure response.** Add bounded JDBC connection acquisition and map only classified transient DB availability failures to HTTP 503 with `Retry-After`; keep validation/constraint failures non-retryable, idempotency unchanged and UI retries user-driven.
- [ ] **Inventory retry path.** Configure bounded delayed retry topics and DLT; keep invalid CDC quarantine separate from transient DB failure handling; switch to container-managed record ack and preserve commit-before-offset idempotency.
- [ ] **Notification retry path.** Configure its own bounded delayed retry topics and DLT; switch to container-managed record ack; persist before SSE publish, preserve notification idempotency, and keep flow observer errors best-effort.
- [ ] **Topic provisioning.** Add retry/DLT topic definitions with partition counts matching the source topics (3 for orders, 1 for inventory CDC) and replication factor 1 for the single local broker.
- [ ] **Operations notes.** Document Connect task status/log/offset checks, binlog retention, restart/recovery, consumer lag, DLT inspection and safe replay.
- [ ] **Manual verification after implementation.** Exercise duplicate requests, transient failures, recovery, commit-before-ack redelivery, retry exhaustion/DLT, invalid CDC and connector restart.
