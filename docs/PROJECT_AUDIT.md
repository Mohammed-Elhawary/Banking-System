# Design Notes

A deeper look at *why* the Banking System is built the way it is. For setup and API usage, see the [README](../README.md).

## Contents

1. [Scope & Intent](#1-scope--intent)
2. [Architecture Decisions](#2-architecture-decisions)
3. [Event Contracts](#3-event-contracts)
4. [Consistency Model](#4-consistency-model)
5. [Analytics Pipeline](#5-analytics-pipeline)
6. [Operational Notes](#6-operational-notes)
7. [Roadmap](#7-roadmap)

---

## 1. Scope & Intent

This is a portfolio project for exploring distributed-systems patterns on the Java / Spring Boot stack: choreographed sagas, event-driven services, rule-based fraud screening, OTP verification, payment webhooks and an observability pipeline. It is intentionally **not** a production banking system. Authentication, delivery guarantees and operational packaging are scoped as future work so that the workflow design stays readable end to end.

---

## 2. Architecture Decisions

### 2.1 Kafka vs REST for the transfer workflow

- **Decision:** The debit is a synchronous call. Everything after it (fraud check, completion, receiver credit, notifications) runs over Kafka.
- **Alternatives considered:** A fully synchronous REST chain (transaction → fraud → account → notification).
- **Why:** Kafka decouples the producer from several consumers. `transaction.completed` fans out to `account-service` and `notification-service` without `transaction-service` knowing either exists, and new consumers (for example a ledger) can subscribe without changing it. The fraud engine runs as a pure consumer with no REST surface.
- **Trade-off accepted:** The client receives `201` with status `PROCCESSING` and the final state arrives asynchronously. Consistency is eventual (see §4).

### 2.2 Choreography saga vs orchestration

- **Decision:** Choreography. Services react to events; `transaction-service` is a **light coordinator** that owns the transaction record and its status, stores the OTP, and issues the compensating refund.
- **Alternatives considered:** A central orchestrator (a workflow engine or a dedicated saga service) issuing commands to each participant.
- **Why:** A transfer has few steps and one natural owner of its state. Full orchestration adds a component and a command protocol without adding clarity at this size. Keeping state in `transaction-service` gives one place to answer "where is this transfer?".
- **Trade-off accepted:** The flow is distributed across consumers, so it is harder to read in one place than an orchestrator definition. An orchestrator becomes attractive when steps and branches multiply.

### 2.3 Cassandra for analytics vs MariaDB

- **Decision:** API request events are stored in Cassandra (`banking_analysis.api_requests`).
- **Alternatives considered:** A table in MariaDB; metrics only, with no raw store.
- **Why:** The workload is append-heavy and time-ordered, which suits Cassandra's write path and clustering model. It also keeps analytics traffic away from the transactional databases.
- **Trade-off accepted:** An extra datastore to run. Partition sizing needs attention as traffic grows (see §5).

### 2.4 Feign for synchronous calls vs full async

- **Decision:** OpenFeign for calls that need an answer *now*: `deduct` (is the sender ACTIVE with enough balance?), `credit` (refund), the balance read in the fraud service, and the email lookup in the notification service.
- **Alternatives considered:** Request/reply over Kafka for every interaction.
- **Why:** The debit result decides whether the saga starts at all, and a direct call returns it in one round trip with plain error semantics. Async is reserved for steps that tolerate delay.
- **Trade-off accepted:** Temporal coupling to `account-service` availability. Production would add timeouts, retries and circuit breakers on these clients (roadmap, Reliability).

### 2.5 Redis for OTP and fraud counters vs SQL

- **Decision:** Redis holds the OTP (`verification:otp:{transactionId}`, 5-minute TTL), the velocity counter (`INCR`, 60 s expiry) and the running amount average.
- **Alternatives considered:** Tables in MariaDB with scheduled cleanup.
- **Why:** Native key expiry models OTP lifetime and velocity windows directly. `INCR` is atomic, which suits per-account counting under concurrent events. State that is meant to disappear stays out of the system of record.
- **Trade-off accepted:** The state is ephemeral by nature. Production would hash OTPs before storing and publishing them.

### 2.6 Spring Cloud Gateway (reactive) vs a servlet-based gateway

- **Decision:** Spring Cloud Gateway on WebFlux.
- **Alternatives considered:** Spring Cloud Gateway Server MVC; a standalone proxy such as NGINX.
- **Why:** Non-blocking I/O is the natural model for a component that only proxies and decorates requests. Global filters give a clean place to emit one analytics event per routed request, and route-level filters carry cross-cutting policy. Each of the four service routes applies a `RequestRateLimiter` (replenish rate 10 req/s, burst capacity 20) keyed by client IP and route, with its token buckets in Redis; requests over the limit receive `429`.
- **Trade-off accepted:** The request-event publisher currently uses a synchronous `KafkaTemplate.send` inside the reactive chain. Moving to a non-blocking publish path (bounded-elastic scheduler or reactive sender) is planned so a slow broker cannot affect request latency. The rate limiter is keyed by IP and route because the system has no authenticated identity yet; once JWT is added at the gateway, limits would be keyed by principal.

---

## 3. Event Contracts

Messages are JSON maps. Keys are the transaction / payment id (the request id for `api-request-events`). Each service uses its own consumer group, so multi-consumer topics fan out.

| Topic | Producer | Consumer(s) | Payload shape | Notes |
| ----- | -------- | ----------- | ------------- | ----- |
| `transaction.initiated` | transaction-service | fraud-detection-service | Transaction details (id, accounts, amount) | Published after the debit succeeds |
| `fraud.check.clean` | fraud-detection-service | transaction-service | Transaction id + clean result | Completes the transaction |
| `verification.required` | fraud-detection-service | transaction-service | Transaction id + `reason` | Triggers OTP generation |
| `transaction.otp.generated` | transaction-service | notification-service | `transactionId, accountNumber, otp, amount, reason` | Emails the OTP |
| `transaction.completed` | transaction-service | account-service, notification-service | `id, senderAccountNumber, receiverAccountNumber, amount, description` | Credits the receiver and notifies; identifier key is `id` |
| `fraud.detected` | transaction-service | account-service, notification-service | `transactionId, accountNumber, reason` | Blocks the account; emails the sender |
| `transaction.refunded` | transaction-service | notification-service | `transactionId, amount, senderAccountNumber, reason` | Notifies the sender of the refund |
| `payment.completed` | payment-service | notification-service | `paymentId, accountNumber, amount, paymobTransactionId` | Notifies a successful payment |
| `payment.failed` | payment-service | notification-service | `paymentId, accountNumber, amount, reason` | Notifies a failed payment |
| `api-request-events` | api-gateway | analysis-service | `requestId, method, path, service, status, durationMs, timestamp` | Identical DTO on both sides |

**Why JSON maps.** Consumers deserialize into untyped maps (type headers disabled, `HashMap` default type). That kept the services independently deployable and avoided a shared-library dependency while the event vocabulary was still changing. The contracts are now aligned across all producers and consumers; because they are implicit, keeping them aligned as they evolve is the next investment. In production this becomes Avro (or Protobuf) with a Schema Registry, compatibility rules enforced in CI, consumer-driven contract tests, and explicitly declared topics (`NewTopic`) instead of broker auto-creation. A shared identifier convention (`transactionId` everywhere, including `transaction.completed`) would also be adopted.

---

## 4. Consistency Model

The current model favors an understandable workflow:

- **At-least-once delivery is assumed.** Consumers are written for the happy path of a single delivery; duplicate-safe handlers are future work.
- **No outbox.** The database write and the Kafka publish are separate steps, so the pair is not atomic.
- **Balance updates are read-modify-write.** Debit and credit load the account, check, and save, which keeps the logic obvious. At scale this would use optimistic locking (`@Version`) or row-level locks.
- **No idempotency keys yet.** `deduct`, `credit` and `verify` do not carry request identifiers, so a repeated call repeats its effect.
- **Block and refund are issued independently** after an incorrect OTP; sequencing them is planned.
- **The receiver is credited asynchronously** on `transaction.completed` and is not validated at transfer start.
- **Transactions have no deadline.** An OTP expires after 5 minutes, and a reaper that compensates expired or unanswered transfers is planned.
- **Gateway rate limiting** (10 req/s, burst 20 per client and route) bounds request bursts at the edge. It complements, and does not replace, idempotency keys.

### What a hardened version looks like

- Transactional outbox (or CDC) so state change and event publish commit together.
- Idempotency keys on `deduct`, `credit`, `verify` and payment webhooks; idempotent consumers keyed by event id.
- Optimistic locking on account balances; a unique constraint on any dedupe key.
- Consumer retry with backoff and dead-letter topics instead of log-and-continue.
- A terminal `FAILED` state with a recorded reason for debit failures, plus a scheduled reaper for stalled transfers.
- Receiver validation up front; a defined order for block and refund.
- Timeouts, retries and circuit breakers on Feign clients.

---

## 5. Analytics Pipeline

```text
api-gateway ──▶ Kafka (api-request-events) ──▶ analysis-service ──▶ Cassandra (api_requests)
                                                       │
                                                       └──▶ Micrometer ──▶ /actuator/prometheus ──▶ Prometheus ──▶ Grafana
```

1. **Gateway.** A `GlobalFilter` records the start time, lets the chain run, then publishes an `ApiRequestEvent` keyed by request id. `service` is the gateway **route id** (for example `account-service`).
2. **Kafka.** Topic `api-request-events`, consumer group `analysis-service-group`.
3. **Cassandra.** Table `banking_analysis.api_requests` with primary key `((service, request_date), timestamp, request_id)` and columns `method, path, status, duration_ms`. The table is created on startup (`spring.cassandra.schema-action=CREATE_IF_NOT_EXISTS`).
4. **Micrometer.** `api_requests_total{service,status}` (counter) and `api_request_duration_ms{service}` (summary).
5. **Prometheus.** Scrapes `/actuator/prometheus` on the Analysis Service every 5 s. Grafana is provided by Compose and receives Prometheus as a datasource.

**Why `(service, request_date)` is the partition key.** The dominant question is "what did service X handle on day D?". That key answers it from a single partition, bounds each partition to one service-day, and keeps rows ordered by `timestamp` through the clustering column. `request_id` makes rows unique.

**What changes at high traffic.** One service-day partition grows with that service's traffic.
- **Bucketing:** add an hour (or minute) bucket to the partition key to cap partition size.
- **TTL:** set a retention window on the table so old rows expire.
- **Reads:** the per-event "last five minutes" read in the consumer is a diagnostic aid; at scale, trend queries move to Prometheus aggregation and the consumer becomes write-only.
- **Metrics:** counters live in memory and reset on restart. Prometheus handles resets in `rate()`; exact historical counts come from Cassandra.
- **Label hygiene:** exclude API-docs routes from business metrics.
- **Coverage:** add `/actuator/prometheus` to every service and provision dashboards.

---

## 6. Operational Notes

### Config surface

| Setting | Where | Default |
| ------- | ----- | ------- |
| Service ports | `application.properties` | gateway 8080, account 8081, transaction 8082, fraud 8083, payment 8084, analysis 8085, notification 8086 |
| Infrastructure ports | `docker-compose.yml` | Redis 6379, MariaDB 3307 (host), Cassandra 9042, Kafka 9092, Mailpit 1025 / 8025, Prometheus 9090, Grafana 3000 |
| `fraud.max-transaction-per-minute` | fraud-detection-service | `5` |
| `fraud.suspicious-amount-miultiplier` | fraud-detection-service | `5` (the key is spelled this way in the config) |
| `fraud.max-balance-percentage` | fraud-detection-service | `0.90` |
| OTP TTL | transaction-service | 5 minutes |
| Rate limit (`replenishRate` / `burstCapacity`) | api-gateway, four service routes | `10` / `20` |
| Gateway actuator exposure | api-gateway | `health,info,gateway` |
| `PAYMOB_SECRET_KEY`, `PAYMOB_PUBLIC_KEY`, `PAYMOB_INTEGRATION_ID`, `PAYMOB_HMAC_KEY` | payment-service (environment) | empty; set before using payments |
| MariaDB password | `docker-compose.yml` + service properties | placeholder `your_password`; change in both |
| `spring.cassandra.schema-action` | analysis-service | `CREATE_IF_NOT_EXISTS` |
| Prometheus scrape interval | `prometheus/prometheus.yml` | 5 s |

### Running locally

1. `docker compose up -d` starts the infrastructure.
2. Create the Cassandra keyspace (manual step, below).
3. Export the Paymob environment variables if you want to use payments.
4. Start `account-service` first, then the remaining services and finally the gateway, each with `./mvnw spring-boot:run`.

### Manual steps

```bash
docker exec -i banking-cassandra cqlsh < analysis-service/src/main/resources/cassandra/schema.cql
```

Run this once after the Cassandra container is up to create the keyspace; the Analysis Service creates the `api_requests` table on startup. The Paymob webhook and redirect URLs are set in `PaymentService`; edit them to receive real webhooks through a tunnel.

See the [README](../README.md#running-the-project) for the full setup and a guided clean / suspicious transfer walkthrough.

---

## 7. Roadmap

**Security hardening**
- [ ] JWT / OAuth2 at the gateway; role checks on account administration; rate limits keyed by principal
- [ ] Restrict internal endpoints (`deduct`, `credit`, `block`, `active`) to the service network
- [ ] Use a non-root DB user; adopt a secret manager with rotation
- [ ] Bind OTP verification to the authenticated user and add attempt throttling
- [ ] Hash OTPs at rest and in events
- [ ] Restrict CORS on payment-service
- [ ] Constant-time HMAC comparison and webhook replay protection; compare webhook amount to the stored payment

**Reliability**
- [ ] Transactional outbox
- [ ] Idempotency keys (`deduct`, `credit`, `verify`, webhooks)
- [ ] Consumer retries with backoff and dead-letter topics
- [ ] Timeouts, retries and circuit breakers on Feign clients
- [ ] Reaper for transactions awaiting a fraud result or OTP; terminal `FAILED` state for debit failures
- [ ] Receiver validation at transfer start; ordered block-then-refund handling
- [ ] Non-blocking event publishing in the gateway

**Data**
- [ ] Flyway / Liquibase migrations in place of `ddl-auto=update`
- [ ] Optimistic locking on account balances
- [ ] Enforce `dailyTransactionLimit`
- [ ] Pagination for list endpoints

**Observability**
- [ ] Distributed tracing with correlation ids
- [ ] Provisioned Grafana datasource and dashboards
- [ ] `/actuator/prometheus` on every service
- [ ] Cassandra TTL and time bucketing

**Ops**
- [ ] Dockerfiles and Compose profiles for all services
- [ ] Pinned image versions
- [ ] Testcontainers integration tests
- [ ] Unit tests for fraud rules
- [ ] Consumer-driven contract tests for events

**Product**
- [ ] Link completed payments to the account ledger
- [ ] Normalize API enum spellings in a versioned release
- [ ] Externalize Paymob URLs and billing data