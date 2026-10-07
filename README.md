# Banking System

![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.1-6DB33F?logo=springboot&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-event--driven-231F20?logo=apachekafka&logoColor=white)
![Status](https://img.shields.io/badge/status-portfolio%20project-blue)

An event-driven banking backend made of an API gateway and six Spring Boot services. Money transfers run as a choreographed saga over Kafka with compensating refunds, and every transfer passes a Redis-backed fraud engine that can escalate to email OTP verification. Every gateway request flows through a Kafka → Cassandra → Prometheus analytics pipeline, and Paymob payments are confirmed through HMAC-SHA512-verified webhooks. Built with Java 21, Spring Boot 4.1.1, Spring Cloud, MariaDB, Redis, Cassandra and Kafka.

For the reasoning behind the design, read **[docs/DESIGN_NOTES.md](docs/DESIGN_NOTES.md)**.

<p align="center">
  <img src="docs/assets/architecture.gif" alt="Animated architecture overview" width="100%">
</p>

---

## What Makes This Interesting

- **Saga-style compensation across services.** The sender is debited synchronously, later steps are driven by Kafka events, and a failed verification triggers a compensating refund. `transaction-service` owns the saga state.
- **Rule-based fraud detection on Redis.** Three ordered rules (velocity, amount versus running average, balance percentage) with configurable thresholds. They run as an event-driven consumer with no REST surface.
- **OTP verification with TTL and email delivery.** A 6-digit `SecureRandom` code is stored in Redis with a 5-minute TTL and emailed through the notification service. The outcome decides whether the transfer completes or is flagged.
- **Full analytics pipeline.** Gateway → Kafka → Cassandra → Micrometer → Prometheus, with a time-series partition model of `(service, request_date)`.
- **Paymob payment integration.** Unified Checkout intentions plus a webhook that recomputes an HMAC-SHA512 over 21 payload fields before accepting a result. Credentials come from environment variables.
- **Reactive API gateway.** Spring Cloud Gateway (WebFlux) routes four service prefixes, rate-limits each route (10 req/s, burst 20), aggregates Swagger UI, and publishes an event for every routed request.

---

## Table of Contents

- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Microservices](#microservices)
- [Transaction Flow](#transaction-flow)
- [OTP Verification](#otp-verification)
- [SAGA / Compensation](#saga--compensation)
- [Fraud Detection](#fraud-detection)
- [Payment Integration](#payment-integration)
- [Event-Driven Architecture](#event-driven-architecture)
- [Redis](#redis)
- [Request Analytics](#request-analytics)
- [API Examples](#api-examples)
- [Project Structure](#project-structure)
- [Running the Project](#running-the-project)
- [Design Trade-offs & Roadmap](#design-trade-offs--roadmap)
- [What I Learned](#what-i-learned)
- [License](#license)

---

## Architecture

An **API Gateway plus six Spring Boot services**. Synchronous calls use REST and OpenFeign; asynchronous workflows use Kafka. MariaDB, Redis, Cassandra, Kafka, a local SMTP sink, Prometheus and Grafana run from Docker Compose.

![Architecture diagram](docs/assets/architecture.png)

| Channel | Used for |
| ------- | -------- |
| **HTTP** (via gateway) | Client → Account, Transaction and Payment APIs |
| **OpenFeign** | Transaction → Account (deduct / credit), Fraud → Account (balance), Notification → Account (email lookup) |
| **Kafka** | Transfer saga events, fraud results, OTP and payment notifications, gateway request events |
| **Redis** | OTP storage (Transaction), fraud counters (Fraud Detection), gateway rate-limit buckets |
| **JDBC / MariaDB** | `account_db`, `transaction_db`, `payment_db` |
| **CQL / Cassandra** | `banking_analysis.api_requests` (Analysis Service) |
| **Scrape** | Prometheus pulls `/actuator/prometheus` from the Analysis Service every 5 s |
| **SMTP** | Notification → Mailpit (local mail sink) |
| **HTTPS** | Payment ↔ Paymob (intention API and webhook callback) |

---

## Technology Stack

| Area | Technology |
| ---- | ---------- |
| Language / build | Java 21, Maven (Maven Wrapper in each service) |
| Framework | Spring Boot 4.1.1, Spring Cloud 2025.1.3 |
| Gateway | Spring Cloud Gateway (reactive / WebFlux), `RequestRateLimiter` |
| Service-to-service | Spring Cloud OpenFeign |
| Messaging | Spring for Apache Kafka (JSON), Confluent Kafka + ZooKeeper 7.4.0 images |
| Persistence | Spring Data JPA / Hibernate on MariaDB; Spring Data Cassandra on Apache Cassandra |
| Cache / ephemeral state | Spring Data Redis |
| Email | Spring Mail, Mailpit (local SMTP sink) |
| Payments | Paymob Unified Checkout, Spring `RestClient`, HMAC-SHA512 webhook verification |
| API docs | springdoc-openapi 3.1.0 (aggregated Swagger UI) |
| Metrics | Micrometer + Prometheus registry, Prometheus, Grafana (Docker Compose) |
| Other | Bean Validation, Spring Boot Actuator, Lombok |
| Tests | Spring Boot context-load tests today; unit, integration and contract tests are on the roadmap |

---

## Microservices

| Service | Responsibility | Port | Storage |
| ------- | -------------- | ---- | ------- |
| `api-gateway` | Path-based routing, per-route rate limiting, aggregated Swagger UI, one Kafka event per routed request | 8080 | Redis (rate-limit buckets) |
| `account-service` | Accounts, balances, status (block / activate), saga debit and credit | 8081 | MariaDB `account_db` |
| `transaction-service` | Transfer coordination, OTP generation and verification, compensation | 8082 | MariaDB `transaction_db`, Redis |
| `fraud-detection-service` | Rule-based fraud checks on new transfers (event-driven, no REST endpoints) | 8083 | Redis |
| `payment-service` | Paymob intentions, webhook verification, payment status | 8084 | MariaDB `payment_db` |
| `notification-service` | Kafka consumer that sends email alerts | 8086 | — |
| `analysis-service` | Consumes gateway events, stores them in Cassandra, exposes Prometheus metrics | 8085 | Cassandra `banking_analysis` |

Docker Compose provides the infrastructure: Redis (`6379`), MariaDB (host port `3307`), Cassandra (`9042`), ZooKeeper, Kafka (`9092`), Mailpit (SMTP `1025`, UI `8025`), Prometheus (`9090`) and Grafana (`3000`). The application services run locally through the Maven Wrapper; service Dockerfiles are planned.

<details>
<summary><b>Account Service walkthrough</b> (animated)</summary>

![Account service](docs/assets/gifs/account-service.gif)

</details>

<details>
<summary><b>API Gateway walkthrough</b> (animated)</summary>

![API gateway](docs/assets/gifs/api-gateway.gif)

The gateway declares four path routes (`/api/v1/accounts/**`, `/api/v1/transactions/**`, `/api/v1/payments/**`, `/api/v1/fraud/**`) plus `/docs/<service>/v3/api-docs` routes that feed a single Swagger UI. Each of the four service routes carries a `RequestRateLimiter` filter (10 requests/s replenish rate, burst capacity 20) keyed by client IP and route; requests over the limit receive `429`. A global filter publishes an `api-request-events` message after every routed request, consumed by the Analysis Service. Gateway-level authentication is planned (see [Design Trade-offs & Roadmap](#design-trade-offs--roadmap)).

</details>

<details>
<summary><b>Notification Service walkthrough</b> (animated)</summary>

![Notification service](docs/assets/gifs/notification-service.gif)

The service listens to six topics and sends an email for each: OTP delivery, transaction completion, fraud alert (to the sender), refund, and payment success and failure.

</details>

<details>
<summary><b>Analysis Service walkthrough</b> (animated)</summary>

![Analysis service](docs/assets/gifs/analysis-service.gif)

See [Request Analytics](#request-analytics).

</details>

---

## Transaction Flow

![Transaction service](docs/assets/gifs/transaction-service.gif)

A transfer is `POST /api/v1/transactions/transfer`:

1. The transaction is saved as `PENDING` with a random reference number.
2. `transaction-service` calls `account-service` over Feign (`POST /api/v1/accounts/{n}/deduct`). **The sender is debited first.**
3. Status becomes `PROCCESSING` (spelling as in the code) and `transaction.initiated` is published. The client immediately gets `201`.
4. `fraud-detection-service` consumes the event and publishes one of two results:
   - **Clean** → `fraud.check.clean` → the transaction becomes `COMPLETED` and `transaction.completed` is published. `account-service` consumes it and **credits the receiver**.
   - **Suspicious** → `verification.required` → the OTP flow below.

Statuses: `PENDING`, `PROCCESSING`, `PENDING_VERIFICATION`, `COMPLETED`, `FLAGGED` (`FAILURE` is declared and reserved for the failure-handling work on the roadmap).

---

## OTP Verification

Only **suspicious** transfers require an OTP; clean transfers complete without one.

- **Generation:** on `verification.required`, `transaction-service` generates a 6-digit code with `SecureRandom` (100000–999999).
- **Storage:** Redis key `verification:otp:{transactionId}` with a **5-minute TTL**; the transaction moves to `PENDING_VERIFICATION`.
- **Delivery:** `transaction.otp.generated` is published and `notification-service` emails the code with the reason, amount and validity window.
- **Verification:** `POST /api/v1/transactions/{transactionId}/verify?otp=<code>`
  - correct code → key deleted, transaction `COMPLETED`;
  - incorrect code → key deleted, `fraud.detected` published (sender account is blocked and the sender is emailed), compensating refund issued, transaction `FLAGGED`;
  - expired / missing key → compensating refund issued, transaction `FLAGGED` (no account block).

Only transactions in `PENDING_VERIFICATION` accept a verification call.

---

## SAGA / Compensation

![Saga flow](docs/assets/gifs/saga-flow.gif)

Transfers use **choreography with compensation**: the debit is a local step, later steps are driven by Kafka events, and `transaction-service` keeps the saga state and issues the compensating refund.

| Step | Mechanism |
| ---- | --------- |
| Debit sender | Feign `POST /accounts/{n}/deduct` |
| Start fraud check | Kafka `transaction.initiated` |
| Complete | Kafka `transaction.completed` → receiver credited by `account-service` |
| Compensate (incorrect / expired OTP) | Feign `POST /accounts/{n}/credit` → status `FLAGGED` → Kafka `transaction.refunded` |
| Block sender (incorrect OTP only) | Kafka `fraud.detected` → `account-service` blocks the account |

The delivery-guarantee layer (outbox, idempotency keys, retries, timeouts) is scoped as future work. See [DESIGN_NOTES §4](docs/DESIGN_NOTES.md#4-consistency-model).

---

## Fraud Detection

![Fraud detection](docs/assets/gifs/fraud-detection-service.gif)

`fraud-detection-service` consumes `transaction.initiated`, reads the sender's post-debit balance from `account-service`, and applies three rules **in order**; the first match wins.

| # | Rule | Implementation | Default |
| - | ---- | -------------- | ------- |
| 1 | Velocity | Redis counter `fraud:velocity:{account}`, 60 s expiry; flagged when the count exceeds the limit | `fraud.max-transaction-per-minute=5` |
| 2 | Amount | Redis running average `fraud:Amount:{account}`; flagged when `amount > average × multiplier`. The first transfer seeds the average | `fraud.suspicious-amount-miultiplier=5` |
| 3 | Balance | Flagged when `amount > balance × percentage` (applied when the post-debit balance is above 0) | `fraud.max-balance-percentage=0.90` |

No rule matched → `fraud.check.clean`. A rule matched → `verification.required` with a reason string.

---

## Payment Integration

![Payment service](docs/assets/gifs/payment-service.gif)

`payment-service` integrates **Paymob Unified Checkout**:

1. `POST /api/v1/payments/create-order` with `{ accountNumber, amount, description }`.
2. The service calls Paymob `POST /v1/intention/` (amount in piasters, currency `EGP`).
3. A `Payment` row is saved as `CREATED`; the response returns `paymentId`, `clientSecret` and `publicKey`.
4. The client opens Paymob's Unified Checkout with those values (see the sample `checkout.html`).
5. Paymob calls `POST /api/v1/payments/webhook?hmac=…`. The service recomputes an **HMAC-SHA512** over 21 payload fields with the configured secret and rejects a mismatch.
6. `success: true` → `COMPLETED` and `payment.completed`; otherwise → `FAILED` and `payment.failed`.
7. `GET /api/v1/payments/payment-result?id=<paymobTransactionId>` renders a result page.

Credentials are read from environment variables through `${...:}` placeholders in `application.properties`; no credentials live in source (see [Running the Project](#running-the-project)). Linking completed payments to the account ledger is planned; today the events drive notifications.

---

## Event-Driven Architecture

Kafka runs as a single broker with topic auto-creation. Messages are JSON; keys are the transaction / payment id (or the request id for `api-request-events`).

| Topic | Producer | Consumer(s) | Purpose |
| ----- | -------- | ----------- | ------- |
| `transaction.initiated` | transaction-service | fraud-detection-service | Start the fraud check after the debit |
| `fraud.check.clean` | fraud-detection-service | transaction-service | Complete the transaction |
| `verification.required` | fraud-detection-service | transaction-service | Generate the OTP |
| `transaction.otp.generated` | transaction-service | notification-service | Email the OTP |
| `transaction.completed` | transaction-service | account-service, notification-service | Credit the receiver; notify |
| `fraud.detected` | transaction-service | account-service, notification-service | Block the sender after an incorrect OTP; alert the sender |
| `transaction.refunded` | transaction-service | notification-service | Notify about the compensating refund |
| `payment.completed` | payment-service | notification-service | Notify a successful payment |
| `payment.failed` | payment-service | notification-service | Notify a failed payment |
| `api-request-events` | api-gateway | analysis-service | One event per routed request |

Payload shapes and the planned move to schema-managed contracts are in [DESIGN_NOTES §3](docs/DESIGN_NOTES.md#3-event-contracts).

---

## Redis

Redis holds short-lived, key-based state:

| Key | Written by | Purpose | TTL |
| --- | ---------- | ------- | --- |
| `verification:otp:{transactionId}` | transaction-service | OTP of a suspicious transfer | 5 minutes |
| `fraud:velocity:{accountNumber}` | fraud-detection-service | Transfers per account in the current window (`INCR`) | 60 s (set on first increment) |
| `fraud:Amount:{accountNumber}` | fraud-detection-service | Running average transfer amount per account | none |

The gateway's `RequestRateLimiter` also keeps its per-client, per-route token buckets in Redis.

---

## Request Analytics

![Analysis service](docs/assets/gifs/analysis-service.gif)

1. A gateway `GlobalFilter` waits for the response, then publishes an `ApiRequestEvent` (`requestId`, `method`, `path`, `service` = gateway route id, `status`, `durationMs`, `timestamp`) to `api-request-events`.
2. `analysis-service` (group `analysis-service-group`) consumes it and writes to Cassandra table `banking_analysis.api_requests`, primary key `((service, request_date), timestamp, request_id)`. The table is created on startup (`spring.cassandra.schema-action=CREATE_IF_NOT_EXISTS`).
3. It records Micrometer metrics at `http://localhost:8085/actuator/prometheus`:
   - `api_requests_total{service, status}` (counter)
   - `api_request_duration_ms{service}` (summary)
4. Prometheus (`prometheus/prometheus.yml`) scrapes the Analysis Service every 5 s. Grafana starts with Compose; add Prometheus (`http://prometheus:9090`) as a datasource to build dashboards. Provisioned dashboards are on the roadmap.

The keyspace definition lives in `analysis-service/src/main/resources/cassandra/schema.cql` (see [Running the Project](#running-the-project)).

---

## API Examples

All requests go through the gateway on `http://localhost:8080`.

<details open>
<summary><b>Create an account</b></summary>

```http
POST /api/v1/accounts/create
Content-Type: application/json

{
  "accountHolderName": "Alice Example",
  "email": "alice@example.com",
  "phone": "01000000000",
  "accountType": "SAVING",
  "initialDeposit": 1000
}
```

`accountType` is one of `SAVING`, `CURRENT`, `FIXED_DEPOSIT`. Response `201`:

```json
{
  "id": "<uuid>",
  "accountHolderName": "Alice Example",
  "accountNumber": "SAV-XXXXXXXXXX",
  "accountStatus": "ACTIVE",
  "accountType": "SAVING",
  "phone": "01000000000",
  "balance": 1000,
  "email": "alice@example.com",
  "dailyTransactionLimit": 100000,
  "createdAt": "…",
  "updatedAt": "…"
}
```

Duplicate email → `409`.

</details>

<details>
<summary><b>Other account endpoints</b></summary>

| Method | Path | Notes |
| ------ | ---- | ----- |
| `GET` | `/api/v1/accounts/{accountNumber}` | Account details, `404` if unknown |
| `GET` | `/api/v1/accounts/{accountNumber}/balance` | Balance only |
| `GET` | `/api/v1/accounts/` | All accounts (pagination planned) |
| `PATCH` | `/api/v1/accounts/{accountNumber}/block` | Status → `BLOCKED` |
| `PATCH` | `/api/v1/accounts/{accountNumber}/active` | Status → `ACTIVE` |
| `POST` | `/api/v1/accounts/{accountNumber}/deduct?amount=` | Saga debit (called by transaction-service) |
| `POST` | `/api/v1/accounts/{accountNumber}/credit?amount=` | Saga refund (called by transaction-service) |

</details>

<details open>
<summary><b>Transfer money</b></summary>

```http
POST /api/v1/transactions/transfer
Content-Type: application/json

{
  "senderAccountNumber": "SAV-XXXXXXXXXX",
  "receiverAccountNumber": "CUR-YYYYYYYYYY",
  "amount": 100,
  "description": "Rent"
}
```

Response `201` (status is normally `PROCCESSING` because the fraud check runs asynchronously):

```json
{
  "id": "<uuid>",
  "senderAccountNumber": "SAV-XXXXXXXXXX",
  "receiverAccountNumber": "CUR-YYYYYYYYYY",
  "amount": 100,
  "transactionType": "TREANSFER",
  "transactionStatus": "PROCCESSING",
  "reasoneFailure": "N/A",
  "description": "Rent",
  "referenceNumber": "<uuid>",
  "createdAt": null,
  "completedAt": null
}
```

`TREANSFER`, `PROCCESSING` and `reasoneFailure` are the spellings of the current API contract; normalizing them belongs with the next versioned API release.

</details>

<details>
<summary><b>Verify an OTP and read history</b></summary>

```http
POST /api/v1/transactions/{transactionId}/verify?otp=123456
GET  /api/v1/transactions/account/{accountNumber}
GET  /api/v1/transactions/All/{accountNumber}
```

Verify returns the transaction: `COMPLETED` for a correct OTP, `FLAGGED` after an incorrect or expired one. `account/{accountNumber}` returns transactions where the account is sender **or** receiver; `All/{accountNumber}` returns those where it is the **sender**.

</details>

<details>
<summary><b>Payments</b></summary>

```http
POST /api/v1/payments/create-order
Content-Type: application/json

{ "accountNumber": "SAV-XXXXXXXXXX", "amount": 100, "description": "Top-up" }
```

Response `201`:

```json
{
  "paymentId": "<uuid>",
  "paymobIntentionId": "<id>",
  "paymobOrderId": "<id>",
  "amount": 100,
  "clientSecret": "<client secret>",
  "publicKey": "<your Paymob public key>",
  "currency": "EGP",
  "paymentStatus": "CREATED"
}
```

`POST /api/v1/payments/webhook?hmac=…` is called by Paymob; `GET /api/v1/payments/payment-result?id=…` renders the result page.

</details>

Swagger UI: `http://localhost:8080/swagger-ui.html`. Metrics: `http://localhost:8085/actuator/prometheus`.

---

## Project Structure

```text
Banking-System/
├── docker-compose.yml            # Redis, MariaDB, Cassandra, ZooKeeper, Kafka, Mailpit, Prometheus, Grafana
├── mariadb/
│   └── init.sql                  # creates account_db, transaction_db, payment_db
├── api-gateway/                  # Spring Cloud Gateway (:8080)
│   ├── config/                   #   GatewayRouteConfig, RateLimiterConfig
│   ├── filter/                   #   global filter → request events
│   └── event/ dto/               #   Kafka producer + ApiRequestEvent
├── account-service/              # (:8081)
│   ├── controller/  service/  repository/  entities/  dto/
│   ├── exception/                #   custom exceptions + GlobalExceptionHandler
│   └── helper/                   #   AccountNumberGenerator
├── transaction-service/          # (:8082)
│   ├── controller/  service/  repository/  entity/  dto/
│   ├── client/                   #   Feign client → account-service
│   ├── event/                    #   event payload classes
│   └── config/                   #   RedisConfig, OpenAPI
├── fraud-detection-service/      # (:8083)
│   ├── service/                  #   Kafka consumer + fraud rules
│   ├── client/                   #   Feign client → account-service
│   └── model/
├── payment-service/              # (:8084)
│   ├── controller/  service/  repository/  entity/  dto/
│   └── config/                   #   Paymob properties, RestClient, CORS
├── notification-service/         # (:8086)
│   ├── service/                  #   Kafka listeners + mail sender
│   └── dto/
├── analysis-service/             # (:8085)
│   ├── kafka/                    #   api-request-events consumer
│   ├── entity/ repository/       #   Cassandra mapping
│   ├── service/                  #   Micrometer metrics (ApiMetrics)
│   └── resources/cassandra/      #   schema.cql
├── prometheus/
│   └── prometheus.yml            # scrapes analysis-service on :8085
├── README.md
└── docs/
    ├── DESIGN_NOTES.md           # decisions, event contracts, consistency model, roadmap
    └── assets/
        ├── architecture.gif
        ├── architecture.png
        └── gifs/                 # one animated walkthrough per service + saga-flow
```

---

## Running the Project

### Prerequisites

- JDK 21
- Docker and Docker Compose
- (Optional) a Paymob test account and a public tunnel URL to receive real webhooks

### 1. Start the infrastructure

```bash
git clone https://github.com/Mohammed-Elhawary/Banking-System.git
cd Banking-System
docker compose up -d
```

This starts Redis (`6379`), MariaDB (`3307`), Cassandra (`9042`), ZooKeeper, Kafka (`9092`), Mailpit (<http://localhost:8025>), Prometheus (<http://localhost:9090>) and Grafana (<http://localhost:3000>). MariaDB creates `account_db`, `transaction_db` and `payment_db` on first start.

Once the `banking-cassandra` container is up (it can take a minute), create the analytics keyspace:

```bash
docker exec -i banking-cassandra cqlsh < analysis-service/src/main/resources/cassandra/schema.cql
```

The Analysis Service creates the `api_requests` table itself on startup.

### 2. Configure

Each service reads its own `src/main/resources/application.properties`; all hosts point at `localhost`.

- **MariaDB password:** the repo uses the placeholder `your_password` in `docker-compose.yml` and the service properties. Change it in both places for a real password.
- **Paymob** (payment-service only): export your own test credentials before starting the service. The properties resolve to empty defaults when a variable is not set.

  | Property | Environment variable |
  | -------- | -------------------- |
  | `paymob.secret-key` | `PAYMOB_SECRET_KEY` |
  | `paymob.public-key` | `PAYMOB_PUBLIC_KEY` |
  | `paymob.integration-id` | `PAYMOB_INTEGRATION_ID` |
  | `paymob.hmac-secret` | `PAYMOB_HMAC_KEY` |

```bash
  export PAYMOB_SECRET_KEY=...
  export PAYMOB_PUBLIC_KEY=...
  export PAYMOB_INTEGRATION_ID=...
  export PAYMOB_HMAC_KEY=...
```

- The webhook `notification_url` and `redirection_url` sent to Paymob are set in `PaymentService`; edit them to receive real webhooks through your tunnel (externalizing them is on the roadmap).

### 3. Start the services

Start `account-service` first (the others call it), then the rest, each in its own terminal:

```bash
cd account-service         && ./mvnw spring-boot:run
cd transaction-service     && ./mvnw spring-boot:run
cd fraud-detection-service && ./mvnw spring-boot:run
cd payment-service         && ./mvnw spring-boot:run
cd notification-service    && ./mvnw spring-boot:run
cd analysis-service        && ./mvnw spring-boot:run
cd api-gateway             && ./mvnw spring-boot:run
```

### 4. Try a transfer

1. Create two accounts through the gateway (see [API Examples](#api-examples)); give the sender `initialDeposit: 1000`.
2. **Clean transfer:** transfer `100` → `COMPLETED`, receiver credited.
3. **Suspicious transfer:** transfer `600` from a fresh `1000` balance. The remaining balance is `400` and `600 > 400 × 0.90`, so the balance rule fires → `PENDING_VERIFICATION`. Open Mailpit for the OTP email, then call the `verify` endpoint.
4. **Rate limiting:** send a quick loop of requests to any service route; once the burst of 20 is spent, the gateway answers `429` for the excess.

---

## Design Trade-offs & Roadmap

This build optimizes for **clarity of the distributed workflow** over production hardening. Each item below is an intentional scope decision with a defined production path. The full roadmap is in [DESIGN_NOTES §7](docs/DESIGN_NOTES.md#7-roadmap).

**Already in place:** per-route gateway rate limiting (10 req/s, burst 20), environment-based secrets with no credentials in source, HMAC-verified payment webhooks, and event contracts aligned across all producers and consumers.

| Area | Current design (by design for this scope) | Production direction |
| ---- | ----------------------------------------- | -------------------- |
| **Authentication** | Out of scope for this build; all endpoints are open on the local network | JWT / OAuth2 at the gateway; internal endpoints (`deduct`, `credit`, `block`, `active`) restricted to the service network; OTP verification bound to the authenticated user |
| **Secrets** | Paymob credentials are read from environment variables via `${...:}` placeholders; no credentials in source | Secret manager with rotation; non-root database user |
| **Delivery guarantees** | At-least-once assumed; DB write and Kafka publish are separate steps | Transactional outbox, idempotency keys, consumer retries, dead-letter topics |
| **Concurrency** | Balance updates are read-modify-write for readability | Optimistic locking (`@Version`) or row-level locks on balances |
| **Stalled transfers** | No timeout sweeper for transactions awaiting a fraud result or an OTP | Scheduled reaper that compensates after a deadline |
| **Receiver handling** | Receiver is credited asynchronously on `transaction.completed` | Validate the receiver at transfer start; sequence account block and refund |
| **Event contracts** | Untyped JSON maps keep services loosely coupled; contracts are aligned across producers and consumers | Avro / Schema Registry plus consumer-driven contract tests to keep them aligned as they evolve |
| **Analytics** | Gateway publishes synchronously; no retention policy on the request log | Non-blocking publisher, TTL and time bucketing, provisioned Grafana dashboards |
| **Operations** | Services run via Maven Wrapper; `:latest` image tags; `ddl-auto=update` | Dockerfiles, pinned images, Flyway/Liquibase, Testcontainers |
| **API naming** | Enum spellings (`PROCCESSING`, `TREANSFER`) are part of the current contract | Normalize in a versioned API release |

---

## What I Learned

- **Distributed sagas are mostly about the unhappy paths.** The debit-then-verify flow looks simple until you ask what happens between the database write and the Kafka publish. That question led me to the outbox pattern and idempotency keys.
- **Choreography versus orchestration is a spectrum.** Events drive the flow here, but a transfer still needs one service to own its state and issue the refund. `transaction-service` ended up as a light coordinator.
- **Event contracts need the same care as REST APIs.** Untyped JSON maps are fast to start with, and field names and types become the real interface. Aligning producers and consumers showed me why schemas and contract tests are where I'd invest next.
- **Observability is a pipeline you design.** Taking gateway traffic through Kafka and Cassandra into Prometheus taught me about partition modelling, scrape intervals and what a metric actually counts under at-least-once delivery.

---

## Support

If you found this project useful or learned something from it, a ⭐ on the 
repository is appreciated — it helps others discover it.

---

## License

Released under the MIT License. See [`LICENSE`](LICENSE).