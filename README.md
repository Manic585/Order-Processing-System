# Order Processing System

An event-driven, multi-service order processing backend built with Spring Boot 3.
Orders flow through a Kafka pipeline — from placement to inventory reservation to customer notification —
with Redis caching, idempotent writes, and circuit-breaker protection at every layer.

---

## Architecture

```
                         ┌─────────────────────────────────────────────────────────┐
                         │                      CLIENT                              │
                         └────────────────────────────┬────────────────────────────┘
                                                       │ JWT  (Bearer token)
                                                       ▼
                         ┌─────────────────────────────────────────────────────────┐
                         │              ORDER SERVICE  :8080                        │
                         │                                                           │
                         │  POST /api/auth/login      →  issues JWT                │
                         │  POST /api/orders          →  place order (idempotent)  │
                         │  GET  /api/orders/{id}     →  read order                │
                         │  GET  /api/orders/{id}/status                           │
                         │  DELETE /api/orders/{id}   →  cancel order              │
                         │                                                           │
                         │  ┌────────────┐   ┌──────────────┐                     │
                         │  │  Postgres  │   │  Redis Cache │                     │
                         │  │  (public   │   │  order:*     │                     │
                         │  │   schema)  │   │  idempotency:│                     │
                         │  └────────────┘   └──────────────┘                     │
                         └───────┬──────────────────────────▲────────────────────-─┘
                                 │ order.created             │ inventory.updated
                                 │ order.cancelled           │
                                 ▼                           │
                    ┌────────────────────────────────────────┴─────────────────┐
                    │                     APACHE KAFKA                          │
                    │                                                            │
                    │  Topics:  order.created      ──►  inventory-service-group │
                    │           order.created      ──►  notification-service-group│
                    │           order.cancelled    ──►  notification-service-group│
                    │           inventory.updated  ──►  order-service-group     │
                    │                                                            │
                    │  DLTs:    *.DLT  (after 3 failed retries)                │
                    └───────────┬──────────────────────────────────────────────┘
                                │                           │
               order.created    │                           │  order.created
                                ▼                           ▼  order.cancelled
          ┌──────────────────────────────┐     ┌──────────────────────────────────┐
          │  INVENTORY SERVICE  :8081    │     │  NOTIFICATION SERVICE  :8082     │
          │                              │     │                                   │
          │  checkAndReserveInventory    │     │  Kafka consumer                   │
          │  @CircuitBreaker(inventory-db│     │  logs order confirmed /           │
          │  releaseReservation          │     │  cancelled notifications          │
          │                              │     │                                   │
          │  ┌────────────┐  ┌────────┐  │     └──────────────────────────────────┘
          │  │  Postgres  │  │ Redis  │  │
          │  │ (inventory │  │ cache  │  │
          │  │  schema)   │  └────────┘  │
          │  └────────────┘              │
          └──────────────────────────────┘

  Order status lifecycle:
  PENDING ──► CONFIRMED   (inventory reserved successfully)
          ──► FAILED      (inventory unavailable / Kafka publish error)
          ──► CANCELLED   (DELETE /api/orders/{id} while still PENDING)
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Language | Java | 17 |
| Framework | Spring Boot | 3.2.4 |
| Build | Maven | 3.x |
| Database | PostgreSQL | 15 |
| Schema migrations | Flyway | (Spring Boot managed) |
| Cache | Redis | 7 |
| Message broker | Apache Kafka | 3.5.1 |
| Authentication | Spring Security + JWT (JJWT) | 0.12.3 |
| Resilience | Resilience4j (circuit breaker + retry) | 2.1.0 |
| Boilerplate reduction | Lombok | 1.18.30 |

---

## Prerequisites

- **Java 17** — `java -version` should show `17.x`
- **Maven 3.8+** — `mvn -version`
- **PostgreSQL 15** — running locally on port `5432`
- **Redis 7** — running locally on port `6379`
- **Apache Kafka 3.5.1** — running locally on port `9092`

---

## Local Setup

### PostgreSQL

**macOS (Homebrew)**
```bash
brew install postgresql@15
brew services start postgresql@15

# Create user and database
psql postgres -c "CREATE USER opsuser WITH PASSWORD 'opspassword';"
psql postgres -c "CREATE DATABASE opsdb OWNER opsuser;"
psql postgres -c "GRANT ALL PRIVILEGES ON DATABASE opsdb TO opsuser;"

# Create the inventory schema (used by inventory-service)
psql opsdb -c "CREATE SCHEMA IF NOT EXISTS inventory AUTHORIZATION opsuser;"
```

**Linux (apt)**
```bash
sudo apt update
sudo apt install -y postgresql-15 postgresql-client-15

sudo systemctl enable --now postgresql

sudo -u postgres psql -c "CREATE USER opsuser WITH PASSWORD 'opspassword';"
sudo -u postgres psql -c "CREATE DATABASE opsdb OWNER opsuser;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE opsdb TO opsuser;"
sudo -u postgres psql opsdb -c "CREATE SCHEMA IF NOT EXISTS inventory AUTHORIZATION opsuser;"
```

### Redis

**macOS (Homebrew)**
```bash
brew install redis
brew services start redis

# Verify
redis-cli ping   # should return PONG
```

**Linux (apt)**
```bash
sudo apt install -y redis-server
sudo systemctl enable --now redis-server

redis-cli ping
```

### Apache Kafka

**macOS (Homebrew)**
```bash
brew install kafka
# Kafka 3.x uses KRaft mode (no Zookeeper needed)
brew services start zookeeper   # still bundled via brew
brew services start kafka

# Verify
kafka-topics --list --bootstrap-server localhost:9092
```

**Linux (manual — Kafka is not in apt)**
```bash
wget https://downloads.apache.org/kafka/3.5.1/kafka_2.13-3.5.1.tgz
tar -xzf kafka_2.13-3.5.1.tgz
cd kafka_2.13-3.5.1

# Terminal 1 — Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2 — Kafka broker
bin/kafka-server-start.sh config/server.properties
```

**Create the required topics** (once Kafka is running)
```bash
BROKERS="localhost:9092"

for TOPIC in order.created order.cancelled inventory.updated; do
  kafka-topics --create --bootstrap-server $BROKERS \
    --replication-factor 1 --partitions 3 --topic $TOPIC
done

for TOPIC in order.created.DLT order.cancelled.DLT inventory.updated.DLT; do
  kafka-topics --create --bootstrap-server $BROKERS \
    --replication-factor 1 --partitions 1 --topic $TOPIC
done
```

---

## Running the Services

Each service is a self-contained Spring Boot application. Run them in separate terminals.
Flyway migrations run automatically on startup and create all required tables and schemas.

**order-service** — port `8080`
```bash
cd order-service
mvn spring-boot:run
```

**inventory-service** — port `8081`
```bash
cd inventory-service
mvn spring-boot:run
```

**notification-service** — port `8082`
```bash
cd notification-service
mvn spring-boot:run
```

**Health checks**
```bash
curl http://localhost:8080/actuator/health
curl http://localhost:8081/actuator/health
curl http://localhost:8082/actuator/health
```

---

## API Reference

### Authentication

The API uses JWT Bearer authentication. All `/api/orders` endpoints require a valid token.

Two built-in accounts for local development:

| Username | Password | Role |
|---|---|---|
| `user1` | `password1` | USER |
| `admin` | `admin123` | ADMIN |

---

#### POST /api/auth/login

Authenticates and returns a signed JWT (valid for 24 hours).

```bash
curl -s -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"user1","password":"password1"}' | jq .
```

Response `200 OK`:
```json
{
  "token": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyMSIsInJvbGVzIjpbIlJPTEVfVVNFUiJdLCJpYXQiOjE3MDAwMDAwMDAsImV4cCI6MTcwMDA4NjQwMH0.abc123",
  "expiresIn": 86400000
}
```

Save the token for subsequent requests:
```bash
TOKEN=$(curl -s -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"user1","password":"password1"}' | jq -r .token)
```

---

#### POST /api/orders

Places a new order. The `Idempotency-Key` header is **required** — use a UUID generated by the client.
Sending the same key within 24 hours returns the original response without creating a duplicate.

```bash
curl -s -X POST http://localhost:8080/api/orders \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "customerId": "customer-001",
    "items": [
      {
        "productId": "prod-001",
        "productName": "Wireless Keyboard",
        "quantity": 2,
        "unitPrice": 49.99
      },
      {
        "productId": "prod-002",
        "productName": "USB-C Hub",
        "quantity": 1,
        "unitPrice": 34.99
      }
    ]
  }' | jq .
```

Response `201 Created`:
```json
{
  "orderId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "customerId": "customer-001",
  "status": "PENDING",
  "totalAmount": 134.97,
  "items": [
    {
      "orderItemId": "a1b2c3d4-...",
      "productId": "prod-001",
      "productName": "Wireless Keyboard",
      "quantity": 2,
      "unitPrice": 49.99
    },
    {
      "orderItemId": "e5f6g7h8-...",
      "productId": "prod-002",
      "productName": "USB-C Hub",
      "quantity": 1,
      "unitPrice": 34.99
    }
  ],
  "createdAt": "2024-04-13T10:30:00Z",
  "message": "Order placed successfully"
}
```

---

#### GET /api/orders/{orderId}

Returns the full order including all items. Response served from Redis if cached (TTL: 10 min).

```bash
ORDER_ID="3fa85f64-5717-4562-b3fc-2c963f66afa6"

curl -s http://localhost:8080/api/orders/$ORDER_ID \
  -H "Authorization: Bearer $TOKEN" | jq .
```

Response `200 OK`: same shape as the POST response above.

---

#### GET /api/orders/{orderId}/status

Lightweight status-only endpoint. Served from Redis if cached (TTL: 5 min).
Use this for polling order progress rather than fetching the full order.

```bash
curl -s http://localhost:8080/api/orders/$ORDER_ID/status \
  -H "Authorization: Bearer $TOKEN"
```

Response `200 OK`:
```
"CONFIRMED"
```

Possible values: `PENDING` | `CONFIRMED` | `FAILED` | `CANCELLED`

---

#### DELETE /api/orders/{orderId}

Cancels an order. Only `PENDING` orders can be cancelled. The customer ID in the
`X-Customer-Id` header must match the order owner.

```bash
curl -s -X DELETE http://localhost:8080/api/orders/$ORDER_ID \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Customer-Id: customer-001"
```

Response `204 No Content` on success.

Error `409 Conflict` if the order is not in `PENDING` state:
```json
{
  "status": 409,
  "error": "Invalid Order State",
  "message": "Only PENDING orders can be cancelled. Current status: CONFIRMED"
}
```

---

#### GET /api/orders/customer/{customerId}

Returns all orders for a customer (no caching — always reads from DB).

```bash
curl -s http://localhost:8080/api/orders/customer/customer-001 \
  -H "Authorization: Bearer $TOKEN" | jq .
```

---

## How Idempotency Works

Every `POST /api/orders` call **must** include an `Idempotency-Key` header — a client-generated
UUID that uniquely identifies this placement attempt. The system guarantees that no matter how
many times you retry with the same key, exactly one order is created.

```
Client                     Order Service                   PostgreSQL        Redis
  │                              │                              │               │
  │── POST /api/orders ─────────►│                              │               │
  │   Idempotency-Key: key-123   │                              │               │
  │                              │── GET idempotency:key-123 ──►│               │
  │                              │                              │◄── (miss) ────│
  │                              │── SELECT * WHERE             │               │
  │                              │   idempotency_key='key-123' ►│               │
  │                              │◄────────────── (not found) ──│               │
  │                              │── INSERT order ─────────────►│               │
  │                              │── Kafka publish ─────────────────────────────│
  │                              │── SET idempotency:key-123 ───────────────────►
  │◄── 201 Created ──────────────│   (TTL 24h)                  │               │
  │                              │                              │               │
  │   (network drop, client retries)                            │               │
  │                              │                              │               │
  │── POST /api/orders ─────────►│                              │               │
  │   Idempotency-Key: key-123   │                              │               │
  │                              │── GET idempotency:key-123 ──────────────────►│
  │                              │◄────────────────── (HIT — cached response) ──│
  │◄── 201 Created ──────────────│  (original response, no DB write)            │
```

**Three-layer protection:**

| Layer | Mechanism | Handles |
|---|---|---|
| 1. Redis | `GET idempotency:{key}` (TTL 24h) | Fast path — most retries hit here |
| 2. DB unique constraint | `UNIQUE(idempotency_key)` column | Cache miss or Redis down |
| 3. Exception handler | `DataIntegrityViolationException` → `OrderAlreadyExistsException` | Concurrent duplicate requests racing past both checks |

> **Note on Redis + transactions:** `OrderCacheService` is called inside a `@Transactional` method
> with `enableTransactionSupport=true`. Spring wraps Redis commands in `MULTI/EXEC`, meaning reads
> return `null` during the transaction. The Redis idempotency check is therefore a no-op on the
> first request — **the DB unique constraint is the true safety net**. Redis becomes the fast-path
> on all subsequent retries, once the key is stored after `EXEC`.

---

## Kafka Event Flow (End-to-End)

```
1. Client calls POST /api/orders
   └─► OrderService.placeOrder()
       ├─► Saves order with status=PENDING to PostgreSQL
       ├─► Calls eventProducer.publishOrderCreated().join()
       │     orderId used as partition key → ordering guaranteed per order
       │     acks=all + enable.idempotence=true → exactly-once broker write
       └─► On Kafka failure: sets status=FAILED, throws (noRollbackFor keeps the FAILED commit)

2. Kafka topic: order.created
   │
   ├─► CONSUMER GROUP: inventory-service-group
   │   └─► InventoryEventConsumer.handleOrderCreated()
   │       ├─► ProductService.checkAndReserveInventory()  [@CircuitBreaker(inventory-db)]
   │       │     ├─► Fetches each product with SELECT ... FOR UPDATE (optimistic lock)
   │       │     ├─► Checks available_quantity >= requested for ALL items
   │       │     ├─► If any item fails: returns InventoryCheckResult.failure(reason)
   │       │     └─► If all OK: decrements available_qty, increments reserved_qty (single TX)
   │       ├─► Publishes InventoryUpdatedEvent {orderId, success, failureReason}
   │       │     to topic: inventory.updated  (blocks with .join() before acking)
   │       └─► acknowledgment.acknowledge()
   │
   └─► CONSUMER GROUP: notification-service-group
       └─► OrderEventConsumer.handleOrderCreated()
           ├─► NotificationService.sendOrderConfirmation(orderId, customerId)
           │     Logs order confirmation notification
           └─► acknowledgment.acknowledge()

3. Kafka topic: inventory.updated
   └─► CONSUMER GROUP: order-service-group
       └─► InventoryEventConsumer.handleInventoryUpdated()
           ├─► event.success() == true  → OrderService.updateOrderStatus(CONFIRMED)
           │     evicts order cache, updates status cache
           └─► event.success() == false → OrderService.updateOrderStatus(FAILED)

4. Kafka topic: order.cancelled  (on DELETE /api/orders/{id})
   └─► CONSUMER GROUP: notification-service-group
       └─► OrderEventConsumer.handleOrderCancelled()
           └─► Logs order cancellation notification

5. Dead Letter Topics (*.DLT)
   If a consumer throws after 3 DefaultErrorHandler retries, the record is
   routed to <topic>.DLT for manual inspection and replay.
```

**Message acknowledgment strategy:** all consumers use `ack-mode: MANUAL_IMMEDIATE`.
A message is acknowledged **only after** the downstream side-effect (DB write or outbound
Kafka publish) completes successfully. This prevents silent message loss at the cost of
at-least-once delivery — consumers are written to be idempotent.

---

## Project Structure

```
order-processing-system/
├── order-service/                   # REST API, JWT auth, idempotency, cache-aside
│   └── src/main/resources/
│       ├── application.yml          # Local defaults
│       ├── application-prod.yml     # Prod overrides (env vars, restricted actuator)
│       └── db/migration/            # Flyway migrations (public schema)
├── inventory-service/               # Stock reservation, circuit breaker, Kafka
│   └── src/main/resources/
│       ├── application.yml
│       ├── application-prod.yml
│       └── db/migration/            # Flyway migrations (inventory schema)
└── notification-service/            # Kafka consumer, logs order notifications
    └── src/main/resources/
        ├── application.yml
        └── application-prod.yml
```

---