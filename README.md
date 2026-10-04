# BadrLink - Realtime Gateway

**The realtime messaging gateway powering live chat, typing indicators, presence, and STOMP communication across BadrLink.**

[![Spring Boot](https://img.shields.io/badge/Spring_Boot-4.1.1-6DB33F?style=flat-square\&logo=springboot\&logoColor=white)](https://spring.io/projects/spring-boot)
[![Java](https://img.shields.io/badge/Java-21-ED8B00?style=flat-square\&logo=openjdk\&logoColor=white)](https://www.oracle.com/java/)
[![WebSocket](https://img.shields.io/badge/WebSocket-STOMP-010101?style=flat-square\&logo=websocket\&logoColor=white)](https://stomp.github.io/)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat-square\&logo=redis\&logoColor=white)](https://redis.io/)
[![Kafka](https://img.shields.io/badge/Kafka-Events-231F20?style=flat-square\&logo=apachekafka\&logoColor=white)](https://kafka.apache.org/)
[![Bucket4j](https://img.shields.io/badge/Bucket4j-Rate_Limiting-6C757D?style=flat-square)](https://bucket4j.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square\&logo=docker\&logoColor=white)](https://www.docker.com/)
[![Maven](https://img.shields.io/badge/Maven-Build-C71A36?style=flat-square\&logo=apachemaven\&logoColor=white)](https://maven.apache.org/)

A dedicated realtime service responsible for STOMP connections, live message delivery, typing indicators, presence tracking, and communication with the Chat and User services.

---

## Responsibilities

This service handles:

* STOMP over WebSocket communication
* Client authentication during `CONNECT`
* Room subscription authorization
* Realtime message delivery
* Typing indicators
* User presence tracking
* Kafka-backed, deduplicated message fan-out (`badrlink.chat.message.v1`)
* Distributed typing fan-out through Redis
* Per-user realtime rate limiting
* Last-seen updates after disconnects

It **does not store messages or room data**. Chat remains responsible for persistence and room membership.

---

## Architecture

The Realtime Gateway sits behind the API Gateway, persists sends through Chat, and consumes Chat's `MESSAGE_SENT` events from Kafka to broadcast to local subscribers.

```text
                         ┌─────────────────────┐
                         │       Client        │
                         │   WebSocket/STOMP   │
                         └──────────┬──────────┘
                                    │
                              /ws/** │
                                    ▼
                         ┌─────────────────────┐
                         │    API Gateway      │
                         │       :8080         │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │  Realtime Gateway   │
                         │       :8084         │
                         ├─────────────────────┤
                         │ STOMP               │
                         │ Authentication      │
                         │ Room subscriptions  │
                         │ Message persistence │
                         │ Kafka MESSAGE_SENT  │
                         │ Dedup + broadcast   │
                         │ Typing indicators   │
                         │ Presence            │
                         └──┬────────┬─────┬───┘
                            │        │     │
              ┌─────────────┘        │     └────────────┐
              ▼                      ▼                  ▼
    ┌─────────────────┐    ┌─────────────────┐  ┌─────────────────┐
    │   Chat Service  │    │   User Service  │  │      Kafka      │
    │      :8083      │    │      :8082      │  │      :9092      │
    └─────────────────┘    └─────────────────┘  └─────────────────┘

                         ┌─────────────────────┐
                         │       Redis         │
                         ├─────────────────────┤
                         │ Presence            │
                         │ Typing pub/sub      │
                         │ Message dedup keys  │
                         │ Rate limiting       │
                         └─────────────────────┘
```

The service has **no database**. Redis is used for shared presence state, distributed rate limits, message deduplication claims, and typing pub/sub; Kafka carries message fan-out.

Centralized references: [`/docs/API_ENDPOINTS.md`](../docs/API_ENDPOINTS.md), [`/docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md), [`/docs/INCOHERENCES_AND_RESOLUTIONS.md`](../docs/INCOHERENCES_AND_RESOLUTIONS.md).

---

## Realtime Flow

### Message

Persistence and broadcast are decoupled:

```text
Client
  │
  │ SEND /app/chat.sendMessage
  ▼
Realtime Gateway
  │
  │ rate limit (fail closed)
  ▼
Chat Service
  │
  │ POST /rooms/{roomId}/messages
  ▼
Message persisted + MESSAGE_SENT emitted
  │
  ▼
Kafka badrlink.chat.message.v1
  │
  ▼
Realtime Gateway consumer (group realtime-gateway)
  │
  │ SET message_dedup:{messageId} 1 NX EX 60
  ▼
/topic/rooms/{roomId}
```

The sender's `Authorization` token is forwarded to Chat. The Realtime Gateway does not create or forward a separate sender identity. Chat's validation, membership, block, and mute errors still reach the sender as STOMP error frames.

### Typing

Typing indicators do not go through Chat:

```text
Client
  │
  │ SEND /app/chat.typing
  ▼
Realtime Gateway
  │
  │ rate limit
  ▼
Redis Pub/Sub
  │
  ▼
/topic/rooms/{roomId}/typing
```

Typing events are temporary and are never persisted.

---

## API

### Connection

| Path            | Transport          | Description                    |
| --------------- | ------------------ | ------------------------------ |
| `/ws`           | WebSocket / SockJS | Main STOMP endpoint            |
| `/ws/websocket` | WebSocket          | Raw SockJS WebSocket transport |
| `/ws/info`      | HTTP               | SockJS capability information  |

SockJS also exposes its standard fallback transports under `/ws/{server}/{session}/...`.

### Authentication

The API Gateway authenticates the WebSocket handshake, but the Realtime Gateway independently authenticates the STOMP `CONNECT` frame.

```text
CONNECT
accept-version:1.2
host:localhost
Authorization:Bearer <jwt>

```

The JWT is validated against Auth's public keys at `AUTH_JWKS_URI` (`/oauth2/jwks`); the signature, expiry, issuer, and `aud` (default `messaging-api`) are all enforced. `AUTH_ISSUER` must equal Auth's `JWT_ISSUER`. See `docs/adr/0010-rs256-access-token-signing.md` and `0011-canonical-jwks-path.md`.

The token's `sub` becomes the authenticated STOMP principal.

### Send

| Method | Destination             | Description             |
| ------ | ----------------------- | ----------------------- |
| `SEND` | `/app/chat.sendMessage` | Send a chat message     |
| `SEND` | `/app/chat.typing`      | Send a typing indicator |

#### Message payload

```json
{
  "roomId": "42",
  "type": "TEXT",
  "content": "hello"
}
```

#### Typing payload

```json
{
  "roomId": "42"
}
```

### Subscribe

| Destination                    | Description                            |
| ------------------------------ | -------------------------------------- |
| `/topic/rooms/{roomId}`        | Receive room messages                  |
| `/topic/rooms/{roomId}/typing` | Receive typing indicators              |
| `/user/queue/errors`           | Receive errors for the current session |

Room subscriptions are checked against Chat before they are accepted.

The typing topic uses the same room membership check as the message topic.

---

## Rate Limiting

Realtime actions use distributed rate limits backed by Redis.

| Action   | Rate | Burst |
| -------- | ---- | ----- |
| Messages | 10/s | 20    |
| Typing   | 5/s  | 10    |

Message sends **fail closed** when Redis is unavailable. This prevents messages from bypassing the configured rate limit.

Typing events **fail open** because they are temporary and can safely be dropped.

---

## Presence

Presence is maintained using Redis keys:

```text
presence:{userId}
```

Each key contains:

```text
ONLINE
```

with a default TTL of 30 seconds.

Presence is:

* Created after a successful STOMP `CONNECT`
* Refreshed during authenticated activity
* Reconciled every 5 seconds for active local sessions
* Removed after the user's last local session disconnects

On disconnect, the service also sends:

```text
PATCH /internal/users/{id}/last-seen
```

to the User Service using the internal service token (`X-Internal-Token`, value of `USER_SERVICE_INTERNAL_TOKEN` — must equal the User service's `INTERNAL_HMAC_SECRET`).

Presence is best-effort. Redis failures do not disconnect users or prevent message sending when the message rate-limit check itself is still available.

> **Note:** Presence is currently an internal state mechanism. There is no `/topic/presence/*` destination for broadcasting online/offline changes to clients.

---


## Error Handling

Errors are delivered to the affected session through:

```text
/user/queue/errors
```

Example:

```json
{
  "timestamp": "2026-09-20T12:00:00Z",
  "status": 403,
  "code": "ROOM_ACCESS_DENIED",
  "message": "The caller is not a participant of this room",
  "destination": "/app/chat.sendMessage"
}
```

### Service-owned errors

| Code                       | Status | Description                                              |
| -------------------------- | ------ | -------------------------------------------------------- |
| `UNAUTHENTICATED`          | `401`  | Missing or invalid JWT                                   |
| `RATE_LIMITED`             | `429`  | Message rate limit exceeded                              |
| `UPSTREAM_UNAVAILABLE`     | `503`  | Chat, User, or authentication infrastructure unavailable |
| `RATE_LIMITER_UNAVAILABLE` | `503`  | Redis unavailable during message rate-limit check        |

Chat errors are forwarded using the code and message returned by Chat.

Examples include:

* `ROOM_ACCESS_DENIED`
* `ROOM_NOT_FOUND`
* `BLOCKED_RELATIONSHIP`
* `PARTICIPANT_MUTED`
* `MESSAGE_CONTENT_REQUIRED`
* `MESSAGE_TOO_LONG`

---

## Getting Started

### Requirements

* Java 21
* Docker
* Redis 7
* Kafka (shared broker; `KAFKA_BOOTSTRAP_SERVERS`)
* Maven (or the included Maven Wrapper)

### Environment Configuration

Copy the example environment file:

```bash
cp .env.example .env
```

Configure the required services:

```text
REDIS_HOST=localhost
REDIS_PORT=6379

KAFKA_ENABLED=true
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

AUTH_JWKS_URI=http://localhost:8081/oauth2/jwks
AUTH_ISSUER=http://localhost:8081
AUTH_AUDIENCE=messaging-api

UPSTREAM_CHAT_SERVICE=http://localhost:8083
UPSTREAM_USER_SERVICE=http://localhost:8082

USER_SERVICE_INTERNAL_TOKEN=<token>
```

See `.env.example` for the complete configuration.


### Start Redis

```bash
docker run -d --name badrlink-redis -p 6379:6379 redis:7-alpine
```

Start a Kafka broker using any one service's compose file (only one is needed per deployment).

### Run the service

```bash
./mvnw spring-boot:run
```

The realtime gateway will be available at:

```text
http://localhost:8084
```

The STOMP endpoint is:

```text
ws://localhost:8084/ws
```

The raw WebSocket transport used by integration tests is:

```text
ws://localhost:8084/ws/websocket
```

### Build

```bash
./mvnw -B clean package
```

To build without tests:

```bash
./mvnw -B -DskipTests package
```

### Run tests

```bash
./mvnw -B clean test
```

Integration tests require Docker because they use Testcontainers. The unit tests (`RedisChannelsTest`, `MessageFanOutServiceTest`, `RealtimeMessageConsumerTest`) run without Docker.

---

## Testing

The test suite uses real Redis containers and MockWebServer instances for the Auth, Chat, and User services.

The main test areas include:

* STOMP authentication
* JWT validation
* Room subscription authorization
* Message relay and error mapping
* Kafka `MESSAGE_SENT` dispatch
* Distributed message deduplication
* Rate limiting
* Presence lifecycle
* Multi-instance Redis failure handling
* Redis channel mapping

The test setup also generates in-memory RS256 tokens and serves a test JWKS endpoint.

---

## Project Structure

```text
src/
├── main/
│   ├── java/
│   │   └── com/ziyadsamhaoui/messagingrealtimegateway/
│   │       ├── config/
│   │       ├── stomp/
│   │       ├── service/
│   │       ├── messaging/
│   │       ├── client/
│   │       ├── dto/
│   │       └── exception/
│   └── resources/
│       └── application.yaml
└── test/
    └── java/
```

---

## Current Scope & Limitations

The current realtime gateway provides:

* STOMP messaging over WebSocket and SockJS
* Independent JWT validation
* Room subscription authorization through Chat
* Kafka-backed, deduplicated message delivery
* Typing indicators
* Redis-based presence
* Distributed rate limiting
* Last-seen updates

It currently does **not** provide:

* Persistent message storage
* Room management
* Peer-visible presence broadcasts
* Direct database access
* Message retrying

Chat remains responsible for persistent messaging and room data, while the API Gateway remains responsible for the external `/ws/**` edge route and handshake rate limiting.

---

## Related Services

BadrLink is split into several independent services:

| Service          | Port   | Responsibility                                       |
| ---------------- | ------ | ---------------------------------------------------- |
| API Gateway      | `8080` | External routing, authentication edge, rate limiting |
| Auth Service     | `8081` | Authentication, credentials, JWT                     |
| User Service     | `8082` | Profiles, blocks, connections                        |
| Chat Service     | `8083` | Rooms, participants, messages                        |
| Realtime Gateway | `8084` | STOMP, WebSocket, realtime delivery                  |
| Notification     | `8085` | In-app notifications, Web Push                       |
| Kafka            | `9092` | Event backbone (message fan-out)                     |
| Redis            | `6379` | Shared realtime state and typing transport           |
