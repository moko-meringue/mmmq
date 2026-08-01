<!-- @formatter:off -->

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MMMQ (Moko-Meringue's Message Queue) is an educational Spring Boot message queue system built from scratch. It is a Gradle multi-module library published via JitPack.

## Build & Test Commands

```bash
# Build all modules
./gradlew build

# Run tests
./gradlew test

# Run tests for a specific module
./gradlew :core:test
./gradlew :producer:test
./gradlew :consumer:test
./gradlew :broker:test

# Publish to Maven Local
./gradlew publishToMavenLocal
```

Requirements: Java 17+, Spring Boot 3.2.0+, Spring Web starter.

## Module Architecture

```
core/       # Shared types: Message, Topic, TopicPattern, Acknowledgement, ConsumerId, Metadata, @Nullable
producer/   # Producer bean + Gateway (RestClient → POST /mmmq/messages to Broker)
consumer/   # Consumer REST controller + HandlerExecutionContainer + HandlerExecution
broker/     # FrontDispatcher + DispatcherContainer + Dispatcher + Sender + TopicQueueContainer
```

**Inter-module dependencies:** `producer → core`, `consumer → core`, `broker → core`. No external Spring dependencies in `core`.

## Message Flow

```
Producer.produce(message)
  → HTTP POST /mmmq/messages → Broker
  → FrontDispatcher.dispatch(message)
    → TopicQueueContainer.getOrCreate(topic) → TopicQueue (SegmentChain, per-topic queue)
    → TopicQueue.offer(message)
    → DispatcherContainer.getSubscribers(queue) → matched Dispatchers
    → each matched Dispatcher.dispatch(queue)  [package-private, async trigger]
      → WorkerPool submits drain(queue) on a single worker thread per (Dispatcher, queue)
      → drain loop: peek → deliver → commit (offset persisted to <consumerId>.checkpoint)
        → Sender.send(message, consumerId, maxRetry=3)
          → Metadata.setConsumerId + HTTP POST /mmmq/messages with header `mmmq-consumer-id: <consumerId>`
            → Consumer (REST controller, synchronous)
              → Metadata.getConsumerId() from request headers
              → HandlerExecutionContainer.find(consumerId) → HandlerExecution
              → execution.execute(message)  [synchronous on Tomcat thread]
              → ACK (success) | NACK (missing/invalid id, no handler, exception)
        → On NACK: dispatcher retries up to MAX_NACK_RETRY_COUNT (3); drops after exhaustion
        → On communication failure: exponential backoff (1s → 2s → ... → 60s, infinite retry)
```

Bootstrap: `DispatcherContainer` reads `dispatchers.json` in its constructor and creates every `Dispatcher`. `TopicQueueBootstrapper` (SmartInitializingSingleton) then restores persisted topic queues at startup and calls `TopicQueueContainer.getOrCreate` for each, which in turn calls `DispatcherContainer.register(queue)` to bind matched Dispatchers.

## Key Design Points

- **Send unit:** A `Dispatcher` is one `(consumerId, host, pattern)` triple — the unit that sends matched messages to one Consumer endpoint. What the Consumer does with them is outside the Broker's concern; the Broker never references `HandlerExecution`. Multiple Dispatchers may target the same Consumer host with different consumerIds.
- **Pattern matching:** `TopicPattern` uses `org.mmmq.core.util.PatternMatcher` (e.g. `order.*`, `**`), a copy of Spring's `AntPathMatcher` — `core` has no Spring dependency, so the algorithm is vendored rather than imported. Each Dispatcher holds a single pattern; `Dispatcher.canDispatch(topic)` checks it. Consumer-side routing is by `consumerId` header only — no pattern matching on Consumer.
- **Synchronous response:** Consumer Controller runs `HandlerExecution.execute(message)` directly on the Tomcat request thread. ACK/NACK in the same HTTP response. No queue/worker pool on Consumer.
- **Atomicity & isolation:** A failing or slow Consumer only blocks its own Dispatcher's drain loop; other Dispatchers are independent threads.
- **Uniqueness on both sides:** Consumer rejects duplicate HE ids at registration (`HandlerExecutionContainer.add`). Broker rejects duplicate consumerIds when `DispatcherContainer` loads the file and on every runtime addition (`DuplicateConsumerIdException`).
- **Identifier:** `ConsumerId` is a `record` in `org.mmmq.core.identifier`, validated by regex `[A-Za-z0-9._-]+` in its compact constructor. Used everywhere (Dispatcher, Sender, HE, container) as a typed value object.
- **Wire format:** `Metadata` (in `org.mmmq.core.metadata`) encapsulates HTTP header transport. Header name `mmmq-consumer-id` (lowercase per HTTP/2 spec).
- **Thread model:** `Dispatcher.WorkerPool` creates one single-threaded `ThreadPoolExecutor` per (Dispatcher, TopicQueue). Consumer uses Tomcat's request thread pool directly.
- **Retry layers:** Producer retries on Broker NACK (default 3). Dispatcher/Sender retries on Consumer NACK (max 3). Dispatcher retries indefinitely with exponential backoff (1s~60s) on network/comm failure.
- **Handler types:** `HandlerExecution` is an interface with `ConsumerId id()` and `void execute(Message)`. Two implementations:
  - `MethodExecution`: invokes an annotated method via reflection with JSON deserialization.
  - `InterfaceExecution`: invokes `MMMQListener<T>.handle()` on a bean implementing `MMMQListener<T>`.
- **Producer constructors:** `new Producer(host)` (default retry count 3) or `new Producer(host, maxRetryCount)` for a custom value.

## Consumer Handler Registration

```java
// Annotation-based (method level)
@MMMQListener(id = "order-created")
public void handle(Order order) { ... }

// Interface-based (class level)
@Service
public class OrderService implements MMMQListener<Order> {

    @Override
    public String id() {
        return "order-created";
    }

    @Override
    public void handle(Order order) { ... }
}
```

Both forms require an explicit string `id` matching the regex `[A-Za-z0-9._-]+`. Duplicate ids at startup throw `IllegalStateException` and fail bean initialization.

## Broker Dispatcher Registration

Dispatchers are defined in a JSON file at `{mmmq.broker.persistence.root-dir}/dispatchers.json` (root-dir default `./mmmq`); the path is fixed and not individually configurable. `DispatcherContainer` reads it at construction and owns every `Dispatcher` instance — Dispatchers are not Spring beans. The top level is an array; one entry maps to exactly one `consumerId` and one pattern.

```json
[
  {
    "consumerId": "order-created",
    "host": "http://consumer-host:8080",
    "pattern": "order.created"
  }
]
```

`host` is an absolute URL of the form `scheme://address:port` and nothing else. The scheme must be `http` or `https` (case-insensitive) and the port is required — a consumer usually listens on a non-standard port, so a missing port is rejected instead of silently falling back to 80/443. A path, query, userInfo, or fragment is rejected too: `Host.toUri()` only round-trips `scheme://address:port`, and a path would not vanish harmlessly — `RestClient` appends `/mmmq/messages` to its baseUrl path, so keeping it would change routing and dropping it would silently ignore what the user wrote. When the file is absent, an empty `[]` file is created and the broker boots with no dispatchers. Invalid entries — duplicate `consumerId`, unsupported scheme, malformed JSON — fail context startup (fail-fast). Validation errors surface as `IllegalArgumentException` from the `core` value types, so the message names the *parameter* being validated (`url must be an absolute URL, but was: …`) rather than the JSON field (`host`); the offending value is echoed, and `core` stays ignorant of the Broker's wire schema. Each Dispatcher gets its own `<consumerId>.checkpoint` file under the per-topic storage directory.

### Runtime management

`DispatcherController` exposes CRUD over the same concept, but **not the same type**. The HTTP layer speaks `dispatcher.api.DispatcherDefinition`; the file speaks `dispatcher.storage.DispatcherEntry`. Both are method-less records with identical components — the duplication is deliberate, so an API-shaping annotation (`@Valid`, `@JsonProperty`) cannot leak into the on-disk format and a file-format change cannot break the HTTP contract. Neither imports `Dispatcher`; `DispatcherContainer` owns every conversion in private helpers, which is what lets `Dispatcher`'s accessors stay package-private.

Changes are written to `dispatchers.json` (temp file + `ATOMIC_MOVE`) before the in-memory state is touched, so they survive a restart.

```
GET    /mmmq/dispatchers               200
POST   /mmmq/dispatchers               201 / 400 / 409
PUT    /mmmq/dispatchers/{consumerId}  200 / 400 / 404
DELETE /mmmq/dispatchers/{consumerId}  204 / 400 / 404
```

PUT takes only `host` and `pattern` in the body — `consumerId` is the identifier, not a mutable field.

`DispatcherController` maps failures itself rather than letting them escape: `IllegalArgumentException` and `HttpMessageNotReadableException` → 400, `DuplicateConsumerIdException` → 409, `DispatcherNotFoundException` → 404, and a catch-all `RuntimeException` → 500 with a fixed body (the real cause goes to the log — an unexpected exception's message can carry server paths). The catch-all is deliberately `RuntimeException`, not `Exception`: `HttpMediaTypeNotSupportedException` extends `ServletException` (checked), so an `Exception` handler would swallow it and turn a correct 415 into a 500 — measured, not assumed.

The guarantee this buys is bounded: **failures raised once a handler method has been entered are answered by the Broker.** 405 and 415 are thrown during handler mapping and argument resolution, before any `@ExceptionHandler` in this controller can see them, so those still reach the host application's `/error`. Closing that would require `ResponseEntityExceptionHandler`, which is `@ControllerAdvice`-based and would swallow the host's own controllers — the reason `@ControllerAdvice` and `@ResponseStatus` are banned here in the first place. Mutations are serialized by a single `ReentrantLock` inside `DispatcherContainer`; the message hot path (`getSubscribers`) stays lock-free.

A new subscription starts at the log **tail**, so attaching a consumer at runtime does not replay the existing backlog. When a subscription ends — dispatcher deleted, or pattern narrowed so a topic drops out — its `<consumerId>.checkpoint` is deleted too.

### Package layout (broker)

```
org.mmmq.broker.dispatcher
├── Dispatcher · DispatcherContainer · FrontDispatcher   domain
├── DispatcherSnapshot                                   domain read model
├── api/       DispatcherController · DispatcherDefinition · DispatcherRoute
├── storage/   DispatchersFile · DispatcherEntry
├── exception/ DispatcherNotFoundException · DuplicateConsumerIdException
└── sender/    Sender
```

Every package dependency runs one way: `api → dispatcher`, `dispatcher → storage`, `storage → persistence`. No package imports another that imports it back.

`DispatcherSnapshot(ConsumerId, Host, TopicPattern)` is what makes that possible. `DispatcherContainer` returns it — never `api.DispatcherDefinition` — so the domain never names a UI type. Its components are **domain value objects, not strings**, which is what distinguishes it from the two wire records; a wire record cannot be mistaken for it and vice versa (the compiler enforces this).

`api.DispatcherDefinition` owns `from(DispatcherSnapshot)` because the container cannot build it without acquiring the very dependency we removed. `storage.DispatcherEntry` stays method-less because the container *can* build it — writing the file is infrastructure the container owns.

## Code Style Guide

### Formatting & Layout
- Indentation: 4 spaces. Line length: max 120 characters.
- If a method signature or call exceeds 120 chars, place EVERY parameter on a new line.
- ALWAYS use curly braces `{}` for control structures, even single-line statements.
- Class/Interface: insert a blank line immediately after the opening brace `{`.
- Record: place each component on a new line. Insert a blank line after `{` if the record has methods.
- Annotation stacking: pyramid order (shortest → longest, top → bottom).
- Do NOT use `this` unless needed to resolve naming conflicts.

### Lombok
- Allowed: `@Getter`, `@RequiredArgsConstructor`, `@NoArgsConstructor(access = AccessLevel.PROTECTED)`, `@Slf4j`.
- Forbidden: NEVER use `@Setter` or `@Data`.
- Note: `broker` module does NOT depend on Lombok — use explicit `Logger` declarations there.

### Immutability
- Use `private final` fields with `@RequiredArgsConstructor` for DI.
- NEVER use `final` for local variables.
- Return immutable collections (`Stream.toList()`, `List.copyOf()`).

### Optional & Streams
- Use `orElseThrow()` / `ifPresent()`. NEVER use `isPresent()` + `get()`.
- Prefer Stream API over traditional for/while loops.
- For nullable returns, use `@Nullable` from `org.mmmq.core.annotation` (project-defined, no external dep).

### Clean Code
- Comments explain WHY, not WHAT. Code must be self-documenting.
- Apply SRP for method extraction, but don't over-fragment (avoid 3~5 line fragments).
- Objects autonomously manage their own state. Avoid direct field access from outside.

## Naming Convention

### Packages
- Format: lowercase. Structure: `org.mmmq.{module}.{subpackage}`
- `{module}`: `core`, `producer`, `consumer`, `broker`
- Example: `org.mmmq.broker.dispatcher`, `org.mmmq.core.message`, `org.mmmq.core.identifier`

### Classes and Interfaces
- Format: PascalCase, nouns or noun phrases.
- Forbidden suffixes: `Client`, `Manager`, `Helper`, `Util` — avoid unless unavoidable.
- `Container` is used for components that own and manage a collection (e.g., `DispatcherContainer`, `TopicQueueContainer`, `HandlerExecutionContainer`).

### Methods
- Format: camelCase, start with verbs.
- Use domain verbs reflecting the messaging context: `produce`, `dispatch`, `send`, `receive`, `handle`, `register`.
- Avoid generic CRUD verbs (create/read/update/delete) — MMMQ is not a REST CRUD app.

### Variables and Constants
- camelCase for variables. Names must clearly state purpose, regardless of length.
- Lambda variables: NEVER use single-letter names. Use descriptive names.
- Constants: `UPPER_SNAKE_CASE` for `static final` fields.
- Map field names: prefer either a single domain noun (e.g. `dispatchers`, `subscriptions`) or `<key>To<value>` (e.g. `handlerIdToDispatcher`) — avoid `by` prefix.

### API Endpoints
- MMMQ exposes `POST /mmmq/messages` as its primary endpoint.
- If new endpoints are added: lowercase, kebab-case for multi-word paths.

### HTTP Headers
- Header names lowercase per HTTP/2 spec. Defined as constants in `Metadata` (e.g. `mmmq-consumer-id`).
