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

Bootstrap: `TopicQueueBootstrapper` (SmartInitializingSingleton) restores persisted topic queues at startup and calls `TopicQueueContainer.getOrCreate` for each, which in turn calls `DispatcherContainer.register(queue)` to bind matched Dispatchers.

## Key Design Points

- **HE-level proxy:** Each `Dispatcher` is a proxy for exactly one HandlerExecution. 1 ID = 1 Dispatcher = 1 HE. Multiple Dispatchers may target the same Consumer host but with different consumerIds.
- **Pattern matching:** `TopicPattern` uses Spring's `AntPathMatcher` (e.g. `order.*`, `**`). Each Dispatcher holds a single pattern; `Dispatcher.canDispatch(topic)` checks it. Consumer-side routing is by `consumerId` header only — no pattern matching on Consumer.
- **Synchronous response:** Consumer Controller runs `HandlerExecution.execute(message)` directly on the Tomcat request thread. ACK/NACK in the same HTTP response. No queue/worker pool on Consumer.
- **Atomicity & isolation:** A failing or slow HE only blocks its own Dispatcher's drain loop; other Dispatchers are independent threads.
- **Uniqueness on both sides:** Consumer rejects duplicate HE ids at registration (`HandlerExecutionContainer.add`). Broker rejects duplicate consumerIds at `DispatcherContainer` construction.
- **Identifier:** `ConsumerId` is a `record` in `org.mmmq.core.identifier`, validated by regex `[A-Za-z0-9._-]+` in its compact constructor. Used everywhere (Dispatcher, Sender, HE, container) as a typed value object.
- **Wire format:** `Metadata` (in `org.mmmq.core.metadata`) encapsulates HTTP header transport. Header name `mmmq-consumer-id` (lowercase per HTTP/2 spec).
- **Thread model:** `Dispatcher.WorkerPool` creates one single-threaded `ThreadPoolExecutor` per (Dispatcher, TopicQueue). Consumer uses Tomcat's request thread pool directly.
- **Retry layers:** Producer retries on Broker NACK (default 3). Dispatcher/Sender retries on Consumer NACK (max 3). Dispatcher retries indefinitely with exponential backoff (1s~60s) on network/comm failure.
- **Handler types:** `HandlerExecution` is an interface with `ConsumerId id()` and `void execute(Message)`. Two implementations:
  - `MethodExecution`: invokes an annotated method via reflection with JSON deserialization.
  - `InterfaceExecution`: invokes `MMMQListener<T>.handle()` on a bean implementing `MMMQListener<T>`.
- **Producer Builder:** `Producer.builder(host).maxRetryCount(n).build()` for custom retry count.

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

```java
// Each Dispatcher proxies exactly one HandlerExecution (matched by consumerId).
@Bean
public Dispatcher orderCreatedDispatcher() {
    return new Dispatcher(
        new Host("http", "consumer-host", 8080),
        new ConsumerId("order-created"),
        new TopicPattern("order.created")
    );
}
```

Broker rejects duplicate consumerIds across all `Dispatcher` beans at startup via `DispatcherContainer`. Each Dispatcher gets its own `<consumerId>.checkpoint` file under the per-topic storage directory.

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
