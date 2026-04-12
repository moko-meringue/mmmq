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
core/       # Shared types: Message, Topic, Pattern, Acknowledgement
producer/   # Producer bean + Gateway (RestClient → POST /messages to Broker)
consumer/   # Consumer REST endpoint + FrontHandler + HandlerExecution
broker/     # Broker REST endpoint + FrontDispatcher + Dispatcher + DeadLetterQueue
```

**Inter-module dependencies:** `producer → core`, `consumer → core`, `broker → core`

## Message Flow

```
Producer.produce(message)
  → HTTP POST /messages → Broker
  → FrontDispatcher.dispatch(message)
    → Filters Dispatchers by Pattern.matches(topic)  [Ant-style wildcard, e.g. order.*]
    → TopicQueueRegistry.get(topic) → TopicQueue (SegmentChain, segment capacity 1000)
    → TopicQueue.offer(message) + publishes MessageArrivedEvent
    → Dispatcher (per Consumer): subscribes to TopicQueue, single Subscription worker thread
      → Drains TopicQueue from per-Subscription Offset
      → Sender.send(message, maxNackRetry=3)  [NACK → retry up to 3x]
        → HTTP POST /messages → Consumer
          → FrontHandler: ArrayBlockingQueue(1000) + single worker thread
            → ThreadPoolExecutor(2~5) for handler execution
              → HandlerExecutions.getExecutions(message)  [filters by pattern.matches(topic)]
                → MethodExecution (@MMMQListener annotation) or InterfaceExecution (MMMQListener<T>)
      → On comm failure: exponential backoff (1s→2s→...→60s, infinite retry)
      → On NACK exhausted: DeadLetterQueue.add(DeadLetter(message))
        → CounterDeadLetterQueue (triggers at N messages) or TimerDeadLetterQueue (triggers on interval)
          → DeadLetterHandler (e.g. DeadLetterFileWriter → JSON file)
```

## Key Design Points

- **Pattern matching:** `Pattern` uses Spring's `AntPathMatcher`. Default `@MMMQListener` value is `"**"` (matches all). Consumer routing is keyed on `message.topic()` — `HandlerExecution.supports(message)` checks `pattern.matches(message.topic())`.
- **Thread isolation:** Each `Dispatcher` Subscription has a single worker thread (not a pool); `FrontHandler` has a `ThreadPoolExecutor` (2–5 threads) for handler execution.
- **Retry layers:** Producer retries on Broker NACK (default 3). Dispatcher/Sender retries on Consumer NACK (max 3). Dispatcher retries indefinitely with exponential backoff (1s~60s) on network/comm failure.
- **DeadLetter:** `DeadLetter` holds only `Message` (no cause/exception). Created when Sender exhausts NACK retries.
- **Handler types:** `HandlerExecution` is abstract — `MethodExecution` invokes annotated methods via reflection with JSON deserialization; `InterfaceExecution` invokes `MMMQListener<T>.handle()`.
- **Producer Builder:** `Producer.builder(host).maxRetryCount(n).build()` for custom retry count.
- **DLQ:** Multiple `DeadLetterQueue` beans can coexist; all receive failed messages. `DeadLetterHandler` is pluggable.

## Consumer Handler Registration

```java
// Annotation-based (method level)
@MMMQListener("order.*")
public void handle(Order order) { ... }

// Interface-based (class level)
@Service
public class OrderService implements MMMQListener<Order> {
    public Pattern listens() { return new Pattern("order.*"); }
    public void handle(Order order) { ... }
}
```

## Broker Dispatcher Registration

```java
// Each Dispatcher binds to one or more Patterns.
@Bean
public Dispatcher orderDispatcher() {
    return new Dispatcher(
        "order-dispatcher",
        new Host("http", "ip", 8080),
        List.of(new Pattern("order.*"))
    );
}
```

## Code Style Guide

### Formatting & Layout
- Indentation: 4 spaces. Line length: max 120 characters.
- If a method signature or call exceeds 120 chars, place EVERY parameter on a new line.
- ALWAYS use curly braces `{}` for control structures, even single-line statements.
- Class/Interface: insert a blank line immediately after the opening brace `{`.
- Record: place each component on a new line. Insert a blank line after `{` if the record has methods.
- Annotation stacking: pyramid order (shortest → longest, top → bottom).
- ALWAYS insert a blank line before a `return` statement.
- Do NOT use `this` unless needed to resolve naming conflicts.

### Lombok
- Allowed: `@Getter`, `@RequiredArgsConstructor`, `@NoArgsConstructor(access = AccessLevel.PROTECTED)`, `@Slf4j`.
- Forbidden: NEVER use `@Setter` or `@Data`.

### Immutability
- Use `private final` fields with `@RequiredArgsConstructor` for DI.
- Use `final` for local variables when reassignment is not needed.
- Return immutable collections (`Stream.toList()`, `List.copyOf()`).

### Optional & Streams
- Use `orElseThrow()` / `ifPresent()`. NEVER use `isPresent()` + `get()`.
- Prefer Stream API over traditional for/while loops.

### Clean Code
- Comments explain WHY, not WHAT. Code must be self-documenting.
- Apply SRP for method extraction, but don't over-fragment (avoid 3~5 line fragments).
- Objects autonomously manage their own state. Avoid direct field access from outside.

## Naming Convention

### Packages
- Format: lowercase. Structure: `org.mmmq.{module}.{subpackage}`
- `{module}`: `core`, `producer`, `consumer`, `broker`
- Example: `org.mmmq.broker.dispatcher`, `org.mmmq.core.message`

### Classes and Interfaces
- Format: PascalCase, nouns or noun phrases.
- Forbidden suffixes: `Client`, `Manager`, `Helper`, `Util` — avoid unless unavoidable.
- `Handler` is permitted in messaging/event contexts (e.g., `DeadLetterHandler`, `FrontHandler`).

### Methods
- Format: camelCase, start with verbs.
- Use domain verbs reflecting the messaging context: `produce`, `dispatch`, `send`, `receive`, `handle`, `register`.
- Avoid generic CRUD verbs (create/read/update/delete) — MMMQ is not a REST CRUD app.

### Variables and Constants
- camelCase for variables. Names must clearly state purpose, regardless of length.
- Lambda variables: NEVER use single-letter names. Use descriptive names.
- Constants: `UPPER_SNAKE_CASE` for `static final` fields.

### API Endpoints
- MMMQ exposes `POST /messages` as its primary endpoint.
- If new endpoints are added: lowercase, kebab-case for multi-word paths.
