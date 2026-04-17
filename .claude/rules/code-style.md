# MMMQ Code Style & Implementation Rules

All rules in this document are non-negotiable. Apply them in full.

---

## Formatting & Layout

- Indentation: 4 spaces. Max line length: 120 characters.
- If a method signature or call exceeds 120 chars, place EVERY parameter on a new line.
- ALWAYS use curly braces `{}` for all control structures, even single-line bodies.
- Class/Interface: insert a blank line immediately after the opening brace `{`. Do NOT insert a blank line before the closing brace `}`.
- Record: place each component on a new line. Insert a blank line after `{` if the record has methods.
- Annotation stacking: pyramid order (shortest → longest, top → bottom). Length is measured as the full annotation text including any internal content (e.g., `@Service` vs `@RequiredArgsConstructor` vs `@EventListener(condition = "...")`), not just the annotation name.
- ALWAYS insert a blank line before a `return` statement.
- Do NOT use `this` unless resolving a naming conflict.
- Stream chaining: place each intermediate/terminal operation on its own line, indented 8 spaces from the base expression.
- Multi-line parameter lists: the closing `)` or `) {` must be on its own line at the base indentation level.
- One blank line between methods, regardless of visibility (public/private/package-private).
- Field declaration order: `static` fields first, then instance fields, then constructors. (All fields are `private`, so no further ordering by visibility is needed.)
- Import grouping: `java.*` → third-party (`com.*`, `org.spring*`, etc.) → project (`org.mmmq.*`). No blank lines between groups.

---

## Access Modifiers & Encapsulation

Encapsulation is a first-class concern. Apply the most restrictive access modifier possible at every declaration site. Never use Lombok — all constructors and getters must be written explicitly.

- **Classes**: Prefer package-private. Use `public` only when the class must be accessible outside the package. NEVER use `final` on classes.
- **Nested classes**: If a nested class is not used outside its enclosing class, it MUST be `private`.
- **Methods & Constructors**: Preference order (most preferred → least): `private` → package-private → `protected` → `public`.
- **Fields**: ALL fields must be `private`. No exceptions — this includes `static final` constants. If a field can be `null`, it MUST be annotated with `@Nullable`.
- **Getters**: Write explicit getter methods. Their access modifier must follow the same preference order — do not default to `public` unless the field genuinely needs to be exposed outside the package.
- **`protected`**: Permitted only when a subclass strictly requires access. Do not use `protected` as a convenience shortcut.
- When in doubt, restrict further. Widening access is always easier than narrowing it later.
- NEVER widen access modifiers for the sake of testing. If internal state must be verified in tests, add an explicit package-private method that exposes only what is needed (e.g., `int subscriptionCount()`).

---

## Immutability

- Prefer `final` for fields. Omit `final` only when the field must change after construction (e.g., mutable state).
- Write an explicit constructor for dependency injection.

---

## Nullable

- A method may return `null`, but if it can, it MUST be annotated with `@Nullable`. Unannotated methods are assumed to never return `null`.

---

## Streams

- Prefer Stream API over traditional for/while loops.
- Lambda parameter names must be descriptive — NEVER single-letter names.
- Use `Stream.toList()` for terminal collection. NEVER use `Collectors.toList()`.
- Avoid `Stream.forEach()` — it encourages side effects. Use an enhanced for-each loop instead when side effects are necessary.

---

## Clean Code

- In `main` source: remove ALL comments except `// TODO`. Code must be self-documenting.
- Extract methods only when the extracted method can carry a meaningful name that conveys intent. Avoid forced extraction that produces names like `doProcess()` or `handleInternal()`.
- **Tell, Don't Ask**: Request behavior from objects — do not extract state, make decisions outside, then push the result back. Move the decision into the object.
- **Guard clauses**: Use early returns to eliminate nesting. The happy path should be the last statement, not wrapped in an `if` block.
- **Avoid primitive obsession**: When a raw primitive is used to represent a domain concept, consider wrapping it in a value object. Wrap it when communicating with the value object feels more natural — i.e., when the value object can carry behavior or meaning that the primitive cannot express on its own (e.g., `Topic`, `Offset`). Do not wrap blindly.
- **Law of Demeter**: Do not chain calls across object boundaries (e.g., `a.getB().getC()`). Talk only to immediate collaborators.
- **No boolean parameters**: A `boolean` parameter is a signal that a method does two things. Split into two methods or replace with an `enum`.
- **Depend on abstractions**: Depend on interfaces or abstract types, not concrete implementations, to keep components replaceable.
- **No magic numbers or strings**: Inline literals (e.g., `60000`, `3`, `"order.*"`) are forbidden. Declare them as named `static final` constants. Exceptions: `0`, `1`, `-1`, and empty string `""` when their meaning is unambiguous in context.
- **Remove dead code**: Delete unused methods, fields, and imports immediately. Do not comment out code — if it is not needed, it must be removed.

---

## Exception Handling

- Wrap checked exceptions in unchecked exceptions (`RuntimeException` or a domain-specific subclass). Do not propagate checked exceptions up the call stack.
- NEVER silently swallow exceptions. Every `catch` block must either rethrow or perform explicit recovery. An empty `catch` block or log-only handling is forbidden.
- Create a custom exception only when the situation carries domain meaning that standard exceptions cannot express. Otherwise use `IllegalArgumentException`, `IllegalStateException`, etc.
- Always handle `InterruptedException` by calling `Thread.currentThread().interrupt()` to restore the interrupt status before rethrowing or returning.
- Exception messages must include context: what value was involved and what went wrong (e.g., `"Failed to write WAL entry for segment: " + segmentIndex`). Vague messages like `"failed"` or `"error"` are forbidden.

---

## Naming Conventions

### Packages
- Lowercase. Format: `org.mmmq.{module}.{subpackage}`
- Modules: `core`, `producer`, `consumer`, `broker`

### Classes and Interfaces
- PascalCase, nouns or noun phrases.
- Forbidden suffixes: `Client`, `Manager`, `Helper`, `Util` (unless unavoidable).
- `Handler` is permitted in messaging/event contexts.

### Methods
- camelCase, start with verbs.
- Use domain verbs: `produce`, `dispatch`, `send`, `receive`, `handle`, `register`.
- Getters must follow standard Java getter naming: `getFieldName()` for regular fields, `isFieldName()` for `boolean` fields.

### Variables & Constants
- camelCase for variables. Names must clearly express purpose.
- Constants: `UPPER_SNAKE_CASE` for `static final` fields.
- Logger: MUST be declared as `private static final Logger log = LoggerFactory.getLogger(ClassName.class);`. The variable name is always `log`.

---

## MMMQ Architectural Constraints

- **Module boundaries**: `producer → core`, `consumer → core`, `broker → core`. Cross-module dependencies in the wrong direction are forbidden.
- **Package structure**: `org.mmmq.{module}.{subpackage}`. New classes must fit into an existing or clearly justified subpackage.
- **Thread model**: `Dispatcher` subscriptions use a single worker thread each. `FrontHandler` uses a `ThreadPoolExecutor(2–5)`. Do not introduce shared mutable state without explicit synchronization.
- **Message flow**: Producer → Broker → FrontDispatcher → TopicQueue → Dispatcher → Sender → Consumer → FrontHandler → HandlerExecution. New features must fit into or clearly extend this flow.
