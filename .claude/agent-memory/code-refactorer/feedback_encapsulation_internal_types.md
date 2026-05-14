---
name: Internal serialization types must not leak across package boundaries
description: Value objects used purely for internal serialization should stay package-private; push construction inside the owning class
type: feedback
---

When a type (e.g., `WalEntry`) exists solely as a serialization wrapper for an internal component (e.g., `WalWriter`), it must not be constructed by callers in other packages. Instead, move the construction inside the owning class and change the public API to accept the domain type (e.g., `Message`).

**Why:** Exposing `WalEntry` forced `SegmentChain` (a different package) to know about WAL internals, violating encapsulation and the Law of Demeter.

**How to apply:** When refactoring I/O or serialization classes, check whether callers are constructing internal wrapper types. If so, internalize the construction and update the public method signature to accept the domain type directly.
