---
name: Stream.forEach is forbidden for side effects
description: Stream.forEach must be replaced with enhanced for-each when side effects are needed
type: feedback
---

Replace `stream().forEach(...)` with an enhanced for-each loop whenever the lambda body has side effects (e.g., mutating state, calling void methods on other objects).

**Why:** The code style rule explicitly forbids `Stream.forEach()` because it encourages side effects. Use `Stream` for transformations and collection, then iterate the result with for-each.

**How to apply:** Collect the stream result with `.toList()`, then iterate with `for (Type item : list)`. This applies in all `main` source files across all modules.
