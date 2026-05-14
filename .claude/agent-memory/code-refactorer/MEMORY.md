# Memory Index

- [Stream.forEach forbidden for side effects](feedback_stream_foreach.md) — replace with enhanced for-each; applies everywhere in main source
- [Internal serialization types must not leak packages](feedback_encapsulation_internal_types.md) — push wrapper construction inside owning class, expose domain type in public API
- [Remove all non-TODO comments from main source](feedback_comments_removal.md) — code must be self-documenting; delete all inline explanatory comments
- [Do not apply final to local variables](feedback_final_local_variables.md) — Immutability rule targets fields only; adding final to locals is a misapplication
