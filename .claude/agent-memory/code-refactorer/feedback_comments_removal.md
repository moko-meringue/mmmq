---
name: All non-TODO comments must be removed from main source
description: Inline comments in main source explaining WHAT or HOW must be deleted; code must be self-documenting
type: feedback
---

Remove every `//` comment from `main` source files that explains what the code does or why a design decision was made. Only `// TODO` is permitted.

**Why:** The code style rule states "In `main` source: remove ALL comments except `// TODO`. Code must be self-documenting."

**How to apply:** Scan every refactored file for `//` comments and delete them. This applies to block-level explanations above methods/fields as well as inline trailing comments.
