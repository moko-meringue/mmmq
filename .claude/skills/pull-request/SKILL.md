<!-- @formatter:off -->

---
name: pr-convention
description: PR 작성, Pull Request 생성, 또는 작업 요약을 요청할 때 이 스킬을 사용하세요.
---

# Pull Request (PR) Skill Rules

## 1. Title Generation
- Language: The description following the tag MUST be in Korean.

## 2. Body Generation (Language & Tone)
- Language: ALWAYS write the PR body in Korean.
- Tone: ALWAYS use polite honorifics (존댓말). (e.g., "~했습니다", "~수정 작업을 진행했습니다").
- The body must be written in an as-is, to-be format. The internal structure is not fixed and should be autonomously determined.
- Readability: Strategically use Markdown elements (headers, lists, etc.) to ensure the reviewer can easily grasp the changes at a glance.

## 3. Execution Logic
1. Analysis: Analyze the changes by reviewing `git diff` or commit history.
2. Title Generation: Select the most appropriate title based on the logic of the changes and generate the title.
3. Body Generation: Design and write the body using the most logical and readable structure.
4. Final Review: Ensure the entire body is in polite Korean before outputting the result.
