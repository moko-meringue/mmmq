---
name: pr-convention
description: PR 작성, Pull Request 생성, 또는 작업 요약을 요청할 때 이 스킬을 사용하세요.
---

# Pull Request (PR) Skill Rules

## 1. Title Generation
- **Prefix Tag**: MUST start with one of the following tags based on the change type:
  - `[feat]`: New features.
  - `[refactor]`: Code improvements without behavior changes.
  - `[fix]`: Bug or error fixes.
  - `[setting]`: CI/CD, environment, or infra changes.
  - `[hotfix]`: Urgent production fixes.
- **Language**: The description following the tag MUST be in **Korean**.

## 2. Body Generation (Language & Tone)
- **Language**: ALWAYS write the PR body in **Korean**.
- **Tone**: ALWAYS use **polite honorifics (존댓말)**. (e.g., "~했습니다", "~수정 작업을 진행했습니다").

## 3. Mandatory Template & Content Strategy
Strictly use the following structure for the PR body:

```markdown
## Issue Number
## 작업 내용
```

- **Autonomous Content Composition (CRITICAL)**: The internal structure of the '작업 내용' section MUST NOT be restricted to a fixed format. Autonomously determine the most effective structure (e.g., sub-bullet points, categorization, or 'Before/After' comparisons) based on the PR's scale, complexity, and nature of changes.
- **Readability**: Strategically use Markdown elements (headers, lists, etc.) to ensure the reviewer can easily grasp the changes at a glance.

## 4. Execution Logic
1. **Analysis**: Analyze the changes by reviewing `git diff` or commit history.
2. **Tag Selection**: Select the most appropriate tag based on the logic of the changes.
3. **Autonomous Composition**: Design and write the '작업 내용' section using the most logical and readable structure.
4. **Final Review**: Ensure the entire body is in polite Korean before outputting the result.
