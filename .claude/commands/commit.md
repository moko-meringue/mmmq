---
name: commit-convention
description: 커밋 작성, 커밋 메시지 작성, 스테이징, 커밋 실행을 요청할 때 이 스킬을 사용하세요.
---

# Commit Skill Rules

## 1. Pre-Commit Checklist
- ALWAYS run `./gradlew test` before committing and verify all tests pass.
- Stage only files relevant to the task. NEVER use `git add -A` or `git add .` blindly.

## 2. Message Format
```
<type>: <subject>

<body>

Co-authored-by: <teammate> <email>
```
- Each line MUST NOT exceed 100 characters.
- Header and body MUST be separated by a blank line.
- Body is optional, but MUST be written for complex changes.
- **Co-authored-by** MUST always be appended, separated from body by a blank line.
  - Run `gh auth status` to identify the current user, then apply the rule below:
  - Current user is `songsunkook` → append `Co-authored-by: cookie-meringue <daehyeon3351@gmail.com>`
  - Current user is `cookie-meringue` → append `Co-authored-by: songsunkook <songsunkook@gmail.com>`

## 3. Type
Use exactly one of the following types:

| Type | 설명 |
|---|---|
| `feat` | 기능 추가 |
| `fix` | 버그 수정 |
| `docs` | 문서 |
| `style` | 형식, 세미콜론 누락 등 |
| `refactor` | 코드 리팩토링 |
| `test` | 누락된 테스트 추가 |
| `chore` | 유지보수 |

## 4. Subject
- Write in Korean, in command form (명령문).
- Example: `feat: 로그인 기능 추가`, `fix: 회원가입 시 이메일 중복 오류 수정`

## 5. Body (Optional)
- Explain WHY the change was necessary and HOW it was solved.
- Write in Korean, in command form (명령문), ending each sentence with a period.
- Use bullet points (`-`) for multiple changes.

## 6. Example
```
refactor: Message content 타입을 Object로 변경

- RestClient 역직렬화 시 타입 손실 문제를 해결하기 위해 content를 Object로 변경.
- Consumer 측에서 ObjectMapper로 원하는 타입으로 변환하도록 책임 이동.

Co-authored-by: cookie-meringue <daehyeon3351@gmail.com>
```
