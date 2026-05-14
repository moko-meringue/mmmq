---
name: Do not apply final to local variables
description: final keyword for immutability applies only to fields, not local variables — adding final to locals is a rule misapplication
type: feedback
---

`final` 키워드를 지역 변수에 붙이지 않는다.

**Why:** `code-style.md`의 Immutability 규칙은 "Prefer `final` for fields"이며, 필드에만 적용된다. 지역 변수는 이 규칙의 대상이 아니다. WAL 리팩터링에서 `payload`, `buffer`, `file`, `walDir`, `topicQueue`, `entries`, `matcher` 등 지역 변수에 `final`을 붙인 것은 규칙을 확대 해석한 실수였다.

**How to apply:** 필드 선언(`private final`, 생성자 주입)에만 `final`을 적용한다. 메서드 본문 안의 지역 변수에는 `final`을 붙이지 않는다.
