<!-- @formatter:off -->

---
name: refactor-code
description: 리팩터링, 코드 수정, 코드 리팩터링을 요청할 때 이 스킬을 사용하세요.
---

# Refactor Skill Rules

## 1. Preliminary Analysis (Identify Before Edit)
- Full Scan: Read ALL files within the target module and related dependencies.
- Problem Identification: Before applying changes, provide a Korean checklist identifying specific violations based on the rules below.

## 2. Refactoring Standards

### 2.1 Core Type Autonomy
- Tell, Don't Ask: 핵심 타입(`Message`, `Topic`, `Pattern`)이 자신의 상태를 스스로 관리하도록 로직을 이동한다.
- Core Type Responsibility: 해당 타입의 필드만으로 검증 가능한 규칙은 Core 타입 내부에 위치해야 한다.
- Module Class Responsibility: 여러 컴포넌트 간 협력이 필요한 로직(e.g., `Dispatcher`가 `Sender`와 협력하는 재시도 로직)은 모듈 클래스에 유지한다.

### 2.2 Clean Code & Naming
- Single Responsibility (SRP): Extract private methods for distinct tasks (Lookup, Processing, Saving).
- Descriptive Naming: Use full descriptive names. NEVER use single-letter parameters (e.g., `e`, `o`, `s`) in lambdas.
- Side Effects: Lambdas SHOULD focus on transformation/filtering. Move side effects (saving, throwing) outside the lambda context when possible.
- Remove Unused Code: Eliminate any code that is not invoked by any test or application flow. If "TODO" comments exist, do not remove that code.

### 2.3 OOP Design Patterns
- Abstraction: Identify repetitive branch conditions (`if (widget.isEnded())`) and consider Encapsulation.
- Value Objects (VO): Consider wrapping primitive types (String, Long) into Value Objects if they represent specific domain concepts (e.g., `LessonName`, `AttachmentSize`).
- Control Flow: Prefer Early Returns. Eliminate `if-else` chains. Use Design Patterns (Strategy/State) for complex OCP violations.

## 3. Prohibitions
- Behavioral Equivalence: DO NOT add new features.
- Test Integrity: Existing tests MUST NOT break. Ensure behavioral consistency.
- Over-fragmentation: DO NOT split methods into 3-5 line fragments if it degrades structural readability.

## 4. Execution Flow
1. Analyze: Scan the package and dependencies.
2. Diagnose: List identified issues in Korean for the user.
3. Refactor: Apply changes while strictly following the `naming-convention` and `code-style` rules.
4. Verify: Ensure the refactored code maintains the same external behavior.
