---
name: refactor-code
description: 리팩터링, 코드 수정, 코드 리팩터링을 요청할 때 이 스킬을 사용하세요.
---

# Refactor Skill Rules

## 1. Preliminary Analysis (Identify Before Edit)
- **Full Scan**: Read ALL files within the target module and related dependencies.
- **Problem Identification**: Before applying changes, provide a Korean checklist identifying specific violations based on the rules below.

## 2. Refactoring Standards

### 2.1 Module Dependency Integrity
- **Directional Dependency**: `broker`, `producer`, `consumer` 모듈은 `core`에 의존할 수 있다. 단, `core`는 다른 모듈을 절대 참조하면 안 된다.
- **Context Isolation**: 모듈 간 경계를 침범하는 의존성을 제거한다 (e.g., `core`에서 `broker` 패키지 클래스를 참조하는 것은 금지).

### 2.2 Core Type Autonomy
- **Tell, Don't Ask**: 핵심 타입(`Message`, `Topic`, `Pattern`)이 자신의 상태를 스스로 관리하도록 로직을 이동한다.
- **Core Type Responsibility**: 해당 타입의 필드만으로 검증 가능한 규칙은 Core 타입 내부에 위치해야 한다.
- **Module Class Responsibility**: 여러 컴포넌트 간 협력이 필요한 로직(e.g., `Dispatcher`가 `Sender`와 협력하는 재시도 로직)은 모듈 클래스에 유지한다.

### 2.3 Exception Consistency
- Core 타입은 특정 모듈의 예외를 던지면 안 된다.
- 예외 생성 시 `null`을 4-arg 생성자에 넘기지 말고, detail이 없으면 3-arg 생성자를 사용한다.

### 2.4 Method Signatures
- **Control Flow**: Early Return을 선호한다. `if-else` 체인을 제거한다. 복잡한 분기는 Strategy/State 패턴을 활용한다.
- **Parameter Responsibility**: 진입점 클래스(`Broker`, `Producer` 등)가 원시 타입을 도메인 타입으로 변환한다. 내부 컴포넌트는 도메인 타입을 받는다.

### 2.5 Clean Code & Naming
- **Single Responsibility (SRP)**: 역할이 다른 작업(수신, 처리, 전달)은 메서드로 분리한다.
- **Descriptive Naming**: 람다 내 단일 문자 파라미터(e.g., `e`, `m`, `s`)는 절대 사용하지 않는다.
- **Side Effects**: 람다는 변환/필터링에 집중한다. 저장·예외 던지기 등 부수효과는 람다 바깥으로 이동한다.
- **Remove Unused Code**: 테스트나 애플리케이션 흐름에서 호출되지 않는 코드는 제거한다. TODO 주석이 있는 코드는 유지한다.

### 2.6 OOP Design Patterns
- **Abstraction**: 반복되는 분기 조건은 캡슐화를 고려한다.
- **Value Objects**: 특정 도메인 개념을 표현하는 원시 타입(String, int)은 Value Object 래핑을 검토한다.

## 3. Prohibitions
- **Behavioral Equivalence**: 새 기능을 추가하거나 API 스펙을 변경하지 않는다.
- **Test Integrity**: 기존 테스트가 깨지면 안 된다. 동작 동일성을 보장한다.
- **Over-fragmentation**: 3~5줄짜리 메서드로 과도하게 분리해 구조적 가독성을 해치지 않는다.

## 4. Execution Flow
1. **Analyze**: 대상 모듈과 의존 모듈을 스캔한다.
2. **Diagnose**: 문제점을 한국어 체크리스트로 사용자에게 제시한다.
3. **Refactor**: `naming-convention`과 `code-style` 규칙을 엄수하며 변경한다.
4. **Verify**: 리팩터링 후 외부 동작이 동일함을 확인한다.
