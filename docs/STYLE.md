# 문서 표기 규칙

> mmmq docs 사이트(producer/broker/consumer/quickstart 등) 공통 표기 컨벤션.

## 핵심 원칙

같은 대상이 **코드 식별자**인지 **일반 개념어**인지에 따라 표기를 결정한다.
한 문장 안에서는 한 가지 표기로 통일한다.

## 표기 표

| 카테고리 | 표기 | 예시 |
|---|---|---|
| 코드 식별자 (클래스 · 매개변수 · 필드 · 어노테이션) | 영문 + `<code>` | `Producer`, `maxRetryCount`, `@Bean` |
| 메서드 (괄호 + 매개변수까지) | 영문 + `<code>` | `produce(Message)`, `getCause()` |
| 코드 상수 · enum 값 | 영문 + `<code>` (항상 적용) | `ACK`, `NACK`, `HTTP` |
| 엔드포인트 · 경로 · 파일명 | 영문 + `<code>` | `/mmmq/messages`, `POST /mmmq/messages`, `build.gradle` |
| 라이브러리 · 외부 API 클래스 | 영문 + `<code>` (항상 적용) | `RestClient` |
| 개념어 (일반 명사) | 한글 | 메시지, 토픽, 응답, 모듈, 발행, 요청, 본문 |
| 외래어로 정착된 일반 명사 | 영문 그대로 (`<code>` 없이) | broker, Spring, Jackson, JSON |

### 메서드 표기 규칙

- 괄호와 매개변수 타입까지 포함한다: `<code>produce(Message)</code>`
- 빈 괄호 단독(`produce()`)은 사용하지 않는다 — 의미가 모호함.
- 매개변수가 없는 메서드는 `<code>build()</code>`처럼 빈 괄호 그대로.
- 여러 오버로드를 동시에 가리킬 때만 매개변수를 생략하고 본문에서 풀어 쓴다.

## 헤딩 (h2 / h3)

- 헤딩 안에서는 코드 식별자라도 `<code>` 처리하지 않는다 (시각 위계 우선).
- 예: `<h2>Producer 생성</h2>`, `<h3>재시도 메커니즘</h3>`

## 동음이의어 처리

같은 단어가 코드와 개념 둘 다로 쓰일 수 있을 때:

| 의미 | 표기 | 예 |
|---|---|---|
| 클래스 · 객체 · 인스턴스 | `<code>Message</code>` | "<code>Message</code>를 요청 바디에 담아…" |
| 일반 데이터 · 추상 개념 | "메시지" | "broker로 메시지를 발행합니다" |

자주 등장하는 짝:
- `Message` ↔ 메시지
- `Topic` ↔ 토픽
- `Producer` ↔ producer 모듈/객체 (소문자 일반 명칭은 평문 "producer")
- `Consumer` ↔ consumer
- `Broker` ↔ broker

## 외래어 처리 보충

- **broker** — 항상 영문, `<code>` 없이. 한글 "브로커"는 사용하지 않음.
- **Spring**, **Jackson** — 영문 그대로, `<code>` 없이.
- **RestClient** — 클래스이므로 항상 `<code>RestClient</code>`.
- **HTTP / HTTPS** — 프로토콜 일반 명사로 쓸 때 평문. enum 값(`WebProtocol.HTTP` 등)으로 쓸 때는 `<code>HTTP</code>`.
- **JSON** — 평문.
- **POJO**, **record** — 일반 용어로 쓰면 평문. 키워드 시연이면 `<code>record</code>`.
