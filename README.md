<!-- @formatter:off -->
# MMMQ (Moko-Meringue's Message Queue)

# 🎯 프로젝트 목적

**MMMQ**는 메시지 큐 시스템의 핵심 개념과 동작 원리를 학습하기 위한 프로젝트입니다.

- 단순히 도구를 사용하는 것을 넘어, 메시지 브로커의 철학과 동작 메커니즘을 깊이 있게 탐구합니다.
- 큐 자료구조부터 메시지 영속화, 동시성 제어까지 바닥부터 직접 쌓아 올립니다.
- 높은 처리량을 달성하기 위해 고민합니다.

# 🏗️ 아키텍처

## 전체 메시지 흐름

```
Producer.produce(message)
  → HTTP POST /messages → Broker
    → FrontDispatcher.dispatch(message)
      → 매칭 Dispatcher 존재 여부 확인
      → TopicQueueRegistry.get(topic)  [없으면 생성 + 매칭 Dispatcher 구독 등록]
      → TopicQueue.add(message)
          → SegmentChain에 메시지 저장
          → MessageArrivedEvent 발행
        → Dispatcher.onMessageArrived(event) [EventListener]
            → Subscription의 Executor에 drain 태스크 제출
              → drain(): TopicQueue.poll(offset)으로 루프 소비
                → Sender.send(message, maxNackRetry=3)
                  → HTTP POST /messages → Consumer
                    → FrontHandler: BlockingQueue(1000) + ThreadPoolExecutor(2~5)
                      → HandlerExecution(@MMMQListener 또는 MMMQListener<T>)
                → NACK 소진 시: 모든 DeadLetterQueue에 전달
                → 통신 실패 시: 지수 백오프(1s→60s) 무한 재시도
```

---

# 📦 모듈 구조

```
core/       # 공유 타입: Message, Topic, Pattern, Acknowledgement
producer/   # Producer 빈 + Gateway (RestClient → POST /messages → Broker)
consumer/   # Consumer REST 엔드포인트 + FrontHandler + HandlerExecution
broker/     # Broker REST 엔드포인트 + FrontDispatcher + Dispatcher + DeadLetterQueue
```

**모듈 의존 관계:** `producer → core`, `consumer → core`, `broker → core`

---

# 🔧 모듈별 상세 설명

## core

공통으로 사용하는 도메인 타입을 제공합니다.

| 클래스 | 설명 |
|--------|------|
| `Message` | 토픽과 콘텐츠(`Map<String, Object>`)를 담는 메시지 레코드 |
| `Topic` | 메시지 토픽 식별자 레코드 |
| `Pattern` | Ant 스타일 와일드카드 패턴 매칭. `matches(Topic)`으로 토픽 매칭 여부 확인 |
| `Acknowledgement` | ACK / NACK 응답 열거형 |
| `BrokerAcknowledgement` | Broker → Producer 응답 레코드 |
| `ConsumerAcknowledgement` | Consumer → Broker 응답 레코드 |
| `Host` | `WebProtocol + InetAddress + port` 조합의 네트워크 엔드포인트 |

**Pattern 매칭 예시**

| 패턴 | 매칭 토픽 예시 |
|------|---------------|
| `order.*` | `order.new`, `order.cancel` |
| `**` | 모든 토픽 |
| `payment.kakao.*` | `payment.kakao.success` |

---

## producer

```
Producer.produce(message)
  → Gateway.send(message) [HTTP POST /messages]
  → BrokerAcknowledgement 수신
  → NACK 시 maxRetryCount까지 재시도 (기본값: 3)
```

| 클래스 | 설명 |
|--------|------|
| `Producer` | 메시지 발행 클라이언트. `produce(Message)`로 브로커에 메시지 전송 |
| `Gateway` | `RestClient`를 통해 브로커로 HTTP POST 요청 전송 |

**Builder 패턴으로 재시도 횟수 커스터마이징:**

```java
Producer producer = Producer.builder(brokerHost)
    .maxRetryCount(5)
    .build();
```

---

## broker

브로커의 핵심 역할을 수행합니다. 메시지를 수신하고 적절한 Consumer에게 전달하며, 실패 메시지는 Dead Letter Queue로 처리합니다.

### REST 엔드포인트

`Broker`는 `POST /messages`를 제공하여 Producer로부터 메시지를 수신합니다. 수신한 메시지를 `FrontDispatcher`에 위임하고 `BrokerAcknowledgement`를 반환합니다.

---

### FrontDispatcher

```
FrontDispatcher.dispatch(message)
  1. dispatchers 중 topic에 매칭되는 것이 없으면 무시
  2. TopicQueueRegistry.get(topic)으로 해당 토픽의 큐 조회/생성
  3. TopicQueue.add(message) 호출
```

매칭 여부만 확인하고, 실제 구독 등록은 `TopicQueueRegistry`에서 처리됩니다.

---

### TopicQueue & SegmentChain

토픽 단위의 메시지 큐입니다. 내부적으로 `SegmentChain`을 통해 메시지를 세그먼트 연결 리스트 구조로 저장하며, 오프셋 기반으로 메시지를 조회합니다.

```
TopicQueue.add(message)
  → SegmentChain.add(message)   // 세그먼트에 메시지 저장
  → MessageArrivedEvent 발행    // Dispatcher에 소비 트리거

TopicQueue.poll(offset)          // 논블로킹 메시지 조회 + offset 증가
TopicQueue.getNewOffset()        // 새로운 구독자용 Offset 생성
```

`SegmentChain`은 고정 용량(기본 1000개)의 `Segment` 배열을 연결 리스트처럼 관리하며, 모든 소비자가 지나간 세그먼트는 자동으로 메모리에서 회수합니다.

| 클래스 | 설명 |
|--------|------|
| `TopicQueue` | 토픽 단위 큐. 이벤트 발행과 오프셋 기반 폴링 API 제공 |
| `TopicQueueRegistry` | `ConcurrentHashMap<Topic, TopicQueue>` 레지스트리. 신규 토픽 감지 시 Queue 생성 및 Dispatcher 구독 등록 |
| `SegmentChain` | 세그먼트 연결 리스트 구조의 저장소. `ReentrantLock`으로 스레드 안전성 보장 |
| `Segment` | 고정 배열(`Message[]`)로 구성된 저장 단위 |
| `Offset` | 메시지의 절대 위치를 나타내는 커서. `increment()`로 위치 전진 |

---

### Dispatcher

Consumer에게 메시지를 실제로 전달하는 핵심 컴포넌트입니다. 하나의 `Dispatcher`는 하나 이상의 `Pattern`을 가지며, 매칭되는 모든 `TopicQueue`를 구독합니다.

#### 구독 등록

```java
// TopicQueueRegistry가 신규 토픽 감지 시 호출
dispatcher.subscribe(topicQueue);
// → 패턴 매칭 확인
// → Subscription(offset, executor) 생성 후 subscriptions 맵에 저장
```

#### 이벤트 기반 소비 (drain-loop 패턴)

```
MessageArrivedEvent 수신
  → subscriptions에서 해당 TopicQueue의 Subscription 조회
  → executor.submit(drain 태스크)
      → 이미 실행 중이면 ArrayBlockingQueue(1)에 대기
      → 큐도 가득 차면 DiscardPolicy로 무시 (drain 루프가 미소비 메시지 모두 처리)

drain(topicQueue, subscription):
  while (topicQueue.poll(offset) != null) {
      send(message)
  }
```

**drain-loop의 안전성:**
제출이 무시되더라도 메시지는 `SegmentChain`에 보존되어 있습니다. 실행 중인 drain 태스크가 루프를 돌며 모든 미소비 메시지를 처리하므로 유실이 없습니다.

#### Subscription (Dispatcher 내부)

토픽별 구독 상태를 관리하는 내부 레코드입니다.

```java
record Subscription(Offset offset, ExecutorService worker)
```

- **Executor 설정:** `ThreadPoolExecutor(coreSize=0, maxSize=1, keepAlive=60s, queue=ArrayBlockingQueue(1), DiscardPolicy)`
  - `coreSize=0`: 유휴 상태 60초 후 스레드 자동 종료 → 트래픽 없을 때 리소스 절약
  - `maxSize=1`: 토픽별 메시지 순서 보장
  - `ArrayBlockingQueue(1)` + `DiscardPolicy`: 중복 신호 무시

#### 재시도 전략 (2계층)

| 계층 | 조건 | 전략 |
|------|------|------|
| NACK 재시도 | Consumer가 NACK 응답 | 최대 3회 재전송. 소진 시 모든 `DeadLetterQueue`에 전달 |
| 통신 실패 재시도 | 네트워크 오류 등 | 지수 백오프 (초기 1초, 최대 60초, 배수 2) 무한 재시도 |

---

### DeadLetterQueue

NACK 재시도를 모두 소진한 메시지를 `DeadLetter`로 포장하여 저장하고 핸들링합니다. 여러 개의 `DeadLetterQueue` 빈을 등록하면 모두 동시에 메시지를 수신합니다.

| 클래스 | 설명 |
|--------|------|
| `CounterDeadLetterQueue` | DeadLetter가 지정 개수에 도달하면 핸들러 실행 |
| `TimerDeadLetterQueue` | 지정 주기마다 핸들러 실행 |
| `DeadLetterFileWriter` | DeadLetter를 JSON으로 직렬화하여 파일 저장 |
| `DeadLetterHandler` | 커스텀 핸들러 구현을 위한 인터페이스 |

---

## consumer

```
Consumer (POST /messages)
  → FrontHandler.handle(message)       // 내부 BlockingQueue(1000)에 추가
    → Worker 스레드: 큐에서 꺼내
      → HandlerExecutions.getExecutions(message) [토픽 기반 캐싱]
        → ThreadPoolExecutor(2~5)에 제출
          → HandlerExecution.execute(message)
```

### 핸들러 등록 방식

#### 어노테이션 방식 (`@MMMQListener`)

```java
@Service
public class OrderService {

    @MMMQListener("order.*")        // pattern 생략 시 "**" (모든 토픽)
    public void handle(Order order) {
        // ...
    }
}
```

내부적으로 `MethodExecution`이 리플렉션으로 메서드를 호출합니다. 파라미터 타입으로 메시지 콘텐츠를 JSON 역직렬화합니다.

#### 인터페이스 방식 (`MMMQListener<T>`)

```java
@Service
public class OrderService implements MMMQListener<Order> {

    @Override
    public Pattern listens() {
        return new Pattern("order.*");
    }

    @Override
    public void handle(Order order) {
        // ...
    }
}
```

`InterfaceExecution`이 제네릭 타입 파라미터를 런타임에 리졸빙하여 타입 안전하게 핸들러를 호출합니다.

| 클래스 | 설명 |
|--------|------|
| `FrontHandler` | 수신 메시지를 내부 큐에 쌓고 ThreadPoolExecutor로 처리 |
| `HandlerExecutions` | 핸들러 레지스트리 + 토픽별 캐시 |
| `MethodExecution` | `@MMMQListener` 어노테이션 메서드 실행 |
| `InterfaceExecution` | `MMMQListener<T>` 인터페이스 구현체 실행 |

---

# 🚀 시작하기

## 최소 버전 요구사항

- Java 17 이상
- Spring Boot 3.2.0 이상
- Spring Web (`spring-boot-starter-web` 의존성 포함)

## 의존성 추가 (build.gradle)

```groovy
repositories {
    maven { url "https://jitpack.io" }
    mavenCentral()
}

dependencies {
    // Broker 모듈
    implementation 'com.github.moko-meringue.mmmq:broker:{버전}'

    // Consumer 모듈
    implementation 'com.github.moko-meringue.mmmq:consumer:{버전}'

    // Producer 모듈
    implementation 'com.github.moko-meringue.mmmq:producer:{버전}'
}
```

## 빈 등록

### Producer 설정

```java
@Configuration
public class ProducerConfig {

    @Bean
    public Producer producer() {
        Host brokerHost = new Host("http", "ip", 8080);
        return new Producer(brokerHost);

        // 재시도 횟수 커스터마이징 (기본값: 3)
        // return Producer.builder(brokerHost)
        //         .maxRetryCount(5)
        //         .build();
    }
}
```

### Consumer 설정

어노테이션 방식과 인터페이스 방식을 동시에 사용할 수 있습니다.

```java
@Service
public class OrderService {

    @MMMQListener("order.*")
    public void handleOrder(Order order) {
        // order.* 패턴의 메시지 처리
    }
}
```

```java
@Service
public class PaymentService implements MMMQListener<Payment> {

    @Override
    public Pattern listens() {
        return new Pattern("payment.*");
    }

    @Override
    public void handle(Payment payment) {
        // payment.* 패턴의 메시지 처리
    }
}
```

### Broker 설정

#### Dispatcher 등록

`Dispatcher`는 하나 이상의 `Pattern`을 바인딩할 수 있습니다.

```java
@Configuration
public class DispatcherConfig {

    @Bean
    public Dispatcher orderDispatcher() {
        return new Dispatcher(
                "order-dispatcher",
                new Host("http", "ip", 8080),       // Consumer 호스트
                List.of(new Pattern("order.*"))      // 바인딩할 패턴 목록
        );
    }

    @Bean
    public Dispatcher paymentDispatcher() {
        return new Dispatcher(
                "payment-dispatcher",
                new Host("http", "ip", 8081),
                List.of(new Pattern("payment.*"))
        );
    }

    // DLQ를 사용하는 경우 생성자에 전달
    @Bean
    public Dispatcher orderDispatcherWithDlq(List<DeadLetterQueue> deadLetterQueues) {
        return new Dispatcher(
                "order-dispatcher",
                new Host("http", "ip", 8080),
                List.of(new Pattern("order.*")),
                deadLetterQueues
        );
    }
}
```

#### DeadLetterQueue 등록 (선택)

여러 개를 등록하면 전송 실패 메시지가 모든 큐에 전달됩니다.

```java
@Configuration
public class DeadLetterConfig {

    @Bean
    public DeadLetterHandler deadLetterFileWriter() {
        return new DeadLetterFileWriter(
                Path.of("/home/ubuntu/broker/dead-letters"),
                "dead-letter-writer"
        );
    }

    @Bean
    public DeadLetterQueue counterDlq(DeadLetterHandler handler) {
        return new CounterDeadLetterQueue(
                "counter-dlq",
                handler,
                50           // 50개 누적 시 핸들링
        );
    }

    @Bean
    public DeadLetterQueue timerDlq(DeadLetterHandler handler) {
        return new TimerDeadLetterQueue(
                "timer-dlq",
                handler,
                10_000       // 10초 주기로 핸들링
        );
    }
}
```

---

# 👥 참여자

모든 코드는 모코와 머랭의 **페어 프로그래밍**으로 작성되었습니다.

<div align="center">
<table>
  <tr height="140px">
    <td align="center">
      <a href="https://github.com/cookie-meringue">
        <img src="https://avatars.githubusercontent.com/u/113977176?v=4" alt="머랭" width="100" />
      </a>
      <br />
      <a href="https://github.com/cookie-meringue">머랭</a>
    </td>
      <td align="center">
      <a href="https://github.com/songsunkook">
        <img src="https://avatars.githubusercontent.com/u/21010656?v=4" alt="모코" width="100" />
      </a>
      <br />
      <a href="https://github.com/songsunkook">모코</a>
    </td>
  </tr>
</table>
</div>

# ⚖️ 라이선스

이 프로젝트는 교육 목적으로 제작되었습니다.
