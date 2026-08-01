<!-- @formatter:off -->
# MMMQ (Moko-Meringue's Message Queue)

# 🎯 프로젝트 목적

**MMMQ**는 메시지 큐 시스템의 핵심 개념과 동작 원리를 학습하기 위한 프로젝트입니다.

- 단순히 도구를 사용하는 것을 넘어, 메시지 브로커의 철학과 동작 메커니즘을 깊이 있게 탐구합니다.
- 큐 자료구조부터 메시지 영속화, 동시성 제어까지 바닥부터 직접 쌓아 올립니다.
- 높은 처리량을 달성하기 위해 고민합니다.

# 🏗️ 아키텍처

![전체_아키텍처.png](docs/images/%EC%A0%84%EC%B2%B4_%EC%95%84%ED%82%A4%ED%85%8D%EC%B2%98.png)

## 전체 메시지 흐름

```
Producer.produce(message)
  → HTTP POST /mmmq/messages → Broker
    → FrontDispatcher.dispatch(message)
      → TopicQueueContainer.getOrCreate(topic)  [없으면 생성 + SubscriptionContainer.register(queue)]
      → TopicQueue.offer(message)
          → SegmentFileChain에 엔트리 append + fsync
          → 인덱스 파일에 주소 append + fsync  ← 메시지 commit point
      → ACK/NACK을 Producer에게 응답
      → SubscriptionContainer.trigger(queue)
        → 그 큐의 Subscription마다 자기 워커에 drain 태스크 제출
          → drain(): TopicQueue.peek(offset)으로 루프 소비
            → Dispatcher.send(message)
              → HTTP POST /mmmq/messages [헤더 mmmq-consumer-id] → Consumer
                → HandlerExecutionContainer.find(consumerId) → HandlerExecution.execute
                → 톰캣 요청 스레드에서 동기 실행, 같은 응답으로 ACK/NACK
            → 전송 완료 시: 체크포인트에 다음 오프셋 fsync
            → NACK 소진 시: 경고 로깅 후 메시지 드랍
            → 통신 실패 시: 지수 백오프(1s→60s) 무한 재시도
```

이벤트 버스를 쓰지 않습니다. 각 단계는 직접 메서드 호출이며, `SubscriptionContainer`가 어떤 구독이 그 큐에 붙어 있는지 아는 유일한 지점입니다.

## 부팅 시 복원 흐름

```
DispatcherContainer 생성자
  → dispatchers.json 읽기 → Dispatcher 생성
  → SubscriptionContainer.rematchAll(전체 Dispatcher)   ← 매칭 대상 목록을 넘겨 둔다

TopicQueueBootstrapper [SmartInitializingSingleton]
  → {rootDir}/topics 하위 토픽 디렉토리 스캔
  → 각 토픽에 대해 TopicQueueContainer.getOrCreate(topic)
      → TopicQueueFactory.create(topic)
          → SegmentFileChain.open: 세그먼트별 SegmentFile.recover()
              → 인덱스에 commit되지 않은 partial write 영역 truncate
      → SubscriptionContainer.register(queue)
          → 패턴이 일치하는 Dispatcher마다 Subscription 생성
              → 체크포인트가 있으면 그 오프셋에서 이어서
              → 없으면 로그 tail에서 시작 (기존 백로그를 재생하지 않음)
```

---

# 📦 모듈 구조

```
core/       # 공유 타입: Message, Topic, TopicPattern, Acknowledgement, ConsumerId, Host
producer/   # Producer 빈 + Gateway (RestClient → POST /mmmq/messages → Broker)
consumer/   # Consumer REST 엔드포인트 + HandlerExecutionContainer + HandlerExecution
broker/     # Broker REST 엔드포인트 + 구독 + Dispatcher + 영속화 저장소
```

`broker` 내부는 책임에 따라 세 갈래로 나뉩니다.

```
org.mmmq.broker
├── dispatcher/     "누구에게 보내는가" — Dispatcher(consumerId·host·pattern)와 그 CRUD
│   ├── api/        HTTP 계층 (DispatcherController)
│   └── storage/    dispatchers.json 입출력
├── subscription/   "어디까지 읽었는가" — Subscription·TopicSubscriptions·SubscriptionContainer
└── topicqueue/     "무엇이 쌓였는가" — append-only 로그와 세그먼트 파일
    └── storage/
```

의존은 한 방향으로만 흐릅니다: `subscription → dispatcher`, `subscription → topicqueue`. 반대 방향이 필요한 두 지점(`topicqueue`가 새 큐를 알릴 때, `dispatcher`가 구성 변경을 알릴 때)은 각 패키지가 소유한 인터페이스(`TopicQueueRegistrar`, `DispatcherRematcher`)를 `SubscriptionContainer`가 구현하는 방식으로 뒤집습니다.

**모듈 의존 관계:** `producer → core`, `consumer → core`, `broker → core`

---

# 🔧 모듈별 상세 설명

## core

공통으로 사용하는 도메인 타입을 제공합니다.

| 클래스 | 설명 |
|--------|------|
| `Message` | 토픽과 콘텐츠(`Map<String, Object>`)를 담는 메시지 레코드 |
| `Topic` | 메시지 토픽 식별자 레코드 |
| `TopicPattern` | Ant 스타일 와일드카드 패턴 매칭. `matches(Topic)`으로 토픽 매칭 여부 확인 |
| `Acknowledgement` | ACK / NACK 응답 열거형 |
| `BrokerAcknowledgement` | Broker → Producer 응답 레코드 |
| `ConsumerAcknowledgement` | Consumer → Broker 응답 레코드 |
| `Host` | `WebProtocol + address + port` 조합의 네트워크 엔드포인트 record. `from(String url)`로 URL을 해석하고 `toUri()`로 되돌린다 |

**TopicPattern 매칭 예시**

| 패턴 | 매칭 토픽 예시 |
|------|---------------|
| `order.*` | `order.new`, `order.cancel` |
| `**` | 모든 토픽 |
| `payment.kakao.*` | `payment.kakao.success` |

---

## producer

```
Producer.produce(message)
  → Gateway.send(message) [HTTP POST /mmmq/messages]
  → BrokerAcknowledgement 수신
  → NACK 시 maxRetryCount까지 재시도 (기본값: 3)
```

| 클래스 | 설명 |
|--------|------|
| `Producer` | 메시지 발행 클라이언트. `produce(Message)`로 브로커에 메시지 전송 |
| `Gateway` | `RestClient`를 통해 브로커로 HTTP POST 요청 전송 |

**재시도 횟수 커스터마이징:**

```java
Producer producer = new Producer(brokerHost, 5);
```

---

## broker

브로커의 핵심 역할을 수행합니다. 메시지를 수신하고 적절한 Consumer에게 전달합니다.

### REST 엔드포인트

`Broker`는 `POST /mmmq/messages`를 제공하여 Producer로부터 메시지를 수신합니다. 수신한 메시지를 `FrontDispatcher`에 위임하고, **디스크 fsync 성공 여부에 따라** `BrokerAcknowledgement(ACK | NACK)`을 반환합니다.

---

### FrontDispatcher

```
FrontDispatcher.dispatch(message)
  1. TopicQueueContainer.getOrCreate(topic)으로 해당 토픽의 큐 조회/생성
  2. TopicQueue.offer(message) 호출
  3. 실패하면 즉시 NACK 반환
  4. SubscriptionContainer.trigger(queue)로 도착을 알리고 ACK 반환
```

신규 토픽이 등장하면 `getOrCreate`가 `computeIfAbsent`로 새 큐를 만들면서 `TopicQueueRegistrar.register(queue)`를 호출하고, 그 구현인 `SubscriptionContainer`가 패턴이 일치하는 `Dispatcher`마다 구독을 엽니다.

`FrontDispatcher`가 `subscription` 패키지에 있는 이유는 `SubscriptionContainer`를 참조해야 하기 때문입니다. `dispatcher`에 두면 `dispatcher → subscription` 의존이 생겨 순환이 됩니다. 옮긴 대가로 `Dispatcher`를 아예 알 필요가 없어졌습니다 — 어떤 Dispatcher가 매칭되는지는 `SubscriptionContainer`만 압니다.

---

### TopicQueue

토픽 단위의 append-only 로그입니다. **누가 어디까지 읽었는지는 모릅니다** — 그 상태는 `subscription` 패키지가 갖습니다.

```
TopicQueue.offer(message) → boolean          // 디스크 append + fsync. 실패 시 false
TopicQueue.peek(offset) → Message            // 인덱스를 이용한 random access read
TopicQueue.tailOffset() → long               // 로그의 끝. 새 구독이 시작할 위치
TopicQueue.close()                           // SegmentFileChain 해제
```

`offer`는 `ReentrantLock`으로 단일 writer를 보장합니다. 다수의 Producer가 동시에 호출해도 같은 토픽의 segment append + 인덱스 update가 atomic 단위로 직렬화됩니다. `peek`은 lock-free이며, 인덱스에 commit되지 않은 in-flight 엔트리를 보지 않습니다.

| 클래스 | 설명 |
|--------|------|
| `TopicQueue` | 토픽 단위 로그. `offer` / `peek` / `tailOffset` 제공. `Closeable` |
| `TopicQueueContainer` | `ConcurrentHashMap<Topic, TopicQueue>` 보관. 신규 큐 생성 시 `TopicQueueRegistrar.register` 호출 |
| `TopicQueueRegistrar` | 새 큐를 구독자와 이어 주는 통로. `topicqueue`가 소유하고 `SubscriptionContainer`가 구현 |
| `TopicQueueFactory` | 토픽 디렉토리 레이아웃의 유일한 소유자. `TopicQueue`를 만들고, 그 토픽의 `CheckpointDirectory`도 연다 |
| `TopicQueueBootstrapper` | 부팅 시 `{rootDir}/topics` 하위 디렉토리를 스캔하여 보존된 토픽 큐를 복원 (`SmartInitializingSingleton`) |
| `Offset` | 메시지의 절대 위치를 나타내는 불변 값 객체. `next()`로 다음 위치 생성 |

---

### 메시지 영속화 저장소

`broker.topicqueue.storage` 패키지에 디스크 저장소가 구현되어 있습니다. 모든 디스크 IO는 `FileHandle` 래퍼를 거치며, fsync 정책은 `FlushMode` enum으로 표현됩니다.

#### 엔트리 프레이밍

```
[4B length][4B CRC32C][message bytes (JSON)]
```

세그먼트 파일은 위 형식의 가변 길이 엔트리를 append-only로 누적합니다. CRC32C는 read 시점에 검증되며, 불일치 시 `ChecksumMismatchException`(`CorruptionException`의 하위)으로 격리됩니다.

#### Commit Point

메시지 한 건의 영속화는 두 단계입니다.

1. 데이터 파일에 엔트리 append + fsync.
2. 인덱스 파일(`.idx`)에 엔트리 시작 주소(8B long) append + fsync.

**인덱스 파일 fsync 완료가 메시지 commit 시점**이며, reader(`peek`)는 인덱스의 `count()`만큼만 노출되므로 부분 영속화된 엔트리를 절대 보지 않습니다.

#### 부팅 시 복원 정책 (`SegmentFile.recover`)

데이터 파일과 인덱스 파일이 불일치한 상태에서 부팅하는 경우, **인덱스 파일을 진실의 원천**으로 삼고 데이터 파일을 인덱스에 맞춰 정리합니다.

| 상태 | 처리 |
|------|------|
| 인덱스 비어있음 + 데이터 비어있지 않음 | 데이터를 0으로 truncate (commit되지 않은 partial write 폐기) |
| 인덱스 마지막 엔트리 + 엔트리 크기 < 데이터 파일 크기 | 그 너머 영역 truncate |
| 인덱스 마지막 엔트리 + 엔트리 크기 > 데이터 파일 크기 | `StorageException`으로 부팅 중단 (정상 시나리오에서는 발생 불가) |

#### 세그먼트 회전 (Soft Cap)

새 메시지 append 직전에 현재 tail 세그먼트의 파일 크기가 `mmmq.broker.persistence.segment.max-bytes` 이상이면 새 세그먼트를 열고 거기에 append합니다. 임계치는 soft cap이므로 단일 메시지가 세그먼트 중간에 절단되지 않습니다.

#### Dispatcher별 체크포인트

각 Dispatcher는 `subscribe`로 토픽에 등록될 때 `checkpoints/{consumerId}.checkpoint` 파일을 생성/로드합니다. 매 메시지 전달 완료 시 `commit`이 다음 소비 오프셋을 fsync합니다. **새로 등록되는 Dispatcher는 로그의 tail에서 시작합니다** — 운영 중에 소비자를 붙여도 쌓여 있던 메시지가 한꺼번에 재생되지 않습니다. 구독이 끝나면(삭제하거나 패턴을 좁혀 토픽이 빠지면) 그 체크포인트 파일도 함께 지워집니다.

| 클래스 | 설명 |
|--------|------|
| `FileHandle` | `FileChannel` 래퍼. 모든 디스크 IO의 단일 진입점, `FlushMode`(FSYNC/NONE)로 fsync 정책 캡슐화 |
| `SegmentFile` | 단일 세그먼트의 데이터 파일(`.mmm`) + 인덱스 파일(`.idx`) 묶음. `recover` / `append` / `readAt` 제공 |
| `SegmentFileChain` | 세그먼트들의 시간순 체인. startOffset → `SegmentFile` 매핑(`ConcurrentSkipListMap`)과 회전 정책 보유 |
| `OffsetIndexFile` | 세그먼트 내 N번째 엔트리의 데이터 파일 byte address를 8B long으로 저장하는 인덱스 |
| `CheckpointFile` | Dispatcher별 다음 소비 오프셋(8B long)을 보관하는 단일 값 파일 |
| `CheckpointDirectory` | 토픽 디렉토리 내 `checkpoints/` 하위의 체크포인트 파일들을 관리 |

#### 예외 계층

| 예외 | 의미 |
|------|------|
| `StorageException` (public) | storage layer 전반의 공통 예외. `RuntimeException` |
| `CorruptionException` (public) | 격리 가능한 손상. Dispatcher의 drain 루프가 catch하여 해당 오프셋만 skip |
| `ChecksumMismatchException` | CRC 불일치의 구체 케이스 (`CorruptionException` 하위) |
| `MessageSerializationException` | 직렬화/역직렬화 실패 (`StorageException` 하위) |

---

### Dispatcher

하나의 `consumerId` 앞으로 메시지를 보내는 **송신 단위**입니다. `(consumerId, host, pattern)` 값과 "그 대상으로 메시지 하나를 보내는 법"만 압니다.

```
Dispatcher.consumerId() → ConsumerId          // 이 Dispatcher의 식별자
Dispatcher.canDispatch(topic) → boolean       // 자기 패턴이 이 토픽과 맞는가
Dispatcher.send(message)                      // 재시도·백오프를 포함한 전송 한 건
```

**어떤 큐를 구독하는지, 어디까지 읽었는지는 모릅니다.** 그 상태는 `Subscription`이 갖습니다. 그래서 워커도 오프셋도 들고 있지 않고, 여러 큐에 걸쳐 공유해도 안전합니다.

`consumerId`는 체크포인트 파일명의 일부로 쓰이므로 `[A-Za-z0-9._-]+` 패턴으로 검증됩니다. 위반 시 `IllegalArgumentException`으로 등록 자체를 거부합니다.

| 클래스 | 설명 |
|--------|------|
| `Dispatcher` | 송신 단위. 1 consumerId = 1 Dispatcher |
| `DispatcherContainer` | 모든 Dispatcher의 소유자. `dispatchers.json` 읽기·쓰기와 런타임 CRUD. 뮤테이션은 단일 `ReentrantLock`으로 직렬화 |
| `DispatcherRematcher` | Dispatcher 구성이 바뀔 때 구독을 다시 맞추는 통로. `dispatcher`가 소유하고 `SubscriptionContainer`가 구현 |
| `DispatcherSnapshot` | 컨테이너가 외부에 내주는 읽기 모델. 컴포넌트가 전부 도메인 값 타입이라 HTTP·파일 스키마와 섞이지 않는다 |
| `Sender` | `RestClient`로 Consumer에 POST. 헤더 `mmmq-consumer-id`를 실어 보낸다 |

---

### 구독 (subscription)

"어떤 Dispatcher가 어떤 큐를 어디까지 읽었는가"를 전담합니다. 이 상태는 원래 `TopicQueue`·`Dispatcher`·`DispatcherContainer` 세 곳에 흩어져 있었고, 한 개념의 상태가 셋으로 쪼개진 탓에 일관성이 코드가 아니라 규약으로 유지됐습니다.

| 클래스 | 설명 |
|--------|------|
| `Subscription` | (TopicQueue 하나, Dispatcher 하나) 짝. 오프셋·체크포인트 파일·워커 스레드 하나를 소유 |
| `TopicSubscriptions` | 토픽 하나의 `CheckpointDirectory` + 그 큐의 `Subscription` 목록. 둘이 따로 놀지 않게 묶는다 |
| `SubscriptionContainer` | 모든 구독의 소유자. `TopicQueueRegistrar`·`DispatcherRematcher` 구현 |

#### drain 루프

```
Subscription.trigger()
  → 자기 워커에 drain 제출 (이미 실행 중이면 ArrayBlockingQueue(1)에 대기,
                            큐도 차 있으면 DiscardPolicy로 무시)

drain():
  while (true) {
      Message message = topicQueue.peek(offset);
      if (message == null) return;
      dispatcher.send(message);
      offset = offset.next();
      checkpointFile.write(offset.value());   // fsync
  }
```

**제출이 무시돼도 안전한 이유:** 메시지는 이미 디스크에 있고, 실행 중인 drain 루프가 미소비 메시지를 끝까지 훑습니다.

**손상 엔트리 격리:** `CorruptionException`을 잡아 해당 오프셋을 로깅 후 건너뛰고 다음으로 진행합니다. 손상된 한 건이 토픽 전체를 막지 않습니다.

#### 워커

구독마다 `ThreadPoolExecutor(coreSize=0, maxSize=1, keepAlive=60s, ArrayBlockingQueue(1), DiscardPolicy)` 하나입니다.

- `coreSize=0`: 유휴 60초 후 스레드 자동 종료 → 트래픽 없을 때 리소스 절약
- `maxSize=1`: 구독별 메시지 순서 보장
- `ArrayBlockingQueue(1)` + `DiscardPolicy`: 중복 신호 무시

느리거나 실패하는 Consumer 하나는 자기 구독의 워커만 막습니다.

#### 재매칭

Dispatcher가 추가·수정·삭제되면 `DispatcherContainer`가 같은 락 안에서 `rematchAll(전체 Dispatcher)`을 호출하고, 각 토픽의 구독이 네 갈래로 갈립니다.

| 경우 | 처리 |
|------|------|
| 같은 Dispatcher 인스턴스 | 손대지 않음 |
| 같은 consumerId, 다른 인스턴스 (수정) | 워커만 종료하고 새 구독. **체크포인트를 지우지 않아** 밀린 메시지를 이어받는다 |
| 더 이상 매칭되지 않음 (삭제·패턴 축소) | 워커 종료 + 체크포인트 삭제 |
| 처음 보는 consumerId | 새 구독. 체크포인트가 없으면 로그 tail에서 시작 |

삭제된 구독은 목록에서 빠지므로 이후 `trigger()`가 **도달할 수 없습니다.** 죽은 Dispatcher가 메시지를 다시 보내는 경로가 구조적으로 없습니다.

#### 재시도 전략 (2계층)

| 계층 | 조건 | 전략 |
|------|------|------|
| NACK 재시도 | Consumer가 NACK 응답 | 최대 3회 재전송. 소진 시 경고 로깅 후 메시지 드랍 |
| 통신 실패 재시도 | 네트워크 오류 등 | 지수 백오프 (초기 1초, 최대 60초, 배수 2) 무한 재시도. `InterruptedException`은 즉시 전파하여 graceful shutdown 보장 |

---

## consumer

```
Consumer (POST /mmmq/messages)
  → 헤더 mmmq-consumer-id 읽기            // 없거나 형식이 틀리면 NACK
    → HandlerExecutionContainer.find(consumerId)
      → 없으면 NACK
      → HandlerExecution.execute(message)  // 톰캣 요청 스레드에서 동기 실행
        → 예외 없이 끝나면 ACK, 던지면 NACK
```

큐도 워커 풀도 없습니다. 같은 HTTP 응답으로 ACK/NACK을 돌려주므로, 처리 결과가 Broker에 그대로 전달됩니다.

**토픽 패턴 매칭은 Consumer 쪽에 없습니다.** 어떤 메시지가 어디로 갈지는 Broker의 `Dispatcher`가 정하고, Consumer는 헤더의 `consumerId`만 보고 핸들러를 찾습니다.

### 핸들러 등록 방식

두 방식 모두 `[A-Za-z0-9._-]+`를 만족하는 **명시적 id**를 요구하며, 이 값이 Broker의 `dispatchers.json`에 적힌 `consumerId`와 일치해야 합니다. 시작 시점에 id가 중복되면 `IllegalStateException`으로 빈 초기화가 실패합니다.

#### 어노테이션 방식 (`@MMMQListener`)

```java
@Service
public class OrderService {

    @MMMQListener(id = "order-created")
    public void handle(Order order) {
        // ...
    }
}
```

`MethodExecution`이 리플렉션으로 메서드를 호출합니다. 파라미터 타입으로 메시지 콘텐츠를 JSON 역직렬화합니다.

#### 인터페이스 방식 (`MMMQListener<T>`)

```java
@Service
public class OrderService implements MMMQListener<Order> {

    @Override
    public String id() {
        return "order-created";
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
| `Consumer` | `POST /mmmq/messages` 수신. 헤더의 consumerId로 핸들러를 찾아 동기 실행 |
| `HandlerExecutionContainer` | id → `HandlerExecution` 레지스트리. 등록 시 중복 id 거부 |
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
        Host brokerHost = new Host(WebProtocol.HTTP, "ip", 8080);
        return new Producer(brokerHost);

        // 재시도 횟수 커스터마이징 (기본값: 3)
        // return new Producer(brokerHost, 5);
    }
}
```

### Consumer 설정

어노테이션 방식과 인터페이스 방식을 동시에 사용할 수 있습니다.

```java
@Service
public class OrderService {

    @MMMQListener(id = "order-created")
    public void handleOrder(Order order) {
        // dispatchers.json의 consumerId "order-created"가 보낸 메시지 처리
    }
}
```

```java
@Service
public class PaymentService implements MMMQListener<Payment> {

    @Override
    public String id() {
        return "payment-success";
    }

    @Override
    public void handle(Payment payment) {
        // dispatchers.json의 consumerId "payment-success"가 보낸 메시지 처리
    }
}
```

어떤 토픽이 이 핸들러로 오는지는 Consumer가 아니라 Broker의 `dispatchers.json`이 `pattern`으로 정합니다.

### Broker 설정

#### 영속화 설정 (`application.yml`)

Broker는 메시지를 디스크에 영속화합니다. 미설정 시 안전한 기본값이 사용됩니다.

```yaml
mmmq:
  broker:
    persistence:
      root-dir: ./mmmq          # 모든 영속화 파일이 저장될 루트 (기본값: ./mmmq)
      segment:
        max-bytes: 67108864     # 세그먼트 회전 임계치 (기본값: 64MiB)
```

영속화 관련 설정은 `mmmq.broker.persistence` 아래로 응집되어 있습니다. `root-dir`은 인프라 결정(저장 위치), `segment.max-bytes`는 워크로드 결정(회전 정책)입니다. Dispatcher 정의 파일은 `{root-dir}/dispatchers.json`, 토픽 데이터는 `{root-dir}/topics/` 아래에 고정되며 개별 경로 설정은 지원하지 않습니다. 부팅 시 보존된 토픽 데이터가 자동으로 복원됩니다.

#### 디렉토리 레이아웃

```
{rootDir}/                                           # 기본값 ./mmmq
├── dispatchers.json                                 # Dispatcher 정의 (고정 경로)
└── topics/
    ├── order.created/                               # 토픽 디렉토리
    │   ├── 0000000000000000000.mmm                  # 세그먼트 데이터 (.mmm)
    │   ├── 0000000000000000000.idx                  # 오프셋 인덱스 (.idx)
    │   ├── 0000000000000123456.mmm                  # 회전 후 다음 세그먼트 (123456번째 메시지부터)
    │   ├── 0000000000000123456.idx
    │   └── checkpoints/
    │       ├── order-dispatcher.checkpoint          # Dispatcher별 다음 소비 오프셋
    │       └── audit-dispatcher.checkpoint
    └── payment.kakao.success/
        └── ...
```

세그먼트와 인덱스 파일명은 19자리 zero-padded startOffset(`Long.MAX_VALUE`의 자릿수)을 사용합니다. startOffset은 해당 세그먼트의 첫 번째 메시지에 부여되는 절대 오프셋(메시지 개수 기준)입니다. 체크포인트 파일명은 `consumerId`가 그대로 사용됩니다.

#### Dispatcher 등록

`Dispatcher`는 JSON 파일에 정의합니다. 파일 경로는 `{mmmq.broker.persistence.root-dir}/dispatchers.json`으로 고정됩니다(기본값 `./mmmq/dispatchers.json`). 별도 경로 설정은 지원하지 않습니다. 파일이 없으면 빈 배열(`[]`) 파일을 생성하고 Dispatcher 없이 기동합니다.

`DispatcherContainer`가 생성자에서 이 파일을 읽어 `Dispatcher`를 만들고 소유합니다. `Dispatcher`는 스프링 빈이 아니며, 태어나는 길은 이 파일 하나입니다.

최상위는 정의 배열이며, 한 항목은 하나의 `consumerId`·하나의 패턴에 대응합니다. `consumerId`는 체크포인트 파일명으로 사용되므로 `[A-Za-z0-9._-]+` 패턴을 따라야 합니다.

```json
[
  {
    "consumerId": "order-created",
    "host": "http://consumer-host:8080",
    "pattern": "order.created"
  },
  {
    "consumerId": "payment-success",
    "host": "https://consumer-host:8443",
    "pattern": "payment.kakao.success"
  }
]
```

`host`는 절대 URL이며 스킴은 `http` 또는 `https`입니다(대소문자를 가리지 않습니다). **포트는 필수입니다** — 소비자는 보통 비표준 포트를 쓰기 때문에, 포트를 빠뜨린 주소를 80·443으로 조용히 돌리는 대신 거절합니다. 경로·query·userInfo·fragment는 붙일 수 없습니다. 그 성분들은 저장·응답 왕복에서 소실되는데, 특히 경로는 무해하게 사라지지 않고 소비자로 보내는 요청 경로를 바꿔 버립니다.

파일 내 `consumerId`가 중복되거나 스킴이 지원되지 않거나 JSON이 깨져 있으면 컨텍스트 기동을 실패시킵니다.

#### 런타임 관리

브로커를 재시작하지 않고 Dispatcher를 추가·수정·삭제할 수 있습니다. 변경은 메모리를 건드리기 전에 `dispatchers.json`에 먼저 반영되므로(임시 파일에 쓴 뒤 `ATOMIC_MOVE`로 교체) 재시작해도 살아남습니다.

| 메서드 | 경로 | 응답 |
|---|---|---|
| `GET` | `/mmmq/dispatchers` | `200` 현재 정의 목록 |
| `POST` | `/mmmq/dispatchers` | `201` 등록된 정의 · `400` · `409` 중복 `consumerId` |
| `PUT` | `/mmmq/dispatchers/{consumerId}` | `200` 바뀐 정의 · `400` · `404` |
| `DELETE` | `/mmmq/dispatchers/{consumerId}` | `204` · `400` · `404` |

`PUT`의 본문은 `host`와 `pattern`뿐입니다. `consumerId`는 식별자이지 바꿀 수 있는 값이 아닙니다.

HTTP가 주고받는 타입과 파일에 저장되는 타입은 **서로 다릅니다**. 컴포넌트는 같지만 의도적으로 나눠 둔 것이라, API를 위한 애노테이션이 디스크 포맷에 새어 들거나 파일 포맷 변경이 API 계약을 깨는 일이 없습니다. 둘 다 메서드가 없는 순수 record이고, 변환은 `DispatcherContainer`가 전담합니다.

검증 실패 메시지는 `core` 값 타입이 던지는 것이라 **검증 대상 파라미터 이름**을 주어로 씁니다(`url must be an absolute URL, but was: ...`). 요청 본문의 필드명(`host`)과 다르지만 문제가 된 값이 그대로 실려 나오고, 이 방식이라야 `core`가 브로커의 와이어 스키마를 모른 채로 남습니다.

```bash
curl -X POST localhost:8080/mmmq/dispatchers \
  -H 'Content-Type: application/json' \
  -d '{"consumerId":"order-created","host":"http://consumer-host:8080","pattern":"order.*"}'
```

변경은 `DispatcherContainer` 안의 단일 락으로 직렬화되고, 메시지 핫패스인 구독자 조회는 락을 잡지 않습니다.

새 구독은 로그의 **tail**에서 시작합니다. 런타임에 소비자를 붙여도 쌓여 있던 메시지가 한꺼번에 재생되지 않습니다. 반대로 구독이 끝나면 — Dispatcher를 삭제했거나 패턴을 좁혀 토픽이 빠졌거나 — 그 토픽의 `<consumerId>.checkpoint`도 함께 지워집니다.

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
