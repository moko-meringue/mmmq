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
      → TopicQueueContainer.get(topic)  [없으면 생성 + TopicQueueInitializedEvent 발행]
      → TopicQueue.offer(message)
          → SegmentFileChain에 엔트리 append + fsync
          → 인덱스 파일에 주소 append + fsync  ← 메시지 commit point
          → ACK/NACK을 Producer에게 응답
          → MessageArrivedEvent 발행
        → Dispatcher.onMessageArrived(event) [EventListener]
            → WorkerPool에 drain 태스크 제출
              → drain(): TopicQueue.peek(offset)으로 루프 소비
                → Sender.send(message, maxNackRetry=3)
                  → HTTP POST /mmmq/messages → Consumer
                    → FrontHandler: BlockingQueue(1000) + ThreadPoolExecutor(2~5)
                      → HandlerExecution(@MMMQListener 또는 MMMQListener<T>)
                → 전송 완료 시: TopicQueue.commit(name, offset)으로 체크포인트 fsync
                → NACK 소진 시: 경고 로깅 후 메시지 드랍
                → 통신 실패 시: 지수 백오프(1s→60s) 무한 재시도
```

## 부팅 시 복원 흐름

```
TopicQueueBootstrapper [SmartInitializingSingleton]
  → {rootDir} 하위 토픽 디렉토리 스캔
  → 각 토픽에 대해 TopicQueueContainer.register(topic)
      → TopicQueueFactory.create(topic)
          → SegmentFileChain.open: 세그먼트별 SegmentFile.recover()
              → 인덱스에 commit되지 않은 partial write 영역 truncate
          → CheckpointDirectory.open: Dispatcher별 체크포인트 파일 로드
      → TopicQueueInitializedEvent 발행
        → 패턴이 일치하는 Dispatcher가 자동 subscribe
            → 체크포인트 파일에서 다음 소비 오프셋 로드

ApplicationReadyEvent
  → 모든 Dispatcher가 자기 subscriptions를 WorkerPool에 submit
  → drain 시작
```

---

# 📦 모듈 구조

```
core/       # 공유 타입: Message, Topic, TopicPattern, Acknowledgement
producer/   # Producer 빈 + Gateway (RestClient → POST /mmmq/messages → Broker)
consumer/   # Consumer REST 엔드포인트 + FrontHandler + HandlerExecution
broker/     # Broker REST 엔드포인트 + FrontDispatcher + Dispatcher + 영속화 저장소
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
| `TopicPattern` | Ant 스타일 와일드카드 패턴 매칭. `matches(Topic)`으로 토픽 매칭 여부 확인 |
| `Acknowledgement` | ACK / NACK 응답 열거형 |
| `BrokerAcknowledgement` | Broker → Producer 응답 레코드 |
| `ConsumerAcknowledgement` | Consumer → Broker 응답 레코드 |
| `Host` | `WebProtocol + InetAddress + port` 조합의 네트워크 엔드포인트 |

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
  1. TopicQueueContainer.get(topic)으로 해당 토픽의 큐 조회/생성
  2. TopicQueue.offer(message) 호출
  3. offer 결과를 Acknowledgement(ACK/NACK)로 반환
  4. ACK인 경우 MessageArrivedEvent 발행
```

신규 토픽이 등장하면 `TopicQueueContainer.get`이 `computeIfAbsent`로 새 큐를 생성하고 `TopicQueueInitializedEvent`를 발행하여, 패턴이 일치하는 모든 `Dispatcher`가 자동으로 구독합니다.

---

### TopicQueue

토픽 단위의 메시지 큐입니다. 메시지를 디스크에 영속화하고, Dispatcher별 오프셋 체크포인트를 관리합니다.

```
TopicQueue.subscribe(dispatcherName)         // 체크포인트에서 다음 소비 오프셋 로드 (없으면 0L 초기화)
TopicQueue.offer(message) → boolean          // 디스크 append + fsync. 실패 시 false
TopicQueue.peek(offset) → Message            // 인덱스를 이용한 random access read
TopicQueue.commit(dispatcherName, offset)    // 다음 소비 오프셋을 체크포인트에 fsync
TopicQueue.close()                           // 보유 자원(SegmentFileChain, CheckpointDirectory) 해제
```

`offer`는 `ReentrantLock`으로 단일 writer를 보장합니다. 다수의 Producer가 동시에 호출해도 같은 토픽의 segment append + 인덱스 update가 atomic 단위로 직렬화됩니다. `peek`은 lock-free이며, 인덱스에 commit되지 않은 in-flight 엔트리를 보지 않습니다.

| 클래스 | 설명 |
|--------|------|
| `TopicQueue` | 토픽 단위 큐. `subscribe` / `offer` / `peek` / `commit` API 제공. `Closeable` |
| `TopicQueueContainer` | `ConcurrentHashMap<Topic, TopicQueue>` 보관. 신규 큐 생성 시 `TopicQueueInitializedEvent` 발행 |
| `TopicQueueFactory` | 토픽 디렉토리·세그먼트·체크포인트를 조립하여 `TopicQueue` 인스턴스를 만드는 팩토리 |
| `TopicQueueBootstrapper` | 부팅 시 `{rootDir}` 하위 디렉토리를 스캔하여 보존된 토픽 큐를 복원 (`SmartInitializingSingleton`) |
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

새 메시지 append 직전에 현재 tail 세그먼트의 파일 크기가 `mmmq.broker.segment.max-bytes` 이상이면 새 세그먼트를 열고 거기에 append합니다. 임계치는 soft cap이므로 단일 메시지가 세그먼트 중간에 절단되지 않습니다.

#### Dispatcher별 체크포인트

각 Dispatcher는 `subscribe`로 토픽에 등록될 때 `checkpoints/{dispatcherName}.checkpoint` 파일을 생성/로드합니다. 매 메시지 전달 완료 시 `commit`이 다음 소비 오프셋을 fsync합니다. 새로 등록되는 Dispatcher는 0L에서 시작하여 토픽에 보존된 모든 메시지를 처음부터 catch-up합니다.

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

Consumer에게 메시지를 실제로 전달하는 핵심 컴포넌트입니다. 하나의 `Dispatcher`는 하나 이상의 `TopicPattern`을 가지며, 매칭되는 모든 `TopicQueue`를 구독합니다.

#### 구독 등록 (이벤트 기반)

```
TopicQueueInitializedEvent 수신
  → 패턴 매칭 확인
  → topicQueue.subscribe(name)로 체크포인트에서 다음 소비 오프셋 로드
  → subscriptions 맵에 (TopicQueue → Offset) 저장
```

신규 토픽 등장 시점(런타임 또는 부팅 복원)에 자동으로 구독이 이루어집니다. 외부에서 `subscribe`를 직접 호출할 필요가 없습니다.

#### 이벤트 기반 소비 (drain-loop 패턴)

```
MessageArrivedEvent 수신
  → 해당 TopicQueue가 subscriptions에 있는지 확인
  → WorkerPool.submit(drain 태스크)
      → 이미 실행 중이면 ArrayBlockingQueue(1)에 대기
      → 큐도 가득 차면 DiscardPolicy로 무시 (drain 루프가 미소비 메시지 모두 처리)

drain(topicQueue):
  Offset offset = subscriptions.get(topicQueue);
  while (true) {
      Message message = topicQueue.peek(offset);
      if (message == null) return;
      deliver(message);
      offset = topicQueue.commit(name, offset);
      subscriptions.put(topicQueue, offset);
  }
```

**drain-loop의 안전성:** 제출이 DiscardPolicy로 무시되더라도 메시지는 디스크에 영속화되어 있습니다. 실행 중인 drain 태스크가 루프를 돌며 모든 미소비 메시지를 처리합니다.

**손상 엔트리 격리:** drain 루프는 `CorruptionException`을 catch하여 해당 오프셋을 로깅 후 skip하고, 다음 오프셋으로 진행합니다. 손상된 엔트리 한 건이 토픽 전체의 진행을 막지 않습니다.

#### WorkerPool (Dispatcher 내부)

토픽별 worker thread pool을 관리하는 nested class입니다.

- `pool`: `ConcurrentHashMap<TopicQueue, ExecutorService>`. 토픽이 처음 등장할 때 worker 생성.
- 각 worker: `ThreadPoolExecutor(coreSize=0, maxSize=1, keepAlive=60s, queue=ArrayBlockingQueue(1), DiscardPolicy)`
  - `coreSize=0`: 유휴 60초 후 스레드 자동 종료 → 트래픽 없을 때 리소스 절약
  - `maxSize=1`: 토픽별 메시지 순서 보장
  - `ArrayBlockingQueue(1)` + `DiscardPolicy`: 중복 신호 무시

#### Dispatcher 이름 검증

Dispatcher 이름은 체크포인트 파일명의 일부로 사용되므로 `[A-Za-z0-9._-]+` 패턴으로 검증됩니다. 패턴 위반 시 `IllegalArgumentException`으로 빈 등록 자체를 거부합니다.

#### 재시도 전략 (2계층)

| 계층 | 조건 | 전략 |
|------|------|------|
| NACK 재시도 | Consumer가 NACK 응답 | 최대 3회 재전송. 소진 시 경고 로깅 후 메시지 드랍 |
| 통신 실패 재시도 | 네트워크 오류 등 | 지수 백오프 (초기 1초, 최대 60초, 배수 2) 무한 재시도. `InterruptedException`은 즉시 전파하여 graceful shutdown 보장 |

---

## consumer

```
Consumer (POST /mmmq/messages)
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
    public TopicPattern listens() {
        return new TopicPattern("order.*");
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
    public TopicPattern listens() {
        return new TopicPattern("payment.*");
    }

    @Override
    public void handle(Payment payment) {
        // payment.* 패턴의 메시지 처리
    }
}
```

### Broker 설정

#### 영속화 설정 (`application.yml`)

Broker는 메시지를 디스크에 영속화합니다. 미설정 시 안전한 기본값이 사용됩니다.

```yaml
mmmq:
  broker:
    storage:
      root-dir: ./data        # 토픽별 디렉토리가 생성될 루트 (기본값: ./data)
    segment:
      max-bytes: 67108864     # 세그먼트 회전 임계치 (기본값: 64MiB)
```

`storage.root-dir`은 인프라 결정(저장 위치), `segment.max-bytes`는 워크로드 결정(회전 정책)으로 변경 축이 다르기 때문에 별도 그룹으로 분리되어 있습니다. 부팅 시 해당 디렉토리에 보존된 토픽 데이터가 자동으로 복원됩니다.

#### 디렉토리 레이아웃

```
{rootDir}/
├── order.created/                              # 토픽 디렉토리
│   ├── 0000000000000000000.mmm                 # 세그먼트 데이터 (.mmm)
│   ├── 0000000000000000000.idx                 # 오프셋 인덱스 (.idx)
│   ├── 0000000000000123456.mmm                 # 회전 후 다음 세그먼트 (123456번째 메시지부터)
│   ├── 0000000000000123456.idx
│   └── checkpoints/
│       ├── order-dispatcher.checkpoint         # Dispatcher별 다음 소비 오프셋
│       └── audit-dispatcher.checkpoint
└── payment.kakao.success/
    └── ...
```

세그먼트와 인덱스 파일명은 19자리 zero-padded startOffset(`Long.MAX_VALUE`의 자릿수)을 사용합니다. startOffset은 해당 세그먼트의 첫 번째 메시지에 부여되는 절대 오프셋(메시지 개수 기준)입니다. 체크포인트 파일명은 `consumerId`가 그대로 사용됩니다.

#### Dispatcher 등록

`Dispatcher`는 JSON 파일에 정의하며, 부팅 시 각 정의가 스프링 빈으로 등록됩니다. 파일 경로는 `mmmq.broker.dispatchers.file`로 지정하고 기본값은 `./dispatchers.json`입니다. 파일이 없으면 빈 배열(`[]`) 파일을 생성하고 Dispatcher 없이 기동합니다.

최상위는 정의 배열이며, 한 항목은 하나의 `consumerId`·하나의 패턴에 대응합니다. `consumerId`는 체크포인트 파일명으로 사용되므로 `[A-Za-z0-9._-]+` 패턴을 따라야 합니다.

```json
[
  {
    "consumerId": "order-created",
    "host": { "protocol": "HTTP", "address": "consumer-host", "port": 8080 },
    "pattern": "order.created"
  },
  {
    "consumerId": "payment-success",
    "host": { "protocol": "HTTP", "address": "consumer-host", "port": 8081 },
    "pattern": "payment.kakao.success"
  }
]
```

`protocol`은 `HTTP` 또는 `HTTPS`이며 대소문자를 가리지 않습니다. 파일 내 `consumerId`가 중복되거나 알 수 없는 `protocol`·깨진 JSON 등 잘못된 정의가 있으면 컨텍스트 기동을 실패시킵니다.

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
