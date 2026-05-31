# Dispatcher를 HandlerExecution 대리인으로 전환

## 배경

현재 MMMQ는 Dispatcher가 *Consumer 호스트 단위*로 동작한다. 한 디스패처가 한 Consumer로 메시지를 보내면, Consumer 측 `FrontHandler`는 들어온 메시지 토픽에 매칭되는 모든 HandlerExecution(이하 HE)에 fan-out하여 `ThreadPoolExecutor`에서 병렬로 실행한다. 그리고 큐에 메시지를 넣자마자 즉시 ACK 응답이 나간다.

이 구조에서 두 가지 문제가 있다.

**원자성 문제**: ACK 응답 후 비동기로 실행되는 HE 중 일부가 실패해도 Broker는 모른다. Broker는 ACK 받은 즉시 Offset commit. 처리되지 않은 HE가 있어도 재시도 경로가 없다. at-least-once 의미론이 깨진다.

**격리 부재**: 한 Consumer 안에서 같은 토픽을 받는 HE가 여럿이면, 한 HE의 지연이 `ThreadPoolExecutor`의 워커를 점유해 다른 HE 처리에도 영향을 미친다.

## 해결 모델

**Dispatcher를 HE 대리인으로 전환한다.** 1 Dispatcher = 1 HE. 같은 Consumer 호스트로 여러 디스패처가 갈 수 있고, 각자 다른 HE를 대상으로 한다. 같은 토픽을 처리하는 HE가 N개이면 디스패처도 N개가 되어 메시지가 N번 별도 HTTP 요청으로 분리 전송된다.

이 구조 변경은 부하분산이나 처리량 증가가 목적이 아니라, 원자성과 격리 확보가 목적이다.

## 결정된 사양

### 1. 응답 모델: 동기

Consumer Controller가 HE 실행을 동기로 기다린 뒤 ACK/NACK 응답한다. HE 성공 → ACK, HE 실패 → NACK. 디스패처의 기존 NACK 재시도(3회) + 지수 백오프(통신 실패 시) 메커니즘이 그대로 맞물린다.

### 2. 등록 방식: 정적

Broker 측 `@Bean Dispatcher(...)`로 운영자가 토폴로지를 명시적으로 등록한다. Consumer→Broker 동적 등록 프로토콜은 두지 않는다. 현재 MMMQ의 운영 모델을 유지한다.

### 3. HE 식별: 문자열 ID 필수 + 양쪽 유일성

- `@MMMQListener` 어노테이션과 `MMMQListener<T>` 인터페이스가 모두 ID를 필수로 받는다. 디폴트 없음.
- 같은 ID를 가진 HE가 한 Consumer 애플리케이션에 둘 이상 존재하면 등록 시점에 예외를 던지고 기동을 실패시킨다.
- Broker 측도 같은 유일성을 강제한다: 같은 `handlerId`로 두 번째 디스패처가 등록되면 예외를 던지고 기동을 실패시킨다. **1 ID = 1 디스패처 = 1 HE** 대칭 유지.
- Consumer는 ID로만 라우팅한다. 패턴 매칭은 Consumer 측에 더 이상 두지 않는다.
- 들어온 메시지의 헤더 ID와 일치하는 HE가 없으면 NACK를 반환한다.

ID를 필수로 두는 이유: 자동 생성(예: 클래스명#메서드명) 디폴트는 리네임 시 운영자 모르게 바뀌어 Broker의 `handlerId` 문자열과 어긋난다. 컴파일에서 잡히지 않는 런타임 NACK 폭주로 이어진다.

Broker·Consumer 양쪽 유일성을 함께 강제하는 이유: 한쪽만 유일성을 가지면 의미가 어긋난다(Broker가 같은 ID로 디스패처 둘을 가지면 같은 메시지를 같은 HE로 두 번 보낼 위험 + 운영자가 디스패처 단위와 HE 단위를 다르게 인지). 양쪽이 동일 강도의 유일성을 가져야 운영 모델이 단순.

### 4. ID 전달 채널: `Metadata` 도메인 추상화

- `core` 모듈에 `Metadata` 단일 클래스를 둔다. 자체 도메인 추상화로 매체와 무관(Apache Kafka `Headers` 패턴에 가까움).
- 내부적으로 `Map<String, String>`을 보유. handlerId는 헤더 이름 `MMMQ-Handler-Id`로 저장(상수는 package-private, 외부는 도메인 메서드만 사용).
- 외부 노출 API: `setHandlerId(String)`, `@Nullable getHandlerId()`, `Map<String, String> toMap()`, `Metadata(Map<String, String>)` 생성자.
- Broker는 `Metadata`를 새로 만들어 `setHandlerId` 후 `toMap()`으로 변환하여 HTTP 요청 헤더에 주입.
- Consumer Controller는 `@RequestHeader Map<String, String>`으로 헤더를 받아 `new Metadata(headers)`로 만든 뒤 `getHandlerId()` 호출.
- 인터페이스 추상화는 두지 않는다. 매체별 carrier(`HttpHeaders`, gRPC `Metadata`)에 직접 의존하지도 않는다 — `core`는 spring-web 의존 없이 순수 도메인 모듈로 유지.
- gRPC 같은 다른 매체가 추가되면 broker/consumer의 wire 레이어가 `Metadata`와 그 매체 사이를 변환만 한다. `Metadata` 자체는 변경 없음.

```java
package org.mmmq.core.metadata;

public class Metadata {

    static final String HANDLER_ID = "MMMQ-Handler-Id";

    private final Map<String, String> headers;

    public Metadata() {
        this.headers = new HashMap<>();
    }

    public Metadata(Map<String, String> source) {
        this.headers = new HashMap<>(source);
    }

    public void setHandlerId(String handlerId) {
        headers.put(HANDLER_ID, handlerId);
    }

    @Nullable
    public String getHandlerId() {
        return headers.get(HANDLER_ID);
    }

    public Map<String, String> toMap() {
        return Map.copyOf(headers);
    }
}
```

**세부 결정 근거**:
- **도메인 메서드**: 범용 `put/get` 노출이 아닌 `setHandlerId`/`getHandlerId`로 두는 이유는 자체 타입의 가치가 "MMMQ 도메인 의도를 코드에서 드러내기"이기 때문. 범용 Map-like 인터페이스만 노출하면 `Map<String, String>`을 직접 쓰는 것과 차이가 없어짐.
- **가변 객체**: Broker 측 사용 흐름이 "생성 → handlerId 채움 → toMap"으로 짧고 직선적. 불변 빌더는 작은 가치 객체에 비해 무게가 큼. Lombok `@Setter` 자동 생성은 그대로 금지(CLAUDE.md §Lombok) — 명시적 도메인 메서드로 직접 작성.
- **`HANDLER_ID` 상수 package-private**: 외부 모듈이 헤더 이름 문자열을 직접 참조할 수 없도록 캡슐화. 헤더 이름은 `Metadata` 내부 구현 상세.
- **`toMap()` 불변 반환**: `Map.copyOf(headers)`로 호출자가 변경 못함.

### 5. Pattern은 Dispatcher 단독 보유

- HE는 ID만 가진다. 패턴 정보를 더 이상 가지지 않는다.
- Dispatcher가 *단일* `TopicPattern`을 보유한다(`List<TopicPattern>` 아님). 1 디스패처 = 1 HE = 1 pattern.
- 한 HE가 여러 토픽을 받아야 하면 두 경로로 흡수: (a) pattern을 와일드카드로 둠(예: `order.*`로 `order.created`·`order.updated`를 한 디스패처가 흡수), (b) 와일드카드로 묶이지 않는 토픽 그룹이면 별도 HE/ID/디스패처로 분리하고 비즈니스 로직은 공통 메서드 호출로 위임. 같은 `handlerId`로 디스패처를 두 번 등록하는 경로는 §3의 유일성 룰에 의해 금지된다.
- Consumer 측에는 pattern 매칭 로직이 남지 않는다.

### 6. Consumer 측 처리: Tomcat 스레드에서 직접 실행

- 큐(`ArrayBlockingQueue(1000)`), 단일 Worker 스레드, `ThreadPoolExecutor(2~5)`를 모두 제거한다.
- `Consumer` Controller가 헤더에서 ID 추출 → 저장소 조회 → HE 실행 → 결과로 응답하는 한 흐름으로 처리한다.
- 동시 처리량은 Tomcat 스레드 풀(`server.tomcat.threads.max`)이 담당한다.

격리는 디스패처 분리로 이미 보장된다(Broker의 디스패처 Subscription은 단일 워커이므로 동일 HE에 대한 동시 호출이 같은 디스패처에서 발생하지 않는다). Consumer 측 추가 격리 메커니즘은 중복이다.

### 7. NACK 케이스

- ID 미일치, HE 실행 중 예외 — 모두 동일한 NACK 응답을 반환한다.
- 로그도 케이스별 구분 없이 단일 메시지로 둔다(향후 필요 시 분리 가능).

### 8. `HandlerExecutions` 자료구조

- `Map<String, HandlerExecution> byId = new ConcurrentHashMap<>()`로 단순 ID 조회 자료구조로 교체.
- `add(HandlerExecution)`: 중복 ID 발견 시 예외 throw.
- `find(String id)`: `@Nullable HandlerExecution` 반환. `Optional` wrap 비용을 피한다. 호출자는 즉시 null 체크.
- 기존 `List + Map<Topic, List<HE>> topicCache` 패턴 매칭 캐시를 제거.

## 컴포넌트별 변경 명세

### `core`

**신규: `core/src/main/java/org/mmmq/core/metadata/Metadata.java`**

위 §4의 정의를 그대로 둔다.

**변경 없음**: `core/build.gradle`. `Metadata`가 JDK 자료형(`Map`, `HashMap`)만 사용하므로 외부 의존(spring-web 등) 추가 없음. `core` 모듈은 도메인 순수성을 유지.

### `broker`

**변경: `broker/src/main/java/org/mmmq/broker/dispatcher/Dispatcher.java`**

- 시그니처: `Dispatcher(Host host, String handlerId, TopicPattern pattern)`. `name` 필드 제거.
- 필드: `patterns: List<TopicPattern>` → `pattern: TopicPattern`. `handlerId: String` 추가. `name: String` 제거.
- 식별자 통합: `handlerId`가 디스패처의 단일 식별자로 쓰인다. checkpoint 파일명도 `handlerId` 기준(`<handlerId>.checkpoint`), 로그도 `handlerId`로 표기.
- 기존 `name`이 강제하던 file-safe 정규식(`[A-Za-z0-9._-]+`) 검증을 `handlerId`로 옮긴다. 생성자에서 인라인 검증(추출 헬퍼 없이 `if (!NAME_PATTERN.matcher(handlerId).matches()) throw ...` 직접 둠).
- `matches(Topic)`: `pattern.matches(topic)` 한 줄로 단순화.
- `deliver(message)` → `sender.send(message, handlerId, MAX_NACK_RETRY_COUNT)`로 `handlerId` 전달.
- `TopicQueue.subscribe(...)`, `topicQueue.commit(...)`에 넘기던 `name` 인자를 모두 `handlerId`로 교체.
- **`@EventListener` 메서드들 모두 제거**: `onApplicationReady`, `onTopicQueueInitialized`, `onMessageArrived`. 디스패처는 더 이상 Spring 이벤트 리스너가 아니다. `DispatcherContainer`가 적절한 시점에 직접 호출.
- 외부 호출 대상 메서드(`subscribe(TopicQueue)`, `drain(TopicQueue)`, `matches(Topic)`)는 같은 `org.mmmq.broker.dispatcher` 패키지의 `DispatcherContainer`에서 호출되므로 package-private으로 노출.

**디스패처 유일성 + 라우팅**: 별도 컴포넌트 `DispatcherContainer`가 두 책임을 함께 담당(아래 §). `Dispatcher` 자체는 자기 `handlerId`만 알고 등록·검증·라우팅 책임은 가지지 않는다.

**신규: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherContainer.java`**

`Dispatcher` Bean 모음을 보유하며 다음 두 책임을 수행:
1. **유일성 검증**: `handlerId` 중복 검사 후 인덱스 확정.
2. **라우팅**: `TopicQueue` 초기화 시 매칭 Dispatcher들에게 `subscribe` 호출 + 매칭 인덱스 갱신. 메시지 도착 시 인덱싱된 매칭 Dispatcher들에게 `drain` 호출.

`SmartInitializingSingleton` 구현. `ObjectProvider<Dispatcher>` 생성자 주입. `afterSingletonsInstantiated()` 시점에 검증과 인덱스 초기화를 *한 번에* 수행.

```java
@Slf4j
@Component
public class DispatcherContainer implements SmartInitializingSingleton {

    private final ObjectProvider<Dispatcher> dispatcherProvider;
    private final Map<String, Dispatcher> byHandlerId = new HashMap<>();
    private final Map<TopicQueue, List<Dispatcher>> subscribersByQueue = new ConcurrentHashMap<>();

    public DispatcherContainer(ObjectProvider<Dispatcher> dispatcherProvider) {
        this.dispatcherProvider = dispatcherProvider;
    }

    @Override
    public void afterSingletonsInstantiated() {
        dispatcherProvider.stream()
                .forEach(dispatcher -> {
                    Dispatcher previous = byHandlerId.putIfAbsent(dispatcher.handlerId(), dispatcher);
                    if (previous != null) {
                        throw new IllegalStateException(
                                "Duplicate handlerId '" + dispatcher.handlerId() + "' across multiple Dispatcher beans"
                        );
                    }
                });
    }

    public void onTopicQueueInitialized(TopicQueue topicQueue) {
        List<Dispatcher> matched = byHandlerId.values().stream()
                .filter(dispatcher -> dispatcher.matches(topicQueue.getTopic()))
                .toList();
        matched.forEach(dispatcher -> dispatcher.subscribe(topicQueue));
        subscribersByQueue.put(topicQueue, matched);
    }

    public void dispatch(TopicQueue topicQueue) {
        List<Dispatcher> subscribers = subscribersByQueue.get(topicQueue);
        if (subscribers == null) {
            return;
        }
        subscribers.forEach(dispatcher -> {
            try {
                dispatcher.drain(topicQueue);
            } catch (Exception exception) {
                log.warn(
                        "Dispatcher '{}' failed during drain on topic '{}'",
                        dispatcher.handlerId(),
                        topicQueue.getTopic(),
                        exception
                );
            }
        });
    }
}
```

`dispatch()`의 람다 안에서 `Exception`을 잡아 로깅만 하고 다음 디스패처로 진행한다. 한 디스패처의 예기치 못한 예외(RuntimeException, 통신 실패 후 전파되는 예외 등)가 같은 큐를 구독하는 다른 디스패처의 `drain`을 막지 않도록 격리. 본 변경의 격리 목적과 정합.

**`ObjectProvider` + `SmartInitializingSingleton` 조합 근거**:
1. **시점 안전**: `afterSingletonsInstantiated()`는 모든 싱글톤 인스턴스화 완료 후 호출. `dispatcherProvider.stream()`이 그 시점에 완전한 집합을 반환.
2. **권한 좁음**: `BeanFactory` 전체 노출 없이 `Dispatcher` 타입만 노출. 책임 경계 단순.
3. **테스트 친화**: `ObjectProvider`는 collection mock이 단순. 컨테이너 부트스트랩 없이도 단위 테스트 가능.
4. **한 번에 초기화**: 검증과 인덱스 갱신을 같은 콜백에서 일괄 수행. 부분 초기화 상태가 외부에 노출되지 않음.

검증 실패 시 `IllegalStateException`이 컨테이너 기동 단계에서 던져져 애플리케이션 기동이 실패한다(Spring 표준 동작).

`SmartInitializingSingleton` 자체를 선택한 이유는 (a) `@PostConstruct`는 자기 Bean 인스턴스화 직후 콜백이라 늦게 등록되는 Bean을 놓칠 위험, (b) `BeanPostProcessor`는 개별 Bean 단위 콜백이라 "전체 컬렉션 검증" 의도와 안 맞음, (c) 인터페이스 자체가 "싱글톤 전체 초기화 후 후처리" 의도를 명확히 표현 — 세 가지를 함께 만족하는 표준 메커니즘이 `SmartInitializingSingleton`이기 때문.

**변경: `broker/src/main/java/org/mmmq/broker/dispatcher/FrontDispatcher.java`**

- 생성자 주입에 `DispatcherContainer` 추가. `ApplicationEventPublisher` 의존 제거.
- `dispatch(message)`에서 `publisher.publishEvent(new MessageArrivedEvent(queue))` 호출 제거. 대신 `dispatcherContainer.dispatch(topicQueue)`를 직접 호출.

**변경: `broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueueContainer.java`**

(현재 새 `TopicQueue` 생성 시 `TopicQueueInitializedEvent`를 발행하던 곳)
- 생성자 주입에 `DispatcherContainer` 추가.
- 큐 생성 직후 이벤트 발행을 제거. 대신 `dispatcherContainer.onTopicQueueInitialized(topicQueue)`를 직접 호출.

**제거: `broker/src/main/java/org/mmmq/broker/dispatcher/MessageArrivedEvent.java`**

이벤트 클래스 자체 제거. 직접 호출로 대체되어 더 이상 발행/수신되지 않음.

**제거: `broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueueInitializedEvent.java`**

마찬가지로 클래스 자체 제거.

**변경: `broker/src/main/java/org/mmmq/broker/dispatcher/sender/Sender.java`**

- `send(Message, String handlerId, int maxRetryCount)`로 시그니처 변경.
- `post(Message)` → `post(Message, String handlerId)`. 내부에서 `Metadata metadata = new Metadata(); metadata.setHandlerId(handlerId);` 후 RestClient 호출 시 `.headers(httpHeaders -> metadata.toMap().forEach(httpHeaders::set))` 추가.
- `Sender.from(Host)` 정적 팩토리 시그니처는 변경 없음(Metadata는 매 호출마다 새로 만드는 가치 객체이므로 주입 대상이 아님).

**변경: `broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueue.java`**

- 매개변수 리네이밍: `dispatcherName` → `name`. TopicQueue는 호출자가 누구인지(Dispatcher든 다른 컴포넌트든)에 무관해야 하므로 추상적 식별자만 받는 의미로 매개변수 이름을 단순화.
- 영향 메서드: `subscribe(String dispatcherName)` → `subscribe(String name)`, `commit(String dispatcherName, Offset offset)` → `commit(String name, Offset offset)`.
- 본문 내 `dispatcherName` 변수 사용 모두 `name`으로 변경(`checkpointDirectory.get(name)` 등).
- 예외 메시지도 일관 정리: `"Cannot commit: '" + name + "' has not subscribed topic '" + topic.name() + "'"`.
- 호출 사이트(Dispatcher)는 자기 `handlerId`를 그대로 넘김(`topicQueue.commit(handlerId, offset)`, `topicQueue.subscribe(handlerId)`). 호출 사이트의 변수 이름과 메서드 시그니처 매개변수 이름은 별개의 추상화 경계.

**변경 없음**: `Offset`, `CheckpointFile`, `CheckpointDirectory`. 디스패처 수가 늘면 `.checkpoint` 파일이 늘긴 하지만, 본 변경의 직접 대상이 아니다.

### `consumer`

**변경: `consumer/src/main/java/org/mmmq/consumer/Consumer.java`**

- 생성자 주입: `HandlerExecutions`만(Metadata는 주입 대상 아닌 가치 객체).
- `receiveMessage(@RequestHeader Map<String, String> headers, @RequestBody Message message)` 시그니처.
- 처리 흐름:
  1. `Metadata metadata = new Metadata(headers);` `metadata.getHandlerId()`로 ID 추출. null이면 NACK.
  2. `handlerExecutions.find(id)`로 HE 조회. null이면 NACK.
  3. `handlerExecution.execute(message)` 호출. 정상 종료 → ACK. 예외 catch → NACK.

**제거: `consumer/src/main/java/org/mmmq/consumer/handler/FrontHandler.java`**

이 변경의 결과로 `FrontHandler`가 보유하던 큐·Worker·`ThreadPoolExecutor`가 모두 의미를 잃고, 남는 책임이 `HandlerExecutions` 위임뿐이라 별도 클래스로 둘 가치가 없다. 클래스 자체를 제거하고 `Consumer`가 `HandlerExecutions`를 직접 주입받는다.

**변경: `consumer/src/main/java/org/mmmq/consumer/handler/execution/HandlerExecutions.java`**

- `@Component`로 변환(기존에는 `FrontHandler` 안에서 `new`로 생성되던 객체). Bean으로 주입 가능.
- 필드: `Map<String, HandlerExecution> byId = new ConcurrentHashMap<>()`. 기존 `List + topicCache` 제거.
- `add(HandlerExecution)`: 중복 ID 시 예외 throw(메시지에 ID 포함).
- `@Nullable HandlerExecution find(String id)`: ID로 즉시 조회.
- 기존 `getExecutions(Message)`(패턴 매칭) 제거.

**변경: `consumer/src/main/java/org/mmmq/consumer/handler/execution/HandlerExecution.java`**

- 필드: `name`, `pattern` 모두 제거. `id: String`만 보유.
- 메서드: `supports(Message)` 제거. `getName()`, `getPattern()` 제거. `id()` getter 추가.
- 생성자: `HandlerExecution(String id)`.

**변경: `consumer/src/main/java/org/mmmq/consumer/handler/execution/method/MMMQListener.java`**

```java
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MMMQListener {

    String id();
}
```

기존 `value()`/`pattern()` 속성 제거. 디폴트 없음(required).

**변경: `consumer/src/main/java/org/mmmq/consumer/handler/execution/type/MMMQListener.java`**

```java
public interface MMMQListener<T> {

    String id();

    void handle(T content);
}
```

기존 `listens()` 메서드 제거. `id()` 추가.

**변경: `consumer/src/main/java/org/mmmq/consumer/handler/execution/method/MethodExecution.java`**

- 생성자: `MethodExecution(String id, Object bean, Method method, ObjectMapper objectMapper)`.
- `super.name = bean.getClass().getCanonicalName() + "#" + method.getName()` 자동 생성 로직 제거.
- 어노테이션의 `id()`에서 받은 값을 그대로 `super(id)`로 전달.

**변경: `consumer/src/main/java/org/mmmq/consumer/handler/execution/type/InterfaceExecution.java`**

- 생성자: `super(mmmqListener.id())`로 ID 받기. `mmmqListener.listens()` 호출 제거.

**변경: `consumer/src/main/java/org/mmmq/consumer/handler/execution/method/MethodExecutionRegistration.java`**

- `HandlerExecutions` Bean을 주입받아 직접 `add(...)` 호출(기존엔 `FrontHandler.addHandlerExecution` 경유).
- `@MMMQListener` 어노테이션 처리 시 `annotation.id()`를 읽어 `MethodExecution(id, ...)`에 전달.

**변경: `consumer/src/main/java/org/mmmq/consumer/handler/execution/type/InterfaceExecutionRegistration.java`**

- `HandlerExecutions` Bean을 주입받아 직접 `add(...)` 호출.
- `MMMQListener<?>` 빈 처리 시 `mmmqListener.id()`를 사용하도록 변경. 기존 `listens()` 호출 제거.

## 변경 후 메시지 흐름

```
Producer.produce(message)
  → HTTP POST /mmmq/messages → Broker (헤더 없음)
  → FrontDispatcher.dispatch(message)
    → TopicQueueRegistry.get(topic) → TopicQueue.offer(message)
    → dispatcherContainer.dispatch(topicQueue)
    → 인덱싱된 매칭 Dispatcher들에게 drain 직접 호출
      → 각 디스패처가 자기 Subscription의 Offset 진행
      → Sender.send(message, handlerId, maxRetry)
        → Metadata 생성 + setHandlerId(handlerId) + toMap()
        → HTTP POST /mmmq/messages
           헤더: MMMQ-Handler-Id: {handlerId}  (Metadata가 결정)
           본문: message
        → Consumer.receiveMessage(headers, message)
          → new Metadata(headers).getHandlerId() → handlerId (null이면 NACK)
          → handlerExecutions.find(handlerId) → HE (null이면 NACK)
          → HE.execute(message)
              → 정상 → ACK
              → 예외 → NACK
      → ACK이면 offset commit, 다음 메시지로
      → NACK이면 디스패처 재시도 3회, 소진 시 메시지 드롭(현재 코드 그대로)
      → 통신 실패 시 지수 백오프 무한 재시도
```

## 변경 범위 외

- **DLQ**: 본 변경에서 일절 다루지 않음.
- **Producer → Broker 흐름**: 헤더 없음. 변경 없음.
- **Offset, CheckpointFile/CheckpointDirectory**: 변경 없음. 디스패처 수 증가에 따른 `.checkpoint` 파일 증가는 운영 부담으로 남기되 본 변경에서 최적화하지 않음.
- **NACK 재시도 소진 시 메시지 운명**: 현재 코드 그대로 드롭. DLQ 도입과 같이 별도 작업.
- **Producer의 retry 메커니즘**: 변경 없음.

## Breaking Changes

이 변경은 라이브러리 사용자 코드에 동시 변경을 요구한다. Deprecation 단계를 거치지 않고 한 번에 간다.

- `@MMMQListener("order.*")`, `@MMMQListener` 형태는 모두 컴파일 에러. `@MMMQListener(id = "...")`로 변경 필요.
- `MMMQListener<T>` 구현체의 `listens()` 메서드는 더 이상 호출되지 않는다. `id()` 구현이 필수.
- `new Dispatcher(name, host, List.of(pattern1, pattern2))` 형태는 더 이상 사용 불가. `new Dispatcher(host, handlerId, pattern)` 형태로 변경. `name` 인자는 사라지고 `handlerId`가 그 자리를 대신한다. 한 HE에 여러 토픽이 필요하면 pattern을 와일드카드로 묶거나 별도 HE/ID로 분리.
- Consumer 측에서 같은 토픽을 받던 N개 HE가 fan-out으로 동작하던 경우, Broker 측에 그 HE 수만큼 디스패처 Bean(서로 다른 `handlerId`)을 명시적으로 등록해야 한다.
- 기존 `.checkpoint` 파일이 `name` 기준 이름으로 저장되어 있었다면, `handlerId` 기준 파일명과 어긋난다. 신규 디스패처는 새 `handlerId.checkpoint` 파일로 Offset 0부터 시작한다(별도 마이그레이션은 두지 않음).

## 검증 시나리오

테스트는 별도 리팩터링에서 일괄 작성 예정. 본 §은 본 변경의 핵심 동작이 충족되는지 점검할 최소 시나리오만 둔다.

1. **정상 라우팅 + 동기 응답**: `handlerId` 헤더로 라우팅된 메시지가 Consumer 측 같은 ID HE에서 동기 실행되고, 결과(ACK/NACK)가 응답으로 돌아와 Broker가 Offset commit / 재시도 흐름으로 분기되는지 확인.
2. **격리**: 한 디스패처의 응답 지연·NACK·예외가 다른 `handlerId`의 디스패처 진행과 Offset 흐름에 영향을 주지 않는지 확인. 본 변경의 본질 목적(원자성·슬로우 컨슈머 해소).
