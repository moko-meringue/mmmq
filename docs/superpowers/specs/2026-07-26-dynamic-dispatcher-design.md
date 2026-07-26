# 런타임 Dispatcher 관리 API 설계

- 작성일: 2026-07-26
- 브랜치: `dynamic-dispatcher`
- 대상 모듈: `broker`, `core`

## 배경

지금 Dispatcher는 `dispatchers.json`으로만 관리한다. 브로커가 뜰 때 `DispatcherBeanRegistrar`(`ImportBeanDefinitionRegistrar`)가 파일을 읽어 각 항목을 스프링 빈으로 등록하고, `DispatcherContainer`가 `Collection<Dispatcher>`를 주입받아 `List.copyOf`로 불변 보관한다.

그래서 소비자를 새로 붙이거나, 대상 주소나 구독 패턴을 바꾸거나, 없애려면 파일을 고치고 브로커를 재기동해야 한다.

## 목표

애플리케이션이 도는 중에 HTTP API로 Dispatcher를 추가·수정·삭제한다. 파일 기반 관리는 그대로 두고, 런타임 변경이 파일에도 반영돼 재기동 후에도 남는다.

- 추가: `consumerId`·`host`·`pattern`을 본문으로 받아 등록
- 수정: `consumerId`는 경로 변수, 본문에는 새 `host`·`pattern`만
- 삭제: `consumerId`를 경로 변수로 받아 제거
- 조회: 현재 등록된 Dispatcher 목록

`consumerId`는 식별자라 수정 대상이 아니다. "1 consumerId = 1 Dispatcher" 규칙은 유지된다.

## 확정한 결정

| 항목 | 결정 |
|---|---|
| Dispatcher 소유권 | `DispatcherContainer`가 직접 소유. `DispatcherBeanRegistrar` 삭제 |
| 파일 host 포맷 | URL 문자열 하나로 통일 (API 요청 형태와 동일) |
| Dispatcher 생성 | `DispatcherFactory`가 `DispatcherDefinition`을 받아 생성 |
| 역방향 변환 | `DispatcherDefinition.from(Dispatcher)` |
| `Host`의 주소 표현 | `InetAddress` 즉시 해석을 그만두고 원본 문자열 보존 |
| 새 구독의 시작 오프셋 | 로그의 tail |
| 체크포인트 수명 | 구독이 끝나면 같이 지운다 (삭제·패턴 축소 모두) |
| 동기화·동시성 | 뮤테이션 단일 락 + 파일 원자 교체, 읽기는 무락 |

각 결정의 근거는 아래 해당 절에 적었다.

## 파일 포맷

```json
[
  {
    "consumerId": "order-created",
    "host": "http://consumer-host:8080",
    "pattern": "order.created"
  }
]
```

기존 `{ "protocol": "HTTP", "address": "...", "port": 8080 }` 중첩 구조를 대체한다. 마이그레이션은 제공하지 않는다(pre-1.0). 이 형태가 곧 POST 요청 본문이자 GET 응답 본문이라, 같은 개념을 두 가지 모양으로 유지할 일이 없다.

포트를 생략하면 스킴 기본값(`http` 80, `https` 443)을 쓴다.

## 컴포넌트

### 신규

**`DispatcherFactory`** (`org.mmmq.broker.dispatcher`, `@Component`)

`DispatcherDefinition`을 받아 `Dispatcher`를 만든다. 문자열 → `Host`·`ConsumerId`·`TopicPattern` 변환과 검증이 전부 여기 모인다. `TopicQueueFactory`와 같은 자리, 같은 모양이다.

```java
public Dispatcher create(DispatcherDefinition definition) {
    URI uri = URI.create(definition.host());
    if (uri.getScheme() == null || uri.getHost() == null) {
        throw new IllegalArgumentException("host must be an absolute URL, but was: " + definition.host());
    }
    WebProtocol protocol = WebProtocol.from(uri.getScheme());
    int port = uri.getPort();
    if (port == -1) {
        port = protocol.getDefaultPort();
    }
    return new Dispatcher(
            new Host(protocol, uri.getHost(), port),
            new ConsumerId(definition.consumerId()),
            new TopicPattern(definition.pattern())
    );
}
```

`URI.create("garbage")`는 예외를 던지지 않고 scheme이 `null`인 URI를 만들기 때문에 scheme·host를 명시적으로 확인해야 한다. 밑줄이 든 호스트명(`consumer_host`)도 `getHost()`가 `null`이라 여기서 걸린다.

**`DispatcherFile`** (`org.mmmq.broker.dispatcher`, `@Component`)

`dispatchers.json` 읽기와 원자적 쓰기를 맡는다. `PersistenceProperties`를 주입받아 경로를 정한다.

- `read()`: 파일이 없으면 `[]`로 만들고 빈 목록을 돌려준다(현행 `DispatcherBeanRegistrar` 동작을 옮긴 것).
- `write(definitions)`: 같은 디렉터리의 `dispatchers.json.tmp`에 전체를 쓰고 `ATOMIC_MOVE`로 교체한다.

`ObjectMapper`는 주입받지 않고 클래스 상수로 직접 만든다. broker는 라이브러리라, 호스트 애플리케이션의 `ObjectMapper` 커스터마이징에 파일 포맷이 휘둘리면 안 된다.

**`DispatcherRoute`** (record)

PUT 본문 전용. `record DispatcherRoute(String host, String pattern)`.

**`DispatcherController`** (`@RestController`)

아래 "HTTP API" 절 참고.

**`DuplicateConsumerIdException`**, **`DispatcherNotFoundException`**

`RuntimeException` 상속. 상태 코드 매핑을 정확히 하기 위한 것이다. `IllegalStateException` 하나로 409를 표현하면 다른 원인의 `IllegalStateException`까지 409가 된다.

### 수정

**`DispatcherDefinition`**

```java
public record DispatcherDefinition(
        String consumerId,
        String host,
        String pattern
) {

    public static DispatcherDefinition from(Dispatcher dispatcher) {
        return new DispatcherDefinition(
                dispatcher.consumerId().value(),
                dispatcher.host().toUri(),
                dispatcher.pattern().value()
        );
    }
}
```

중첩 `HostDefinition` 레코드와 `toHost()`는 삭제한다. `Host`가 원본 주소 문자열을 보존하게 되면서 `Dispatcher` → 정의 복원이 무손실이 된다. 덕분에 컨테이너가 런타임 객체와 정의를 두 벌로 들 필요가 없다.

**`Dispatcher`**

- 생성자 시그니처는 그대로 `(Host, ConsumerId, TopicPattern)`. 와이어 포맷을 모른다.
- `host()`, `pattern()` 접근자 추가 (`consumerId()`는 이미 있음).
- `destroy()`에서 `@PreDestroy` 제거. 더 이상 빈이 아니므로 컨테이너가 생명주기를 책임진다.

**`DispatcherContainer`**

```java
private final DispatcherFactory factory;
private final DispatcherFile file;
private final Map<ConsumerId, Dispatcher> dispatchers = new LinkedHashMap<>();
private final Map<TopicQueue, List<Dispatcher>> subscriptions = new ConcurrentHashMap<>();
private final ReentrantLock mutationLock = new ReentrantLock();
```

- 생성자에서 `file.read()`로 Dispatcher를 만들어 채운다. 중복 `consumerId`, 미지원 스킴, 깨진 JSON은 여기서 터져 컨텍스트 기동을 막는다(현행 fail-fast 유지).
- `dispatchers`는 `LinkedHashMap`. 락으로 보호되고, 파일에 쓸 때 순서가 안정적이다.
- `subscriptions`는 `ConcurrentHashMap`. 값은 불변 리스트를 통째로 교체한다.
- `@PreDestroy destroy()`가 모든 Dispatcher의 `destroy()`를 호출한다. 워커만 종료하고 체크포인트는 건드리지 않는다 — 애플리케이션 종료는 구독 해제가 아니다.

공개 메서드: `register(TopicQueue)`, `getSubscribers(TopicQueue)`, `definitions()`, `add(DispatcherDefinition)`, `modify(ConsumerId, DispatcherRoute)`, `remove(ConsumerId)`.

내부에 매칭 로직을 하나 두고 세 뮤테이션이 공유한다.

```java
private List<Dispatcher> match(TopicQueue topicQueue) {
    List<Dispatcher> matched = dispatchers.values().stream()
            .filter(dispatcher -> dispatcher.canDispatch(topicQueue.getTopic()))
            .toList();
    matched.forEach(dispatcher -> dispatcher.subscribe(topicQueue));
    return matched;
}

private void rematchAll() {
    subscriptions.replaceAll((topicQueue, previous) -> {
        List<Dispatcher> matched = match(topicQueue);
        Set<ConsumerId> retained = matched.stream()
                .map(Dispatcher::consumerId)
                .collect(Collectors.toSet());
        previous.stream()
                .map(Dispatcher::consumerId)
                .filter(consumerId -> !retained.contains(consumerId))
                .forEach(consumerId -> topicQueue.unsubscribe(consumerId.value()));
        return matched;
    });
}
```

`Dispatcher.subscribe`가 `computeIfAbsent`라 이미 구독한 큐를 다시 매칭해도 아무 일도 일어나지 않는다.

구독이 끊긴 짝을 찾는 비교는 **`ConsumerId` 기준**이어야 한다. `Dispatcher`는 `equals`가 없어 객체 동일성으로 비교되는데, 수정은 새 인스턴스로 교체하는 방식이라 객체로 비교하면 호스트만 바꾼 수정에서도 모든 토픽의 체크포인트가 지워진다.

`retained`와 지워지는 쪽은 서로 겹치지 않으므로 `match`가 먼저 새 구독을 만들고 그 뒤에 잃은 쪽을 정리해도 순서 문제가 없다. `remove`는 사라진 Dispatcher가 `matched`에 안 잡히므로 이 로직만으로 정리가 끝난다.

**`TopicQueue`**

새 구독의 시작 오프셋을 tail로 잡는다.

```java
public Offset subscribe(String name) {
    CheckpointFile checkpointFile = checkpointDirectory.get(name);
    if (checkpointFile == null) {
        checkpointFile = checkpointDirectory.register(name);
        checkpointFile.write(segmentFileChain.tailOffset());
    }
    return new Offset(checkpointFile.read());
}
```

`register`가 `computeIfAbsent`라 새로 만든 건지 알 수 없어서, `get`이 `null`인지로 신규를 판별한다. 경쟁은 없다 — `subscribe`에 닿는 경로는 `register`·`add`·`modify` 셋뿐이고 전부 뮤테이션 락 안이다.

구독을 끊는 쪽도 추가한다.

```java
public void unsubscribe(String name) {
    try {
        checkpointDirectory.deregister(name);
    } catch (StorageException exception) {
        log.error("Failed to remove checkpoint '{}' on topic {}", name, topic, exception);
    }
}
```

예외를 삼키고 로그만 남긴다. 이 호출은 `rematchAll`의 `replaceAll` 안에서 여러 토픽을 돌며 일어나는데, 한 토픽의 파일 삭제 실패가 예외로 올라가면 `subscriptions`가 반쯤 갱신된 채 남는다. 지우다 실패한 체크포인트는 아무도 읽지 않는 파일로 남을 뿐이다.

**`SegmentFileChain`**

```java
public long tailOffset() {
    SegmentFile tailSegmentFile = segmentsByStartOffset.lastEntry().getValue();
    return tailSegmentFile.startOffset() + tailSegmentFile.count();
}
```

`append`가 로테이션할 때 쓰던 `nextOffset` 계산과 같은 식이므로 그쪽도 이 메서드를 쓴다.

**`CheckpointDirectory`**

`deregister(name)`를 추가한다. 맵에서 빼고, 빠진 게 있으면 `CheckpointFile.delete()`를 부른다.

```java
public void deregister(String name) {
    CheckpointFile checkpointFile = checkpoints.remove(name);
    if (checkpointFile != null) {
        checkpointFile.delete();
    }
}
```

맵에서 먼저 빼기 때문에 `close()`가 나중에 돌면서 이미 닫힌 파일을 다시 닫는 일은 없다.

**`CheckpointFile`**

`delete()`를 추가한다. 경로를 이미 필드로 들고 있으니 자기 파일을 스스로 지운다.

```java
public void delete() {
    close();
    try {
        Files.deleteIfExists(file);
    } catch (IOException exception) {
        throw new StorageException("Failed to delete offset checkpoint: " + file, exception);
    }
}
```

`open`의 `size == 0 → write(0L)`은 유지한다. 그건 파일을 유효한 상태로 만드는 일이고, tail은 `TopicQueue.subscribe`가 그 위에 덮어쓴다.

`// MOKO: 새 Checkpoint 생성 시 처음부터 시작할지, 최신부터 시작할지 옵션 고려.` 주석은 제거한다. 이 스펙이 "최신(tail)"으로 결론을 냈다.

**`Host`** (core)

```java
final WebProtocol protocol;
final String address;
final int port;
```

`InetAddress.getByName()` 즉시 해석을 없애고 원본 주소 문자열을 보존한다. `toUri()`가 그 문자열을 그대로 쓰므로 이름 해석은 `Sender`의 `RestClient`가 요청 시점에 한다.

이유는 두 가지다.

1. 아직 뜨지 않아 DNS에 없는 소비자를 미리 등록할 수 없으면, "운영 중에 소비자를 새로 붙인다"는 이 기능의 핵심 용례가 막힌다.
2. 등록 후 소비자 IP가 바뀌어도 브로커가 재기동 전까지 옛 IP로 계속 보내던 문제도 같이 사라진다.

`equals`/`hashCode`는 지금 `address`만 비교해서 포트가 달라도 같다고 나온다. 비교 대상 필드를 손대는 김에 `protocol`·`address`·`port` 전부 포함하도록 고친다.

주소 형식 검증은 `DispatcherFactory`의 URL 파싱이 맡는다. 다만 `Host`는 core의 공개 타입이라 라이브러리 사용자가 직접 생성하므로, 생성자에 빈 주소와 포트 범위(1~65535) 확인은 인라인으로 남긴다.

**`WebProtocol`** (core)

각 상수에 기본 포트를 붙이고 `getDefaultPort()`를 노출한다. 기본 포트를 아는 주체는 프로토콜이다.

**`BrokerConfiguration`**

`@Import(DispatcherBeanRegistrar.class)` 제거.

**`PersistenceProperties`**

`bind(Environment)` 정적 메서드 제거. `ImportBeanDefinitionRegistrar`가 `@ConfigurationProperties` 바인딩보다 먼저 도는 탓에 필요했던 우회였다. 이제 `DispatcherFile`이 빈으로 주입받는다.

### 삭제

- `DispatcherBeanRegistrar`
- `DispatcherDefinition.HostDefinition`
- `DispatcherBeanRegistrarTest`, `HostDefinitionTest`

`DispatcherContainer`가 더 이상 `Collection<Dispatcher>`를 주입받지 않으므로, 라이브러리 사용자가 직접 정의한 `Dispatcher` 빈은 무시된다. 파일이 유일한 출처다.

## HTTP API

```
GET    /mmmq/dispatchers               200
POST   /mmmq/dispatchers               201 / 400 / 409
PUT    /mmmq/dispatchers/{consumerId}  200 / 400 / 404
DELETE /mmmq/dispatchers/{consumerId}  204 / 404
```

메서드 이름은 기존 `Broker.postMessage` 선례를 따라 `getDispatchers`·`postDispatcher`·`putDispatcher`·`deleteDispatcher`로 짓는다.

| 상황 | 코드 |
|---|---|
| 잘못된 URL, 알 수 없는 스킴, `consumerId` 정규식 위반, JSON 바인딩 실패 | 400 |
| 중복 `consumerId` | 409 |
| 없는 `consumerId` | 404 |
| 파일 쓰기 실패 | 500 |

응답 본문은 성공 시 `DispatcherDefinition`(DELETE는 본문 없음), 실패 시 예외 메시지 문자열이다. 전용 에러 레코드는 만들지 않는다.

전역 `@RestControllerAdvice`는 쓰지 않는다. broker는 라이브러리라 호스트 애플리케이션의 다른 컨트롤러 예외까지 가로챈다. `DispatcherController` 안의 `@ExceptionHandler`로 가둔다.

## 데이터 흐름

세 뮤테이션 모두 **검증 → 파일 → 메모리** 순서다. 검증에서 터지면 파일도 메모리도 안 건드리고, 파일이 넘어간 뒤 죽어도 재기동하면 파일 상태로 수렴한다.

**추가 (POST)**

1. 락 획득
2. `factory.create(definition)` — 여기서 검증. 실패 시 400
3. 이미 있는 `consumerId`면 `DuplicateConsumerIdException`. 409
4. 현재 정의 목록에 새 정의를 더해 `file.write`
5. `dispatchers.put`
6. `rematchAll()` — 기존 큐를 훑어 매칭되면 구독하고 구독 리스트를 교체
7. 락 해제, 201

새 구독은 tail부터라 밀린 메시지가 없다. 그래서 등록 직후 `dispatch`를 킥할 필요가 없고, 다음 메시지가 오면 `FrontDispatcher`가 평소대로 깨운다.

**수정 (PUT)**

1. 락 획득
2. 없으면 `DispatcherNotFoundException`. 404
3. `factory.create(...)`로 새 Dispatcher 생성 — 검증. 실패 시 400
4. 해당 항목을 바꾼 정의 목록으로 `file.write`
5. 옛 Dispatcher `destroy()`
6. `dispatchers.put`으로 교체
7. `rematchAll()`
8. 락 해제, 200

호스트만 바꾸는 수정은 오프셋이 저절로 이어진다. 체크포인트 파일이 `<consumerId>.checkpoint`라 Dispatcher 객체를 갈아끼워도 새 객체가 같은 파일을 읽는다.

패턴을 넓히면 새로 매칭된 토픽을 tail부터 구독한다. 좁히면 빠진 토픽으로 더 이상 전달하지 않고 그 토픽의 체크포인트도 지운다. 나중에 패턴을 다시 넓히면 예전 진도가 아니라 tail부터 시작한다 — 구독하지 않은 구간의 메시지는 받지 않는다는 뜻이고, "새 구독은 tail부터"와 같은 원칙이다.

**삭제 (DELETE)**

1. 락 획득
2. 없으면 404
3. 해당 항목을 뺀 정의 목록으로 `file.write`
4. 옛 Dispatcher `destroy()`
5. `dispatchers.remove`
6. `rematchAll()` — 구독이 끊긴 토픽마다 체크포인트를 지운다
7. 락 해제, 204

수정과 삭제 모두 옛 Dispatcher의 `destroy()`를 `rematchAll()` **앞**에 둔다. 인터럽트를 먼저 던져야 낙오한 워커가 체크포인트 삭제 뒤에 `commit`을 부를 확률이 줄어든다. 그 사이 이미 죽은 워커에 `dispatch`가 들어갈 수는 있지만 `WorkerPool`의 `DiscardPolicy`가 조용히 버린다.

낙오한 워커가 삭제된 체크포인트에 `commit`을 부르면 `checkpointDirectory.get`이 `null`이라 `IllegalStateException`이 나고 드레인 루프가 로그를 남기며 끝난다. 어차피 멈춰야 할 루프라 결과는 맞고, `commit`은 `register`가 아니라 `get`을 쓰므로 지워진 파일이 되살아나지도 않는다.

**조회 (GET)** — 락을 잡고 `dispatchers.values()`를 `DispatcherDefinition.from`으로 옮겨 담아 돌려준다. 읽기지만 락을 잡는 이유는 뮤테이션 중간 상태가 아닌 일관된 스냅샷을 돌려주기 위해서다. 호출 빈도가 낮아 핫패스와 무관하다.

**새 토픽 생성 (`register`)** — 현행 로직 그대로, 락만 추가.

## 파일 동기화

같은 디렉터리에 `dispatchers.json.tmp`로 전체를 쓰고 `ATOMIC_MOVE`로 교체한다. 같은 파일시스템이라 이동이 원자적이고, 반쯤 쓰인 JSON이 최종 경로에 보일 수 없다.

증분 append 대신 전체 재작성을 택했다. Dispatcher는 수십 개 규모고 뮤테이션은 드물어서 증분이 벌 수 있는 게 없다.

`fsync`는 하지 않는다. 프로세스가 죽는 경우는 커버되고, OS 크래시나 전원 손실까지는 다루지 않는다. 필요해지면 디렉터리 fsync를 추가하면 된다.

파일 쓰기가 실패하면 예외가 올라가고 메모리는 손대지 않은 상태로 남는다(500). 반대로 파일이 넘어간 뒤 6~7단계에서 터지면 메모리가 부분 갱신된 채 500이 나가는데, 재기동하면 파일 기준으로 수렴한다.

## 동시성

- 뮤테이션 락 하나가 `register`·`add`·`modify`·`remove`·`definitions`를 감싼다. 뮤테이션은 초당 몇 번이 아니라 하루 몇 번짜리라 경합이 없다.
- 핫패스인 `getSubscribers`는 락을 잡지 않는다. `ConcurrentHashMap` 읽기 그대로고 `FrontDispatcher`는 변경이 없다.
- 데드락은 생기지 않는다. `register`는 `TopicQueueContainer.queues.computeIfAbsent` 안에서 불려서 `CHM bin lock → mutationLock` 순서인데, 뮤테이션 경로는 `mutationLock`을 잡은 채 `TopicQueueContainer`를 건드리지 않아 역순이 만들어질 수 없다.

**교체·삭제 시 옛 워커의 종료를 기다리지 않는다.** `Sender`의 `RestClient`에 타임아웃 설정이 없어서, 소비자가 응답하지 않으면 `awaitTermination`이 API 요청을 무한정 붙잡는다.

대신 옛 워커가 인터럽트를 확인하기 전에 메시지를 한 건 더 보내고 커밋할 수 있다. 체크포인트가 잠깐 뒤로 되감기면서 몇 건이 중복 전송될 수 있다는 뜻이다. MMMQ는 NACK 재시도 때문에 이미 at-least-once라 보장이 약해지지 않고, 메시지 손실은 없다.

## 테스트

broker는 `@SpringBootConfiguration`이 없어서 `@WebMvcTest`를 쓸 수 없다. 컨트롤러는 standalone MockMvc로 검증한다.

**`DispatcherFileTest`**
- 파일이 없으면 `[]`로 만들고 빈 목록을 돌려준다
- 쓰고 다시 읽으면 같은 목록이 나온다
- 쓰기 후 `.tmp`가 남지 않는다

**`DispatcherFactoryTest`**
- URL을 `Host`로 파싱한다
- 포트를 생략하면 스킴 기본값을 쓴다
- scheme이 없는 문자열, 미지원 스킴, 정규식에 안 맞는 `consumerId`에서 예외

**`DispatcherContainerTest`**
- 추가하면 매칭되는 기존 큐에 붙고 tail부터 시작한다
- 추가·수정·삭제가 파일에 반영된다
- 호스트만 바꾸면 체크포인트가 승계된다. 인스턴스가 교체돼도 **체크포인트 파일이 지워지지 않는다**(`ConsumerId` 기준 비교가 깨지면 여기서 잡힌다)
- 패턴을 넓히면 새 토픽을 tail부터 구독한다
- 패턴을 좁히면 전달이 끊기고 그 토픽의 체크포인트도 지워진다
- 삭제하면 `getSubscribers`에서 빠지고 워커가 종료되며, 구독하던 모든 토픽의 체크포인트가 지워진다
- 중복 `consumerId`는 `DuplicateConsumerIdException`, 없는 id는 `DispatcherNotFoundException`
- 검증에 실패하면 파일이 바뀌지 않는다
- 부트스트랩: 파일 없음 / 중복 id / 미지원 스킴 / 깨진 JSON → 생성자에서 실패 (기존 `DispatcherBeanRegistrarTest`에서 이관)
- `register`와 `add`를 여러 스레드에서 동시에 호출해도 최종 상태가 일관된다 (`CountDownLatch`)

**`DispatcherControllerTest`**
- 네 엔드포인트의 상태 코드 매핑

**`CheckpointFileTest`·`CheckpointDirectoryTest` 보강**
- `delete()`가 핸들을 닫고 파일을 지운다
- `deregister(name)` 후 `get(name)`이 `null`이고, 뒤이은 `close()`가 터지지 않는다
- 없는 이름으로 `deregister`해도 아무 일도 없다

**기존 테스트 영향**
- `HostTest`: "잘못된 호스트명이면 예외" 케이스는 성립하지 않으므로 제거. 빈 주소·포트 범위 케이스로 대체
- `SenderTest`·`GatewayTest`: 양쪽 다 `host.toUri()`를 쓰고 있어 그대로 통과
- `DispatcherTest`·`FrontDispatcherTest`: `Dispatcher` 생성자가 그대로라 영향 없음

## 문서

`CLAUDE.md`의 "Broker Dispatcher Registration" 절을 새 파일 포맷과 런타임 API 기준으로 갱신한다.

## 다루지 않는 것

- API 인증·인가. 교육용 라이브러리이고 프롬프트 범위 밖이다.
- 새 구독 시작 지점을 요청마다 고르는 옵션(`from: earliest | latest`). tail 고정으로 간다.
- `dispatchers.json` 포맷 마이그레이션.
- OS 크래시·전원 손실 수준의 내구성(fsync).
- `Sender`의 HTTP 타임아웃 설정. 별도 과제다.
