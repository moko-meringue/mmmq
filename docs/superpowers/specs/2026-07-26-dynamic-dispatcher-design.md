# 런타임 Dispatcher 관리 API 설계

- 작성일: 2026-07-26
- 브랜치: `dynamic-dispatcher`
- 대상 모듈: `broker`, `core`

## 배경

지금 Dispatcher는 `dispatchers.json`으로만 관리한다. 브로커가 뜰 때 `DispatcherBeanRegistrar`(`ImportBeanDefinitionRegistrar`)가 파일을 읽어 각 항목을 스프링 빈으로 등록하고, `DispatcherContainer`가 `Collection<Dispatcher>`를 주입받아 `List.copyOf`로 불변 보관한다.

그래서 소비자를 새로 붙이거나, 대상 주소나 구독 패턴을 바꾸거나, 없애려면 파일을 고치고 브로커를 재기동해야 한다.

## 목표

애플리케이션이 도는 중에 HTTP API로 Dispatcher를 추가·수정·삭제·조회한다. 파일 기반 관리는 그대로 두고, 런타임 변경이 파일에도 반영돼 재기동 후에도 남는다.

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
| 엔드포인트 범위 | 추가·수정·삭제에 목록 조회(`GET /mmmq/dispatchers`)를 포함한다 |
| 동기화·동시성 | 뮤테이션 단일 락 + 파일 원자 교체, 읽기는 락을 잡지 않는다 |

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

포트는 생략할 수 없다. 소비자는 대개 8080 같은 비표준 포트에 있어서, 스킴 기본값으로 대체하면 포트를 빼먹은 등록이 조용히 80으로 향한다. 빠뜨렸으면 등록 시점에 거절하는 쪽이 낫고, `Host`가 포트를 필수로 들고 있어 저장·응답 형태도 입력과 같은 모양으로 유지된다.

host는 `scheme://address:port` 세 성분만 받는다. 경로·query·userInfo·fragment가 붙으면 거절한다. `Host`가 그 세 성분만 들어 왕복에서 나머지가 소실되는데, 경로는 무해하게 사라지지 않기 때문이다 — `RestClient`의 baseUrl 경로에 `/mmmq/messages`가 덧붙으므로(spring-web 6.1.1 실측), 경로를 보존하면 라우팅이 달라지고 버리면 사용자가 지정한 경로를 무시하고 다른 엔드포인트로 보낸다. userInfo는 인증 정보가 조용히 사라지는 같은 부류다. 포트를 필수로 요구한 것과 같은 논리다.

## 컴포넌트

### 신규

**`DispatcherFactory`** (`org.mmmq.broker.dispatcher`)

`DispatcherDefinition`을 받아 `Dispatcher`를 만든다. 문자열 → `Host`·`ConsumerId`·`TopicPattern` 변환과 검증이 전부 여기 모인다.

상태가 없어 `create`는 정적 메서드다. 빈으로 만들면 컨테이너가 필드와 생성자 인자를 하나 더 들어야 하는데, 호출자는 `DispatcherContainer` 하나뿐이고 대체 구현이나 목이 필요한 곳이 없다. `SegmentFileChain.open`·`Sender.from`처럼 이 저장소에 이미 있는 정적 팩토리와 같은 모양이다. 클래스와 메서드 모두 package-private다 — 호출자가 전부 같은 패키지 안이고, Dispatcher를 만드는 경로가 파일 하나로 모인다는 결정과 노출 범위가 맞는다.

`create(definition)`은 host 문자열을 `URI.create`로 파싱해 스킴·호스트·포트를 확인한 뒤 `Host`·`ConsumerId`·`TopicPattern`을 만든다.

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

레코드 컴포넌트는 `consumerId`·`host`·`pattern` 세 문자열이고, `from(Dispatcher)`가 `host().toUri()`로 host를 되돌린다. 중첩 `HostDefinition` 레코드와 `toHost()`는 삭제한다. `Host`가 원본 주소 문자열을 보존하게 되면서 `Dispatcher` → 정의 복원이 무손실이 된다. 덕분에 컨테이너가 런타임 객체와 정의를 두 벌로 들 필요가 없다.

**`Dispatcher`**

- 생성자 시그니처는 그대로 `(Host, ConsumerId, TopicPattern)`. 와이어 포맷을 모른다.
- `host()`, `pattern()` 접근자 추가 (`consumerId()`는 이미 있음).
- `destroy()`에서 `@PreDestroy` 제거. 더 이상 빈이 아니므로 컨테이너가 생명주기를 책임진다.

**`DispatcherContainer`**

`DispatcherFile` 하나를 주입받고, `Map<ConsumerId, Dispatcher> dispatchers`·`Map<TopicQueue, List<Dispatcher>> subscriptions`·`ReentrantLock mutationLock`을 필드로 든다.

- 생성자에서 `file.read()`로 Dispatcher를 만들어 채운다. 중복 `consumerId`, 미지원 스킴, 깨진 JSON은 여기서 터져 컨텍스트 기동을 막는다(현행 fail-fast 유지).
- `dispatchers`는 `LinkedHashMap`. 락으로 보호되고, 파일에 쓸 때 순서가 안정적이다.
- `subscriptions`는 `ConcurrentHashMap`. 값은 불변 리스트를 통째로 교체한다.
- `@PreDestroy destroy()`가 모든 Dispatcher의 `destroy()`를 호출한다. 워커만 종료하고 체크포인트는 건드리지 않는다 — 애플리케이션 종료는 구독 해제가 아니다.

공개 메서드: `register(TopicQueue)`, `getSubscribers(TopicQueue)`, `definitions()`, `add(DispatcherDefinition)`, `modify(ConsumerId, DispatcherRoute)`, `remove(ConsumerId)`.

내부 `match(TopicQueue)`가 패턴이 맞는 Dispatcher를 골라 그 큐를 구독시키고(현행 `register`의 본문 그대로다), `rematchAll()`이 `subscriptions`의 모든 큐에 `match`를 다시 돌려 구독 리스트를 갈아치우면서 이번에 빠진 짝은 `TopicQueue.unsubscribe`로 끊는다. 세 뮤테이션이 이 둘을 공유한다.

`Dispatcher.subscribe`가 `computeIfAbsent`라 이미 구독한 큐를 다시 매칭해도 아무 일도 일어나지 않는다.

구독이 끊긴 짝을 찾는 비교는 **`ConsumerId` 기준**이어야 한다. `Dispatcher`는 `equals`가 없어 객체 동일성으로 비교되는데, 수정은 새 인스턴스로 교체하는 방식이라 객체로 비교하면 호스트만 바꾼 수정에서도 모든 토픽의 체크포인트가 지워진다.

남는 쪽과 지워지는 쪽은 서로 겹치지 않으므로 `match`가 먼저 새 구독을 만들고 그 뒤에 잃은 쪽을 정리해도 순서 문제가 없다. `remove`는 사라진 Dispatcher가 매칭 결과에 안 잡히므로 이 로직만으로 정리가 끝난다.

**`TopicQueue`**

`subscribe(name)`은 체크포인트가 없을 때만 `segmentFileChain.tailOffset()`을 써서 새로 만든다. 기존 체크포인트는 손대지 않으므로 재기동 동작은 그대로다.

`register`가 `computeIfAbsent`라 새로 만든 건지 알 수 없어서, `get`이 `null`인지로 신규를 판별한다. 경쟁은 없다 — `subscribe`에 닿는 경로는 `register`·`add`·`modify` 셋뿐이고 전부 뮤테이션 락 안이다. `CheckpointDirectory.open`이 디스크의 체크포인트 파일을 전부 맵에 올려두므로, 재기동 후 첫 `subscribe`에서도 `get`이 `null`이 아니고 tail로 덮어쓰지 않는다.

구독을 끊는 `unsubscribe(name)`도 추가한다. `checkpointDirectory.deregister(name)`를 부르고 `StorageException`은 삼켜 로그만 남긴다. 이 호출은 `rematchAll`이 여러 토픽을 돌며 일어나는데, 한 토픽의 파일 삭제 실패가 예외로 올라가면 `subscriptions`가 반쯤 갱신된 채 남는다. 지우다 실패한 체크포인트는 아무도 읽지 않는 파일로 남을 뿐이다.

**`SegmentFileChain`**

`tailOffset()`을 추가한다. tail 세그먼트의 `startOffset() + count()`이고, `append`가 로테이션할 때 쓰던 `nextOffset` 계산과 같은 식이므로 그쪽도 이 메서드를 쓴다.

**`CheckpointDirectory`**

`deregister(name)`를 추가한다. 맵에서 빼고, 빠진 게 있으면 `CheckpointFile.delete()`를 부른다. 맵에서 먼저 빼기 때문에 `close()`가 나중에 돌면서 이미 닫힌 파일을 다시 닫는 일은 없다.

**`CheckpointFile`**

`delete()`를 추가한다. 경로를 이미 필드로 들고 있으니 핸들을 닫고 자기 파일을 스스로 지운다.

`open`의 `size == 0 → write(0L)`은 유지한다. 그건 파일을 유효한 상태로 만드는 일이고, tail은 `TopicQueue.subscribe`가 그 위에 덮어쓴다.

`// MOKO: 새 Checkpoint 생성 시 처음부터 시작할지, 최신부터 시작할지 옵션 고려.` 주석은 제거한다. 이 스펙이 "최신(tail)"으로 결론을 냈다.

**`Host`** (core)

`address` 필드 타입을 `InetAddress`에서 `String`으로 바꿔 `InetAddress.getByName()` 즉시 해석을 없애고 원본 주소 문자열을 보존한다. `toUri()`가 그 문자열을 그대로 쓰므로 이름 해석은 `Sender`의 `RestClient`가 요청 시점에 한다.

이유는 두 가지다.

1. 아직 뜨지 않아 DNS에 없는 소비자를 미리 등록할 수 없으면, "운영 중에 소비자를 새로 붙인다"는 이 기능의 핵심 용례가 막힌다.
2. 등록 후 소비자 IP가 바뀌어도 브로커가 재기동 전까지 옛 IP로 계속 보내던 문제도 같이 사라진다.

`equals`/`hashCode`는 고치지 않고 지운다. 지금 구현은 `address`만 비교해서 포트가 달라도 같다고 나오는데, 프로덕션에서 `Host`를 비교하는 코드가 없다 — `Sender.from`·`Gateway`는 `toUri()`만 쓰고, `Dispatcher`는 필드로만 들고, 맵·셋의 키는 `TopicQueue`와 `ConsumerId`다. 새로 들어오는 컨테이너·`rematchAll`도 `ConsumerId` 기준으로 비교한다. 비교하는 곳은 `GatewayTest`의 `assertThat(gateway.host).isEqualTo(host)` 한 줄뿐이고 같은 인스턴스를 넘기므로 제거 후 동일성 비교로 통과한다. 틀린 비교를 남기는 것보다 없는 편이 안전하고, 필요해지는 날 전 필드로 넣으면 된다.

필드는 `private final`로 바꾼다. 클래스 밖에서 읽는 코드가 없다.

주소 형식 검증은 `DispatcherFactory`의 URL 파싱이 맡는다. 다만 `Host`는 core의 공개 타입이라 라이브러리 사용자가 직접 생성하므로, 생성자에 빈 주소와 포트 범위(1~65535) 확인은 인라인으로 남긴다.

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
DELETE /mmmq/dispatchers/{consumerId}  204 / 400 / 404
```

메서드 이름은 기존 `Broker.postMessage` 선례를 따라 `getDispatchers`·`postDispatcher`·`putDispatcher`·`deleteDispatcher`로 짓는다.

| 상황 | 코드 |
|---|---|
| 잘못된 URL, 알 수 없는 스킴, 포트 누락, `consumerId` 정규식 위반, JSON 바인딩 실패 | 400 |
| 중복 `consumerId` | 409 |
| 없는 `consumerId` | 404 |
| 파일 쓰기 실패 | 500 |

응답 본문은 성공 시 `DispatcherDefinition`(DELETE는 본문 없음), 실패 시 예외 메시지 문자열이다. 전용 에러 레코드는 만들지 않는다.

전역 `@RestControllerAdvice`는 쓰지 않는다. broker는 라이브러리라 호스트 애플리케이션의 다른 컨트롤러 예외까지 가로챈다. `DispatcherController` 안의 `@ExceptionHandler`로 가둔다.

예외 클래스에 `@ResponseStatus`를 붙이는 방식도 쓰지 않는다. `ResponseStatusExceptionResolver`는 `reason`이 비어 있어도 `response.sendError(statusCode)`를 부르기 때문에(spring-webmvc 6.1.1 확인), 컨테이너의 에러 페이지 디스패치를 타고 응답 본문이 호스트 애플리케이션의 `/error` 처리로 넘어간다. 상태 코드는 맞게 나가지만 이 API의 실패 응답 모양을 남의 설정이 정하게 되고, `IllegalArgumentException`(JDK 클래스라 어노테이션을 못 붙인다)만 브로커가 만든 본문을 돌려주는 비대칭도 생긴다.

## 데이터 흐름

세 뮤테이션 모두 **검증 → 파일 → 메모리** 순서다. 검증에서 터지면 파일도 메모리도 안 건드리고, 파일이 넘어간 뒤 죽어도 재기동하면 파일 상태로 수렴한다.

검증 안에서의 순서는 추가와 수정·삭제가 반대다. 추가는 `DispatcherFactory.create`가 중복 확인보다 앞서고(형식과 중복이 함께 어긋난 요청은 400), 수정·삭제는 존재 확인이 형식 검증보다 앞선다(없는 `consumerId`에 잘못된 host를 보내면 404).

**추가 (POST)**

새 구독은 tail부터라 밀린 메시지가 없다. 그래서 등록 직후 `dispatch`를 킥할 필요가 없고, 다음 메시지가 오면 `FrontDispatcher`가 평소대로 깨운다.

**수정 (PUT)**

호스트만 바꾸는 수정은 오프셋이 저절로 이어진다. 체크포인트 파일이 `<consumerId>.checkpoint`라 Dispatcher 객체를 갈아끼워도 새 객체가 같은 파일을 읽는다.

패턴을 넓히면 새로 매칭된 토픽을 tail부터 구독한다. 좁히면 빠진 토픽으로 더 이상 전달하지 않고 그 토픽의 체크포인트도 지운다. 나중에 패턴을 다시 넓히면 예전 진도가 아니라 tail부터 시작한다 — 구독하지 않은 구간의 메시지는 받지 않는다는 뜻이고, "새 구독은 tail부터"와 같은 원칙이다.

**삭제 (DELETE)**

수정과 삭제 모두 옛 Dispatcher의 `destroy()`를 `rematchAll()` **앞**에 둔다. 인터럽트를 먼저 던져야 낙오한 워커가 체크포인트 삭제 뒤에 `commit`을 부를 확률이 줄어든다.

버려진 Dispatcher에 늦은 `dispatch`가 들어오는 창이 있다 — 메시지 스레드가 `getSubscribers(queue)`의 리스트를 잡은 뒤 `rematchAll`이 그 리스트를 교체하는 경우다. `WorkerPool`의 `DiscardPolicy`는 이걸 막지 못한다. `shutdownAll()`이 `pool.clear()`까지 하므로 뒤이은 `submit`의 `computeIfAbsent`가 **인터럽트되지 않은 새 executor**를 만들고, 버려진 Dispatcher의 `subscriptions` 맵에 큐가 그대로 남아 있어 기존 가드도 통과한다. 그러면 `drain`의 `while (true)`가 로그 끝까지 돌아 삭제된 소비자나 옛 host로 꼬리 전체가 간다.

그래서 `Dispatcher`에 `volatile boolean destroyed`를 두고 `destroy()`가 그것을 먼저 세운 뒤 `dispatch`의 가드에서 확인한다. `dispatch`가 `submit`의 유일한 경로라 워커 자체가 생기지 않고, 그래서 `drain` 루프에는 체크가 필요 없다.

낙오한 워커가 삭제된 체크포인트에 `commit`을 부르면 `checkpointDirectory.get`이 `null`이라 `IllegalStateException`이 나고 `drain` 루프가 로그를 남기며 끝난다. 어차피 멈춰야 할 루프라 결과는 맞고, `commit`은 `register`가 아니라 `get`을 쓰므로 지워진 파일이 되살아나지도 않는다.

**조회 (GET)** — 락을 잡고 `dispatchers.values()`를 `DispatcherDefinition.from`으로 옮겨 담아 돌려준다. 읽기지만 락을 잡는 이유는 뮤테이션 중간 상태가 아닌 일관된 스냅샷을 돌려주기 위해서다. 호출 빈도가 낮아 핫패스와 무관하다.

**새 토픽 생성 (`register`)** — 현행 로직 그대로, 락만 추가.

## 파일 동기화

같은 디렉터리에 `dispatchers.json.tmp`로 전체를 쓰고 `ATOMIC_MOVE`로 교체한다. 같은 파일시스템이라 이동이 원자적이고, 반쯤 쓰인 JSON이 최종 경로에 보일 수 없다.

증분 append 대신 전체 재작성을 택했다. Dispatcher는 수십 개 규모고 뮤테이션은 드물어서 증분이 벌 수 있는 게 없다.

`fsync`는 하지 않는다. 프로세스가 죽는 경우는 커버되고, OS 크래시나 전원 손실까지는 다루지 않는다. 필요해지면 디렉터리 fsync를 추가하면 된다.

파일 쓰기가 실패하면 예외가 올라가고 메모리는 손대지 않은 상태로 남는다(500). 반대로 파일이 넘어간 뒤 메모리를 갱신하다 터지면 메모리가 부분 갱신된 채 500이 나가는데, 재기동하면 파일 기준으로 수렴한다.

## 동시성

- 뮤테이션 락 하나가 `register`·`add`·`modify`·`remove`·`definitions`를 감싼다. 뮤테이션은 초당 몇 번이 아니라 하루 몇 번짜리라 경합이 없다.
- 핫패스인 `getSubscribers`는 락을 잡지 않는다. `ConcurrentHashMap` 읽기 그대로고 `FrontDispatcher`는 변경이 없다.
- 데드락은 생기지 않는다. `register`는 `TopicQueueContainer.queues.computeIfAbsent` 안에서 불려서 `CHM bin lock → mutationLock` 순서인데, 뮤테이션 경로는 `mutationLock`을 잡은 채 `TopicQueueContainer`를 건드리지 않아 역순이 만들어질 수 없다.

**교체·삭제 시 옛 워커의 종료를 기다리지 않는다.** `Sender`의 `RestClient`에 타임아웃 설정이 없어서, 소비자가 응답하지 않으면 `awaitTermination`이 API 요청을 무한정 붙잡는다.

대신 옛 워커가 인터럽트를 확인하기 전에 메시지를 한 건 더 보내고 커밋할 수 있다. 체크포인트가 잠깐 뒤로 되감기면서 몇 건이 중복 전송될 수 있다는 뜻이다. MMMQ는 NACK 재시도 때문에 이미 at-least-once라 보장이 약해지지 않고, 메시지 손실은 없다.

## 테스트

케이스 목록은 계획서가 실제 테스트 코드로 갖는다. 여기에는 무엇을 테스트하지 않기로 했는지와 그 이유, 그리고 기존 테스트가 받는 영향만 적는다.

broker는 `@SpringBootConfiguration`이 없어서 `@WebMvcTest`를 쓸 수 없다. 컨트롤러는 standalone MockMvc로 검증한다.

**두지 않는 케이스와 이유**

- 쓰기 후 `.tmp`가 남지 않는다: `Files.move`의 `ATOMIC_MOVE` 계약이고, 이동이 실패하면 쓰기 자체가 예외로 끝난다.
- 컨테이너 부트스트랩의 파일 없음·깨진 JSON·미지원 스킴·잘못된 `consumerId`: 앞의 둘은 `DispatcherFileTest`가, 뒤의 둘은 `DispatcherFactoryTest`가 이미 같은 코드를 본다. 생성자는 그 둘을 잇는 7줄이라 컨테이너 수준에서는 와이어링(순서)과 유일한 분기(중복 `consumerId`)만 확인한다.
- `DispatcherDefinition.from`의 단독 왕복: 세 필드를 그대로 옮기는 매핑이라 분기가 없고, 컨테이너의 추가 케이스가 파일 내용을 완전한 정의로 단정해 `create` → `from` → Jackson 왕복을 통째로 지난다. 호스트 이름이 IP로 바뀌지 않는다는 회귀는 `DispatcherFactoryTest`·`HostTest`가 `toUri()`로 잡는다.
- 호스트만 바꾼 수정에서 오프셋 값 승계: 체크포인트 파일이 남았는지만 본다. 파일이 남아 있으면 값을 바꾸는 경로가 `commit`뿐이고, 기존 체크포인트를 tail로 덮어쓰지 않는다는 것은 `TopicQueueTest`가 본다.
- 형식 검증 실패 시 파일 무변경: `add`가 파일에 쓰는 값이 생성된 `Dispatcher`에서 복원한 정의라, `file.write`가 구조적으로 `DispatcherFactory.create`보다 앞설 수 없다. "거절 시 파일이 바뀌지 않는다"는 성질은 중복 `consumerId` 케이스가 붙든다.
- 동시성: 뮤테이션 락은 한 줄이고, 스레드를 여러 쌍 띄워 확인할 수 있는 것은 특정 인터리빙 한 번뿐이다. 뮤테이션이 만드는 최종 상태는 컨테이너 케이스들이 이미 본다.
- 수정·삭제 시 옛 워커의 실제 종료: 인스턴스를 팩토리가 만들어 스파이를 끼울 수 없고, `ThreadPoolExecutor`의 종료 여부를 밖에서 관찰할 통로도 없다. 대신 `destroyed` 플래그가 늦은 `dispatch`를 막는지는 `DispatcherTest`가 워커 풀이 비어 있는지로 결정적으로 관찰한다.
- 성공 케이스의 `verify(container).add(...)`·`verify(container).modify(...)`: 스텁을 실인자로 주면 컨트롤러가 다른 값을 넘기는 순간 목이 `null`을 돌려주고 `jsonPath`가 깨지므로 같은 것을 두 번 단정하는 셈이다. 반환값이 없는 DELETE만 `verify`로 확인한다.
- `BrokerTest`에 새 엔드포인트 추가: standalone MockMvc가 `@RequestMapping` 경로와 `@RestController` 본문 직렬화를 이미 보고, `BrokerTest`는 tempDir root-dir로 새 컨테이너·파일 부팅 경로를 이미 지난다. 컴포넌트 스캔이 이 패키지를 놓치면 같은 패키지의 `FrontDispatcher`가 없어 기존 케이스가 먼저 깨진다.
- DELETE의 404: `@ExceptionHandler`는 컨트롤러 단위라 어느 엔드포인트로 들어와도 같은 코드를 지난다. PUT에서 한 번만 확인한다.
- `deregister` 뒤의 `close()`가 터지지 않는다: `FileChannel.close`가 멱등이라 맵에서 빼든 안 빼든 통과해서, 어떤 구현에서도 지나가는 테스트가 된다. 맵에서 먼저 빼는 이유는 코드 옆 문장으로 남긴다.
- `CheckpointFile.delete()` 단독 케이스: `deregister`가 유일한 호출자라 `CheckpointDirectory` 케이스가 같은 경로를 지나고, 핸들을 먼저 닫는지는 POSIX에서 관찰할 수 없다(열린 파일도 unlink된다).
- `unsubscribe` 뒤 재구독이 tail을 받는다: "체크포인트가 없으면 tail"과 "`deregister`하면 맵에서도 빠진다"의 합성이라 새로 지나는 분기가 없다.
- `TopicQueue.unsubscribe` 단독 케이스: `deregister`로 넘기는 3줄 위임이라, 아래로는 `CheckpointDirectory` 케이스가 파일 삭제를 보고 위로는 컨테이너의 패턴 축소·삭제 케이스가 체크포인트 경로를 직접 단정하며 같은 경로를 지난다.

**기존 테스트 영향**
- `TopicQueueTest`: 4개(`peekReturnsFirstMessage`·`commitAdvancesOffset`·`resumesFromCommittedOffsetAfterRestart`·`redeliversAfterCrashBeforeCommit`)가 "offer 먼저, subscribe 나중" 순서라 tail 전환으로 깨진다. `peekWithoutCommitReturnsSameMessage`는 `offer`가 1건이라 tail=1에서 두 `peek`이 모두 `null`을 돌려주고 통과하지만 아무것도 검증하지 못하게 되므로 같이 순서를 뒤집는다. 다섯 곳 모두 `subscribe`를 `offer` 앞으로 옮기는 것이 새 의미론에 맞는 표현이다
- `TopicQueueBootstrapperTest`: 2개가 같은 이유로 깨지지만, 둘 다 `getOrCreate`의 지연 생성 때문에 `TopicQueueBootstrapper`를 관찰하지 못하고 있어 고치는 대신 다르게 손본다. `restoresAllTopicsOnBoot`는 구독·`peek`을 걷어내고 목 `DispatcherContainer.register`가 받은 큐의 토픽을 단정해 처음으로 복원을 관찰한다. `resumesFromLastCommittedOffset`은 삭제한다 — 커밋 위치 재개는 `TopicQueueTest.resumesFromCommittedOffsetAfterRestart`가, `<root>/topics/<topic>` 경로 조합은 `DispatcherContainerTest`의 체크포인트 경로 단정이 본다
- `HostTest`: "잘못된 호스트명이면 예외" 케이스는 성립하지 않으므로 제거. 빈 주소·포트 범위 케이스로 대체
- `SenderTest`·`GatewayTest`: 양쪽 다 `host.toUri()`를 쓰고 있어 그대로 통과
- `DispatcherTest`: 영향 없다(여섯 곳 모두 `subscribe`가 `offer`보다 앞이다). `destroyed` 플래그를 지키는 케이스 하나가 추가된다
- `docs/index.html`: `dispatchers.json` 예시가 배열이 아니라 단일 객체라, host 줄만 고치면 여전히 `DispatcherDefinition[]` 역직렬화에 실패한다. 배열로 감싸는 것까지 함께 한다
- `DispatcherTest`·`FrontDispatcherTest`: `Dispatcher` 생성자가 그대로라 영향 없음

## 문서

`CLAUDE.md`의 "Broker Dispatcher Registration" 절을 새 파일 포맷과 런타임 API 기준으로 갱신한다.

문서 사이트의 `docs/index.html`·`docs/quickstart.html`도 `dispatchers.json` 예시를 URL 문자열 포맷으로 바꾼다. 옛 중첩 host는 새 `DispatcherDefinition`에 바인딩되지 않아 그대로 따라 쓰면 브로커가 뜨지 못한다. `docs/docs/0.0.2/broker.html`은 릴리스 스냅샷이라 손대지 않는다.

## 다루지 않는 것

- API 인증·인가. 교육용 라이브러리이고 프롬프트 범위 밖이다.
- 새 구독 시작 지점을 요청마다 고르는 옵션(`from: earliest | latest`). tail 고정으로 간다.
- `dispatchers.json` 포맷 마이그레이션.
- OS 크래시·전원 손실 수준의 내구성(fsync).
- `Sender`의 HTTP 타임아웃 설정. 별도 과제다.
