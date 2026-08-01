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
| Dispatcher 생성 | `DispatcherContainer`의 private 헬퍼. 와이어 타입은 변환을 모른다 |
| 계층별 타입 | HTTP는 `api.DispatcherDefinition`, 파일은 `storage.DispatcherEntry` |
| `Host`의 주소 표현 | `InetAddress` 즉시 해석을 그만두고 원본 문자열 보존 |
| 새 구독의 시작 오프셋 | 로그의 tail |
| 체크포인트 수명 | 구독이 끝나면 같이 지운다 (삭제·패턴 축소 모두) |
| 엔드포인트 범위 | 추가·수정·삭제에 목록 조회(`GET /mmmq/dispatchers`)를 포함한다 |
| 동기화·동시성 | 뮤테이션 단일 락 + 파일 원자 교체, 읽기는 락을 잡지 않는다 |

각 결정의 근거는 아래 해당 절에 적었다.

## 계층별 타입과 패키지

`dispatchers.json`의 한 행과 HTTP 본문은 **모양이 같지만 다른 타입**이다.

```
org.mmmq.broker.dispatcher
├── Dispatcher · DispatcherContainer · FrontDispatcher   도메인
├── DispatcherSnapshot                                   도메인 읽기 모델
├── api/       DispatcherController · DispatcherDefinition · DispatcherRoute
├── storage/   DispatcherFile · DispatcherEntry
├── exception/ DispatcherNotFoundException · DuplicateConsumerIdException
└── sender/    Sender
```

**나누는 이유는 필드가 달라서가 아니라 변경 이유가 달라서다.** 하나의 클래스는 하나의 직렬화 정책만 가질 수 있는데, 공유하면 HTTP 계약 안정화를 위한 애노테이션(`@Valid`, `@JsonProperty`)이 디스크 포맷에도 새어 든다. 반대로 파일 포맷 변경이 API 계약을 깬다. 그리고 실제로 결합이 이미 있었다 — 분리 전에는 `DispatcherContainer.add`·`modify`·`remove`가 GET 응답용 목록을 그대로 파일 재작성 버퍼로 재사용했다.

**두 타입 다 메서드가 0개고 `Dispatcher`를 import하지 않는다.** 변환은 전부 `DispatcherContainer`의 private 헬퍼가 한다. 이게 `Dispatcher`의 접근자 셋을 package-private으로 유지하는 유일한 길이다 — 변환이 DTO 쪽에 있으면 그 DTO가 사는 패키지에서 접근자가 보여야 하므로 표면을 넓혀야 한다.

`DispatcherContainer.add`·`modify`는 원시 문자열이나 와이어 타입이 아니라 `(ConsumerId, Host, TopicPattern)`을 받는다. 컨트롤러가 경계에서 `Host.from(...)` 등으로 변환하며, 검증 실패는 기존 `@ExceptionHandler(IllegalArgumentException)`가 400으로 받는다. `DELETE`가 이미 `new ConsumerId(...)`를 경계에서 하고 있었으므로 오히려 일관성이 맞는다.

**`DispatcherSnapshot`이 계층을 가른다.** `(ConsumerId, Host, TopicPattern)` record이고 컴포넌트가 **도메인 값 타입**이다 — 문자열 셋인 두 wire 타입과 컴파일러가 구분해 준다. `DispatcherContainer`는 이걸 반환하지 `api.DispatcherDefinition`을 반환하지 않는다. 도메인 오케스트레이터가 UI 타입을 아는 상태를 없애는 것이 이 타입의 존재 이유다.

변환의 소유자를 가르는 규칙은 하나다 — **컨테이너가 만들어 줄 수 없는 타입만 자기 변환을 갖는다.** `api.DispatcherDefinition.from(DispatcherSnapshot)`이 그 경우다(컨테이너가 만들면 지우려던 의존이 되살아난다). `storage.DispatcherEntry`는 메서드가 없다 — 파일 쓰기는 컨테이너가 소유한 인프라라 컨테이너가 직접 만든다. `DispatcherEntry.from(snapshot)`으로 옮기면 `storage → dispatcher` 엣지가 새로 생겨 문제가 자리만 옮긴다.

**패키지 의존은 전부 한 방향이다**: `api → dispatcher`, `dispatcher → storage`, `storage → persistence`. 서로를 import하는 쌍이 없다. `topicqueue ↔ dispatcher`는 `TopicQueueRegistrar` 인터페이스로 끊었다.

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

**`Host.from(String url)`** (`org.mmmq.core`)

URL 문자열을 해석해 `Host`를 만든다. "무엇이 유효한 host URL인가"라는 지식이 전부 여기 있다.

`Host`가 이 일을 갖는 이유는 대칭이다. `toUri()`(내보내기)와 `from()`(되받기)은 같은 문자열 포맷에 대한 앞뒤 연산인데, 하나는 core에 있고 하나는 broker에 있을 이유가 없다. 같은 패키지의 `WebProtocol`이 이미 `getScheme()`/`from(String)` 양방향을 다 갖고 있다. 그리고 이 포맷 규칙("경로를 붙일 수 없다")은 broker 고유 규칙이 아니라 **`Host`가 `RestClient`의 baseUrl로 쓰인다는 계약**에서 나온다 — producer의 `Gateway`도 같은 방식으로 쓴다.

`URI.create("garbage")`는 예외를 던지지 않고 scheme이 `null`인 URI를 만들기 때문에 scheme·host를 명시적으로 확인해야 한다. 밑줄이 든 호스트명(`consumer_host`)도 `getHost()`가 `null`이라 여기서 걸린다.

**검증 절의 순서가 성능이 아니라 정확성을 좌우한다.** `blank` 검사가 `URI.create`보다 앞서야 하고(`URI.create(null)`은 NPE), scheme·host 검사가 path 검사보다 앞서야 한다 — opaque URI(`consumer-host:8080`)는 `getHost()`와 `getPath()`가 **둘 다 `null`**이라 순서가 뒤집히면 400이어야 할 입력이 NPE로 500이 된다.

빈 주소·포트 범위 검사는 compact constructor에 남는다. `from`은 canonical 생성자를 부르므로 검증이 우회되지 않고, `Host`는 core의 공개 타입이라 라이브러리 사용자가 3인자 생성자로 직접 만드는 경로가 계속 살아 있다.

**`TopicPattern`** (`org.mmmq.core.message`)

compact constructor에서 빈 값을 거절한다. 이전에는 broker의 팩토리가 대신 지켜 줬는데, 그러면 `TopicPattern`을 직접 만드는 다른 경로가 방어를 못 받는다. `ConsumerId`가 정규식으로 자기를 지키는 것과 같은 모양이다.

**`DispatcherFile`** (`org.mmmq.broker.dispatcher`, `@Component`)

`dispatchers.json` 읽기와 원자적 쓰기를 맡는다. `PersistenceProperties`를 주입받아 경로를 정한다.

- `read()`: 파일이 없으면 `[]`로 만들고 빈 목록을 돌려준다(현행 `DispatcherBeanRegistrar` 동작을 옮긴 것).
- `write(definitions)`: 같은 디렉터리의 `dispatchers.json.tmp`에 전체를 쓰고 `ATOMIC_MOVE`로 교체한다.

`ObjectMapper`는 주입받지 않고 클래스 상수로 직접 만든다. broker는 라이브러리라, 호스트 애플리케이션의 `ObjectMapper` 커스터마이징에 파일 포맷이 휘둘리면 안 된다.

**`TopicQueueRegistrar`** (`org.mmmq.broker.topicqueue`)

`void register(TopicQueue topicQueue)` 하나짜리 인터페이스다. `DispatcherContainer`가 구현하고 `TopicQueueContainer`가 이 타입으로 주입받는다.

**패키지 순환을 끊기 위한 것이다.** 그 전에는 `TopicQueueContainer`(저장소 계층)가 `DispatcherContainer`(업무 계층)를 직접 import했고, 반대로 `dispatcher`의 세 파일이 `topicqueue`를 import해서 두 패키지가 서로를 알았다. 인터페이스를 호출자 쪽 패키지에 두면 컴파일 의존이 `dispatcher → topicqueue` 한 방향만 남는다.

메서드 이름을 `register`로 둔 이유는 `DispatcherContainer`가 이미 그 이름·그 시그니처의 메서드를 갖고 있어서다. `@Override`만 붙으면 되고 새로 쓸 코드가 없다. `onCreate` 같은 이벤트 이름을 새로 지으면 같은 일을 가리키는 진입점이 둘이 된다.

큐 생성 경로가 `TopicQueueContainer.getOrCreate` 하나로 수렴하므로(부팅 복원의 `TopicQueueBootstrapper`, 런타임 도착의 `FrontDispatcher` 둘 다) 이 인터페이스가 두 경로를 빠짐없이 덮는다.

**`DispatcherRoute`** (record)

PUT 본문 전용. `record DispatcherRoute(String host, String pattern)`.

**`DispatcherController`** (`@RestController`)

아래 "HTTP API" 절 참고.

**`DuplicateConsumerIdException`**, **`DispatcherNotFoundException`**

`RuntimeException` 상속. 상태 코드 매핑을 정확히 하기 위한 것이다. `IllegalStateException` 하나로 409를 표현하면 다른 원인의 `IllegalStateException`까지 409가 된다.

### 수정

**`DispatcherDefinition`**

레코드 컴포넌트는 `consumerId`·`host`·`pattern` 세 문자열이고, **양방향 변환을 이 타입이 소유한다** — `from(Dispatcher)`가 `host().toUri()`로 host를 되돌리고, `toDispatcher()`가 `Host.from(host)`로 런타임 객체를 만든다. 방향을 여기 모으는 이유는 의존이 한쪽으로만 흐르게 하기 위해서다. `Dispatcher`에 `toDefinition()`을 두면 도메인 객체가 와이어 타입을 알게 되어 JSON 스키마가 바뀔 때마다 dispatch 로직 파일이 열리고, 두 타입이 서로를 아는 순환이 생긴다. 주변적이고 교체 가능한 표현(정의)이 중심적이고 안정적인 개념(Dispatcher)을 아는 방향이 맞다.

소속 판정 `matches(ConsumerId)`와 `(ConsumerId, DispatcherRoute)` 부생성자도 여기 둔다. 앞은 `DispatcherContainer`의 `modify`·`remove`가 각자 `.value()`로 문자열 비교를 반복하던 것이고, 뒤는 `modify`가 정의를 손으로 재조립하던 것이다. 중첩 `HostDefinition` 레코드와 `toHost()`는 삭제한다. `Host`가 원본 주소 문자열을 보존하게 되면서 `Dispatcher` → 정의 복원이 무손실이 된다. 덕분에 컨테이너가 런타임 객체와 정의를 두 벌로 들 필요가 없다.

**`Dispatcher`**

- 공개 생성자는 `(Host, ConsumerId, TopicPattern)`. **와이어 포맷을 끝까지 모른다** — `DispatcherDefinition`을 import하지 않는다.
- 협력자 주입용 package-private 4인자 생성자를 두고 공개 생성자가 거기 위임한다. `sender`가 `private final`이 되어 완성된 객체의 협력자를 밖에서 갈아 끼울 수 없다.
- `consumerId()`·`host()`·`pattern()` 접근자는 전부 package-private이다. 셋 다 패키지 밖 호출자가 없고, 식별자가 밖으로 나가는 통로는 `DispatcherDefinition`이며 그것도 같은 패키지다.
- `destroy()`에서 `@PreDestroy` 제거. 더 이상 빈이 아니므로 컨테이너가 생명주기를 책임진다.

**`DispatcherContainer`**

`DispatcherFile` 하나를 주입받고, `Map<ConsumerId, Dispatcher> dispatchers`·`Map<TopicQueue, List<Dispatcher>> subscriptions`·`ReentrantLock mutationLock`을 필드로 든다.

- 생성자에서 `file.read()`로 Dispatcher를 만들어 채운다. 중복 `consumerId`, 미지원 스킴, 깨진 JSON은 여기서 터져 컨텍스트 기동을 막는다(현행 fail-fast 유지).
- `dispatchers`는 `LinkedHashMap`. 락으로 보호되고, 파일에 쓸 때 순서가 안정적이다.
- `subscriptions`는 `ConcurrentHashMap`. 값은 불변 리스트를 통째로 교체한다.
- `@PreDestroy destroy()`가 모든 Dispatcher의 `destroy()`를 호출한다. 워커만 종료하고 체크포인트는 건드리지 않는다 — 애플리케이션 종료는 구독 해제가 아니다.

공개 메서드: `register(TopicQueue)`, `getSubscribers(TopicQueue)`, `definitions()`, `add(DispatcherDefinition)`, `modify(ConsumerId, DispatcherRoute)`, `remove(ConsumerId)`.

내부 `match(TopicQueue)`가 패턴이 맞는 Dispatcher를 고르고(순수 질의), `subscribeMatched(TopicQueue)`가 그 결과를 구독시킨다(부작용). 둘을 가른 이유는 이름이 하는 일과 맞지 않아서다 — 매칭 알고리즘이 바뀌는 이유와 새 구독이 체크포인트를 만드는 이유는 다르다. `rematchAll()`이 `subscriptions`의 모든 큐에 `match`를 다시 돌려 구독 리스트를 갈아치우면서 이번에 빠진 짝은 `TopicQueue.unsubscribe`로 끊는다. 세 뮤테이션이 이 둘을 공유한다.

`Dispatcher.subscribe`가 `computeIfAbsent`라 이미 구독한 큐를 다시 매칭해도 아무 일도 일어나지 않는다.

구독이 끊긴 짝을 찾는 비교는 **`ConsumerId` 기준**이어야 한다. `Dispatcher`는 `equals`가 없어 객체 동일성으로 비교되는데, 수정은 새 인스턴스로 교체하는 방식이라 객체로 비교하면 호스트만 바꾼 수정에서도 모든 토픽의 체크포인트가 지워진다.

남는 쪽과 지워지는 쪽은 서로 겹치지 않으므로 `match`가 먼저 새 구독을 만들고 그 뒤에 잃은 쪽을 정리해도 순서 문제가 없다. `remove`는 사라진 Dispatcher가 매칭 결과에 안 잡히므로 이 로직만으로 정리가 끝난다.

**`TopicQueue`**

`subscribe(ConsumerId)`는 체크포인트가 없을 때만 `segmentFileChain.tailOffset()`을 써서 새로 만든다. 기존 체크포인트는 손대지 않으므로 재기동 동작은 그대로다.

`register`가 `computeIfAbsent`라 새로 만든 건지 알 수 없어서, `get`이 `null`인지로 신규를 판별한다. 경쟁은 없다 — `subscribe`에 닿는 경로는 `register`·`add`·`modify` 셋뿐이고 전부 뮤테이션 락 안이다. `CheckpointDirectory.open`이 디스크의 체크포인트 파일을 전부 맵에 올려두므로, 재기동 후 첫 `subscribe`에서도 `get`이 `null`이 아니고 tail로 덮어쓰지 않는다.

구독을 끊는 `unsubscribe(ConsumerId)`도 추가한다. `checkpointDirectory.deregister(consumerId.value())`를 부르고 `StorageException`은 삼켜 로그만 남긴다. 이 호출은 `rematchAll`이 여러 토픽을 돌며 일어나는데, 한 토픽의 파일 삭제 실패가 예외로 올라가면 `subscriptions`가 반쯤 갱신된 채 남는다. 지우다 실패한 체크포인트는 아무도 읽지 않는 파일로 남을 뿐이다.

**`SegmentFileChain`**

`tailOffset()`을 추가한다. tail 세그먼트의 `startOffset() + count()`이고, `append`가 로테이션할 때 쓰던 `nextOffset` 계산과 같은 식이므로 그쪽도 이 메서드를 쓴다.

**`CheckpointDirectory`**

`deregister(name)`를 추가한다. 맵에서 빼고, 빠진 게 있으면 `CheckpointFile.delete()`를 부른다. 맵에서 먼저 빼기 때문에 `close()`가 나중에 돌면서 이미 닫힌 파일을 다시 닫는 일은 없다.

**`CheckpointFile`**

`delete()`를 추가한다. 경로를 이미 필드로 들고 있으니 핸들을 닫고 자기 파일을 스스로 지운다.

`open`의 `size == 0 → write(0L)`은 유지한다. 그건 파일을 유효한 상태로 만드는 일이고, tail은 `TopicQueue.subscribe`가 그 위에 덮어쓴다.

`// MOKO: 새 Checkpoint 생성 시 처음부터 시작할지, 최신부터 시작할지 옵션 고려.` 주석은 제거한다. 이 스펙이 "최신(tail)"으로 결론을 냈다.

**`Host`** (core)

`(WebProtocol protocol, String address, int port)` record다. `address`가 `InetAddress`가 아닌 이유는 `InetAddress.getByName()` 즉시 해석을 없애고 원본 주소 문자열을 보존하기 위해서다 — `toUri()`가 그 문자열을 그대로 쓰므로 이름 해석은 `Sender`의 `RestClient`가 요청 시점에 한다.

**record인 이유는 동등성이다.** class였을 때는 `equals`가 없어 값이 같은 두 `Host`가 서로 달랐고, `Host`가 `DispatcherContainer.add`·`modify`의 공개 시그니처에 들어간 뒤로는 테스트가 실인자로 스텁할 수 없었다(컴파일은 되고 런타임에 조용히 매칭 실패해 `argThat` 우회가 생겼다). 저장소의 값 타입은 전부 record라 자동 equals로 단정하는 것이 지배적 방식이고, `Host`만 예외였다. record로 바꾸면서 컴포넌트 접근자 셋이 공개되지만 — `Host`의 역할이 데이터 컨테이너라는 판단에서 그 노출은 누설이 아니라 계약이다. 실제로 쓰는 코드는 아직 0건이고 `toUri()`가 유일한 사용 경로다.

이유는 두 가지다.

1. 아직 뜨지 않아 DNS에 없는 소비자를 미리 등록할 수 없으면, "운영 중에 소비자를 새로 붙인다"는 이 기능의 핵심 용례가 막힌다.
2. 등록 후 소비자 IP가 바뀌어도 브로커가 재기동 전까지 옛 IP로 계속 보내던 문제도 같이 사라진다.

`equals`/`hashCode`는 고치지 않고 지운다. 지금 구현은 `address`만 비교해서 포트가 달라도 같다고 나오는데, 프로덕션에서 `Host`를 비교하는 코드가 없다 — `Sender.from`·`Gateway`는 `toUri()`만 쓰고, `Dispatcher`는 필드로만 들고, 맵·셋의 키는 `TopicQueue`와 `ConsumerId`다. 새로 들어오는 컨테이너·`rematchAll`도 `ConsumerId` 기준으로 비교한다. 비교하는 곳은 `GatewayTest`의 `assertThat(gateway.host).isEqualTo(host)` 한 줄뿐이고 같은 인스턴스를 넘기므로 제거 후 동일성 비교로 통과한다. 틀린 비교를 남기는 것보다 없는 편이 안전하고, 필요해지는 날 전 필드로 넣으면 된다.

필드는 `private final`로 바꾼다. 클래스 밖에서 읽는 코드가 없다.

URL 형식 검증은 `Host.from`이 맡는다. 다만 `Host`는 core의 공개 타입이라 라이브러리 사용자가 3인자 생성자로 직접 생성하므로, compact constructor의 빈 주소와 포트 범위(1~65535) 확인은 인라인으로 남긴다.

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

검증 안에서의 순서는 추가와 수정·삭제가 반대다. 추가는 `definition.toDispatcher()`가 중복 확인보다 앞서고(형식과 중복이 함께 어긋난 요청은 400), 수정·삭제는 존재 확인이 형식 검증보다 앞선다(없는 `consumerId`에 잘못된 host를 보내면 404).

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
- `rematchAll`의 `replaceAll`은 **맵 전체로는 원자적이지 않다**(실측). 그래서 재매칭 도중의 `getSubscribers`는 토픽에 따라 갱신 전/후가 섞인 상태를 볼 수 있다. 항목별로는 원자적이라 — 값이 불변 `List`의 참조 통째 교체라 — 찢긴 리스트는 보이지 않고, 관측되는 최악은 메시지 한 건이 갱신 직전 구독자 집합으로 배달되는 것이다. 삭제된 Dispatcher 쪽은 아래 `destroyed` 가드가 드레인을 막는다.
- 데드락은 생기지 않는다. `register`는 `TopicQueueContainer.queues.computeIfAbsent` 안에서 불려서 `CHM bin lock → mutationLock` 순서인데, 뮤테이션 경로는 `mutationLock`을 잡은 채 `TopicQueueContainer`를 건드리지 않아 역순이 만들어질 수 없다.

**교체·삭제 시 옛 워커의 종료를 기다리지 않는다.** `Sender`의 `RestClient`에 타임아웃 설정이 없어서, 소비자가 응답하지 않으면 `awaitTermination`이 API 요청을 무한정 붙잡는다.

대신 옛 워커가 인터럽트를 확인하기 전에 메시지를 한 건 더 보내고 커밋할 수 있다. 체크포인트가 잠깐 뒤로 되감기면서 몇 건이 중복 전송될 수 있다는 뜻이다. MMMQ는 NACK 재시도 때문에 이미 at-least-once라 보장이 약해지지 않고, 메시지 손실은 없다.

## 테스트

케이스 목록은 계획서가 실제 테스트 코드로 갖는다. 여기에는 무엇을 테스트하지 않기로 했는지와 그 이유, 그리고 기존 테스트가 받는 영향만 적는다.

broker는 `@SpringBootConfiguration`이 없어서 `@WebMvcTest`를 쓸 수 없다. 컨트롤러는 standalone MockMvc로 검증한다.

**두지 않는 케이스와 이유**

- 쓰기 후 `.tmp`가 남지 않는다: `Files.move`의 `ATOMIC_MOVE` 계약이고, 이동이 실패하면 쓰기 자체가 예외로 끝난다.
- 컨테이너 부트스트랩의 파일 없음·깨진 JSON·미지원 스킴·잘못된 `consumerId`: 앞의 둘은 `DispatcherFileTest`가, 뒤의 둘은 `HostTest`·`DispatcherDefinitionTest`가 이미 같은 코드를 본다. 생성자는 그 둘을 잇는 7줄이라 컨테이너 수준에서는 와이어링(순서)과 유일한 분기(중복 `consumerId`)만 확인한다.
- `DispatcherDefinition.from`의 단독 왕복: 세 필드를 그대로 옮기는 매핑이라 분기가 없고, 컨테이너의 추가 케이스가 파일 내용을 완전한 정의로 단정해 `toDispatcher` → `from` → Jackson 왕복을 통째로 지난다. 호스트 이름이 IP로 바뀌지 않는다는 회귀는 `HostTest`가 `toUri()`로 잡는다.
- 호스트만 바꾼 수정에서 오프셋 값 승계: 체크포인트 파일이 남았는지만 본다. 파일이 남아 있으면 값을 바꾸는 경로가 `commit`뿐이고, 기존 체크포인트를 tail로 덮어쓰지 않는다는 것은 `TopicQueueTest`가 본다.
- 형식 검증 실패 시 파일 무변경: `add`가 파일에 쓰는 값이 생성된 `Dispatcher`에서 복원한 정의라, `file.write`가 구조적으로 `definition.toDispatcher()`보다 앞설 수 없다. "거절 시 파일이 바뀌지 않는다"는 성질은 중복 `consumerId` 케이스가 붙든다.
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
