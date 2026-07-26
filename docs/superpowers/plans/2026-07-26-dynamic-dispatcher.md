# 런타임 Dispatcher 관리 API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 브로커가 도는 중에 HTTP API로 Dispatcher를 추가·수정·삭제·조회할 수 있게 하고, 그 변경이 `dispatchers.json`에 원자적으로 반영돼 재기동 후에도 유지되게 한다.

**Architecture:** `DispatcherContainer`가 Dispatcher의 소유자가 되어 `dispatchers.json`을 직접 읽고 쓴다. 뮤테이션(추가·수정·삭제·새 토픽 등록)은 컨테이너 내부의 `ReentrantLock` 하나로 직렬화하고, 메시지 핫패스인 `getSubscribers`는 `ConcurrentHashMap` 무락 읽기를 유지한다. 파일은 임시 파일에 전체를 쓰고 `ATOMIC_MOVE`로 교체한다. 새 구독은 로그 tail부터 시작하고, 구독이 끝나면 그 체크포인트 파일도 지운다.

**Tech Stack:** Java 17, Spring Boot 3.2, Gradle 멀티모듈, JUnit 5, AssertJ, Mockito, spring-test(MockMvc standalone), Jackson

**Spec:** `docs/superpowers/specs/2026-07-26-dynamic-dispatcher-design.md`

**커밋 정책:** 이 계획을 실행할 때 **코드 변경은 커밋하지 않는다.** Task 1~8은 파일 수정과 테스트 통과 확인까지만 하고 멈춘다. 스테이징과 커밋은 사용자가 직접 한다. 예외는 문서만 바꾸는 Task 9뿐이고, 그 태스크에만 커밋 스텝이 있다. 실행 스킬(subagent-driven-development, executing-plans)이 태스크마다 커밋하라고 지시해도 이 정책이 우선한다.

`git rm`은 파일 삭제 수단이므로 그대로 쓴다 — 삭제가 스테이징되는 건 커밋이 아니다.

---

## 파일 구조

### 신규

| 경로 | 책임 |
|---|---|
| `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherFactory.java` | `DispatcherDefinition` → `Dispatcher` 변환과 입력 검증의 단일 지점 (상태가 없어 정적 `create`) |
| `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherFile.java` | `dispatchers.json` 읽기 + 원자적 쓰기 |
| `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherRoute.java` | PUT 본문 (host·pattern) |
| `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherController.java` | 런타임 관리 REST 엔드포인트 4개 |
| `broker/src/main/java/org/mmmq/broker/dispatcher/DuplicateConsumerIdException.java` | 409 매핑용 |
| `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherNotFoundException.java` | 404 매핑용 |

### 수정

| 경로 | 변경 |
|---|---|
| `core/src/main/java/org/mmmq/core/Host.java` | `InetAddress` → 원본 주소 문자열, 아무도 쓰지 않는 `equals`/`hashCode` 제거 |
| `broker/.../topicqueue/storage/SegmentFileChain.java` | `tailOffset()` |
| `broker/.../topicqueue/storage/CheckpointFile.java` | `delete()`, MOKO 주석 제거 |
| `broker/.../topicqueue/storage/CheckpointDirectory.java` | `deregister(name)` |
| `broker/.../topicqueue/TopicQueue.java` | `subscribe`가 신규 구독을 tail로 초기화, `unsubscribe(name)` |
| `broker/.../dispatcher/Dispatcher.java` | `host()`·`pattern()` 접근자, `@PreDestroy` 제거 |
| `broker/.../dispatcher/DispatcherDefinition.java` | host를 URL 문자열로, `from(Dispatcher)`, `HostDefinition` 삭제 |
| `broker/.../dispatcher/DispatcherContainer.java` | 소유·뮤테이션·영속화 |
| `broker/src/main/java/org/mmmq/broker/BrokerConfiguration.java` | `@Import(DispatcherBeanRegistrar.class)` 제거 |
| `broker/.../persistence/PersistenceProperties.java` | `bind(Environment)` 제거 |
| `CLAUDE.md` | 새 파일 포맷과 런타임 API |
| `docs/index.html`, `docs/quickstart.html` | `dispatchers.json` 예시의 중첩 host를 URL 문자열로 |

### 삭제

- `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherBeanRegistrar.java`
- `broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java`
- `broker/src/test/java/org/mmmq/broker/dispatcher/HostDefinitionTest.java`

---

## Task 1: Host가 원본 주소 문자열을 보존

`Host`가 생성자에서 `InetAddress.getByName()`으로 DNS를 즉시 해석하는 탓에, DNS에 아직 없는 소비자를 런타임에 등록할 수 없고 소비자 IP가 바뀌어도 재기동 전까지 옛 IP로 보낸다. 주소 문자열을 그대로 들고, 해석은 `Sender`의 `RestClient`가 요청할 때 하게 한다.

`equals`/`hashCode`도 같이 지운다. 지금 구현은 `address`만 비교해서 포트가 달라도 같다고 나오는데, 이 저장소에서 `Host`를 비교하는 코드가 없다 — `Sender.from`·`Gateway`는 `toUri()`만 쓰고, `Dispatcher`는 필드로만 들고, 맵·셋의 키는 `TopicQueue`와 `ConsumerId`다. 이번에 들어오는 컨테이너도 `ConsumerId` 기준으로 비교한다. 틀린 비교를 고쳐서 남기는 것보다 없애는 편이 짧고, 필요해지는 날 전 필드로 넣는다.

**Files:**
- Modify: `core/src/main/java/org/mmmq/core/Host.java`
- Modify: `core/src/test/java/org/mmmq/core/HostTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`core/src/test/java/org/mmmq/core/HostTest.java` 전체를 아래로 교체. 기존 `createWithUnknownHost`("invalid..host..name"이면 예외)는 DNS 해석을 없애면 성립하지 않으므로 사라지고, 그 자리를 형식 검증 케이스가 대신한다. 기존 `createWithValidHost`도 사라진다 — 유효한 값으로 생성이 터지지 않는다는 것은 `keepsOriginalAddressInUri`가 이미 지나는 경로다.

```java
package org.mmmq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HostTest {

    @Test
    @DisplayName("주소가 비어 있으면 IllegalArgumentException을 던진다.")
    void rejectsBlankAddress() {
        assertThatThrownBy(() -> new Host(WebProtocol.HTTP, "  ", 8080))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("포트가 1~65535 범위를 벗어나면 IllegalArgumentException을 던진다.")
    void rejectsPortOutOfRange() {
        assertThatThrownBy(() -> new Host(WebProtocol.HTTP, "localhost", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Host(WebProtocol.HTTP, "localhost", 65536))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("toUri는 이름을 IP로 바꾸지 않고 원본 주소를 그대로 쓴다.")
    void keepsOriginalAddressInUri() {
        Host host = new Host(WebProtocol.HTTPS, "consumer-host", 8443);

        assertThat(host.toUri()).isEqualTo("https://consumer-host:8443");
    }
}
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `./gradlew :core:test --tests "org.mmmq.core.HostTest"`
Expected: FAIL — `keepsOriginalAddressInUri`는 `consumer-host`가 해석되지 않아 `UnknownHostException` 기반 `IllegalArgumentException`을 받고, `rejectsPortOutOfRange`는 포트 검증이 없어 아무 예외도 받지 못한다. `rejectsBlankAddress`는 `InetAddress.getByName("  ")`이 던지는 덕에 지금도 우연히 통과할 수 있다.

- [ ] **Step 3: 구현**

`core/src/main/java/org/mmmq/core/Host.java` 전체를 아래로 교체:

```java
package org.mmmq.core;

public class Host {

    final WebProtocol protocol;
    final String address;
    final int port;

    public Host(WebProtocol webProtocol, String address, int port) {
        if (address == null || address.isBlank()) {
            throw new IllegalArgumentException("address must not be blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be in 1..65535, but was: " + port);
        }
        protocol = webProtocol;
        this.address = address;
        this.port = port;
    }

    public String toUri() {
        return String.format("%s://%s:%d", protocol.getScheme(), address, port);
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :core:test --tests "org.mmmq.core.HostTest"`
Expected: PASS (3 tests)

- [ ] **Step 5: 전체 테스트로 회귀 확인**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL

`SenderTest`와 `GatewayTest`는 `RestClient`의 baseUrl과 `requestTo(...)` 양쪽에 같은 `host.toUri()`를 쓰기 때문에 `"http://127.0.0.1:8080"`이 `"http://localhost:8080"`으로 바뀌어도 함께 움직여 통과한다. `HostDefinitionTest`는 주소를 이미 `"127.0.0.1"`로 주고 있어 기대 문자열이 그대로 성립한다. `equals`/`hashCode` 제거는 어느 테스트도 건드리지 않는다 — `ProducerTest`의 `mock(Host.class)`는 Mockito가 `equals`를 목으로 넘기지 않아 동일성 비교 그대로다.

---

## Task 2: SegmentFileChain.tailOffset()

새 구독을 로그 끝에서 시작시키려면 다음에 쓰일 절대 오프셋을 알아야 한다. `append`가 로테이션할 때 쓰던 계산식과 같은 값이라 그쪽도 이 메서드를 쓴다.

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/topicqueue/storage/SegmentFileChain.java`
- Modify: `broker/src/test/java/org/mmmq/broker/topicqueue/storage/SegmentFileChainTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`SegmentFileChainTest`의 마지막 `@Test` 뒤에 아래를 추가한다. 필요한 import(`Message`, `Topic`, `Map`, `TempDir`, `assertThat`)는 이미 파일에 있다.

로테이션 케이스 하나만 둔다. `startOffset`과 `count` 두 항이 모두 걸리는 유일한 케이스이고, 빈 체인이 0이라는 것과 append 개수만큼 늘어난다는 것은 Task 4의 `TopicQueueTest`가 같은 숫자로 붙든다 — 빈 큐에 `subscribe`한 뒤 `peek`하는 테스트들은 tail이 0이 아니면 전부 깨지고, `newSubscriptionStartsAtTail`은 `offer` 2건 뒤의 `subscribe`가 2를 돌려주는지 본다.

```java
    @Test
    @DisplayName("세그먼트가 로테이션돼도 tailOffset은 전체 개수를 반영한다")
    void tailOffsetSpansRotatedSegments(@TempDir Path tempDir) {
        try (SegmentFileChain chain = SegmentFileChain.open(tempDir, 1L)) {
            chain.append(new Message(new Topic("topic"), Map.of("seq", 1)));
            chain.append(new Message(new Topic("topic"), Map.of("seq", 2)));
            chain.append(new Message(new Topic("topic"), Map.of("seq", 3)));

            assertThat(chain.tailOffset()).isEqualTo(3L);
        }
    }
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.topicqueue.storage.SegmentFileChainTest"`
Expected: 컴파일 실패 — `cannot find symbol: method tailOffset()`

- [ ] **Step 3: 구현**

`SegmentFileChain.java`의 `append` 메서드를 아래로 교체하고 바로 뒤에 `tailOffset()`을 추가한다.

```java
    public void append(Message message) {
        SegmentFile tailSegmentFile = segmentsByStartOffset.lastEntry().getValue();
        if (tailSegmentFile.reaches(rotationThreshold)) {
            long nextOffset = tailOffset();
            tailSegmentFile = SegmentFile.open(path, nextOffset);
            segmentsByStartOffset.put(nextOffset, tailSegmentFile);
        }
        tailSegmentFile.append(message);
    }

    public long tailOffset() {
        SegmentFile tailSegmentFile = segmentsByStartOffset.lastEntry().getValue();
        return tailSegmentFile.startOffset() + tailSegmentFile.count();
    }
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.topicqueue.storage.SegmentFileChainTest"`
Expected: PASS

---

## Task 3: 체크포인트 삭제 경로

Dispatcher가 사라졌는데 그 `consumerId`의 읽기 위치가 디스크에 남으면 아무도 소유하지 않는 상태가 된다. 파일을 지우는 경로를 만든다.

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/topicqueue/storage/CheckpointFile.java`
- Modify: `broker/src/main/java/org/mmmq/broker/topicqueue/storage/CheckpointDirectory.java`
- Create: `broker/src/test/java/org/mmmq/broker/topicqueue/storage/CheckpointDirectoryTest.java`

`CheckpointFile.delete()`에는 단독 케이스를 두지 않는다. `deregister`가 유일한 호출자라 `delete()`를 깨는 변경은 아래 `deregisterRemovesCheckpoint`에서 먼저 터지고, 핸들을 먼저 닫는지는 POSIX에서 어느 테스트도 관찰할 수 없다(열린 파일도 unlink된다).

- [ ] **Step 1: 실패하는 테스트 작성 (CheckpointDirectory)**

`broker/src/test/java/org/mmmq/broker/topicqueue/storage/CheckpointDirectoryTest.java`:

```java
package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointDirectoryTest {

    private static final String SUBDIRECTORY_NAME = "checkpoints";

    @Test
    @DisplayName("deregister하면 get이 null이고 파일도 사라진다")
    void deregisterRemovesCheckpoint(@TempDir Path tempDir) {
        CheckpointDirectory directory = CheckpointDirectory.open(tempDir);
        directory.register("dispatcher-a");

        directory.deregister("dispatcher-a");

        assertThat(directory.get("dispatcher-a")).isNull();
        assertThat(tempDir.resolve(SUBDIRECTORY_NAME).resolve("dispatcher-a.checkpoint")).doesNotExist();
        directory.close();
    }

    @Test
    @DisplayName("없는 이름으로 deregister해도 아무 일도 없다")
    void deregisterUnknownNameIsNoop(@TempDir Path tempDir) {
        CheckpointDirectory directory = CheckpointDirectory.open(tempDir);

        assertThatCode(() -> directory.deregister("absent")).doesNotThrowAnyException();
        directory.close();
    }
}
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.topicqueue.storage.CheckpointDirectoryTest"`
Expected: 컴파일 실패 — `cannot find symbol: method deregister(String)`

- [ ] **Step 3: CheckpointFile 구현**

`CheckpointFile.java`의 `write` 메서드 뒤, `close` 앞에 추가:

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

같은 파일의 `open` 안에 있는 아래 주석을 제거한다. 신규 체크포인트를 tail에서 시작하기로 결정해 이 미결 사항이 해소됐다(결정은 스펙에 기록).

```java
            // MOKO: 새 Checkpoint 생성 시 처음부터 시작할지, 최신부터 시작할지 옵션 고려.
```

- [ ] **Step 4: CheckpointDirectory 구현**

`CheckpointDirectory.java`의 `get` 메서드 뒤, `close` 앞에 추가:

```java
    public void deregister(String name) {
        CheckpointFile checkpointFile = checkpoints.remove(name);
        if (checkpointFile != null) {
            checkpointFile.delete();
        }
    }
```

맵에서 먼저 빼기 때문에 뒤이은 `close()`가 이미 닫힌 파일을 다시 닫지 않는다. 이 순서를 테스트로는 잡지 않는다 — `FileHandle.close`가 `FileChannel.close`고 그건 멱등이라, 맵에 남겨두는 구현도 이중 close에서 터지지 않는다. `deregisterRemovesCheckpoint`가 보는 `get(name) == null`이 맵 제거 자체는 붙든다.

- [ ] **Step 5: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.topicqueue.storage.CheckpointDirectoryTest"`
Expected: PASS (2 tests)

---

## Task 4: TopicQueue의 구독 시작점과 구독 해제

신규 구독은 tail부터 시작한다. 이미 체크포인트가 있으면 손대지 않으므로 재기동은 영향이 없다. `register`가 `computeIfAbsent`라 새로 만든 건지 알 수 없어서 `get`이 `null`인지로 신규를 판별한다.

**이 태스크는 기존 테스트 6개를 깨뜨린다.** `TopicQueueTest`에서 4개(`peekReturnsFirstMessage`·`commitAdvancesOffset`·`resumesFromCommittedOffsetAfterRestart`·`redeliversAfterCrashBeforeCommit`)가 "offer 먼저, subscribe 나중" 순서라 tail이 0이 아니게 되어 깨진다. `peekWithoutCommitReturnsSameMessage`는 `offer`가 1건이라 tail=1에서 두 `peek`이 모두 `null`을 돌려주고 `null == null`로 조용히 통과하지만 아무것도 검증하지 못하게 되므로 같이 순서를 뒤집는다. 순서를 뒤집는 것이 새 의미론에 맞는 표현이다.

`TopicQueueBootstrapperTest`도 2개가 같은 이유로 깨진다.

- `restoresAllTopicsOnBoot`: `seedTopic`이 세그먼트에 메시지 1건만 심고 체크포인트는 안 심어서, 복원된 큐에 처음 `subscribe`하면 tail(=1)을 받고 `peek`이 `null`이 된다.
- `resumesFromLastCommittedOffset`: `offer` 2건 뒤에 `subscribe`해서 오프셋이 0이 아닌 2가 되고, `commit`이 3을 써서 `isEqualTo(1L)`이 깨진다.

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueue.java`
- Modify: `broker/src/test/java/org/mmmq/broker/topicqueue/TopicQueueTest.java`
- Modify: `broker/src/test/java/org/mmmq/broker/topicqueue/TopicQueueBootstrapperTest.java`

- [ ] **Step 1: 기존 테스트 5개의 subscribe 위치를 옮기고 신규 2개 추가**

`broker/src/test/java/org/mmmq/broker/topicqueue/TopicQueueTest.java`에서 아래 다섯 테스트의 `queue.subscribe("dispatcher-1")` 줄을 그 테스트의 첫 `queue.offer(...)` 줄 위로 옮긴다. 옮기는 것 말고는 어느 줄도 건드리지 않는다 — 구독이 0에서 출발하므로 기존 기대값은 모두 그대로 성립한다.

- `peekReturnsFirstMessage`
- `peekWithoutCommitReturnsSameMessage`
- `commitAdvancesOffset`
- `resumesFromCommittedOffsetAfterRestart`
- `redeliversAfterCrashBeforeCommit`

이 파일의 지역변수 `final` 28개는 그대로 둔다. 프로젝트 규칙에는 어긋나지만, 걷어내면 이 변경의 diff가 tail 의미론과 무관한 28줄을 더 들고 `BrokerTest`·`SegmentFileChainTest`·`SegmentFileTest`·`ConsumerRoutingTest`는 그대로 남아 반쪽 정리가 된다. 아래 신규 테스트는 규칙대로 `final`을 쓰지 않으므로 한동안 한 파일에 두 스타일이 섞인다.

`get != null` 분기(체크포인트가 이미 있으면 tail로 덮어쓰지 않는다)는 따로 케이스를 두지 않는다. `resumesFromCommittedOffsetAfterRestart`가 `commit(1)` 뒤 재기동 `subscribe`에서 tail(2)이 아니라 1을 받는지 단정하고, `redeliversAfterCrashBeforeCommit`도 tail(1)이 아니라 0을 기대해 같은 분기를 한 번 더 지난다(`CheckpointDirectory.open`이 디스크의 체크포인트를 맵에 올려두므로 재기동 후에도 `get`이 `null`이 아니다).

`DEFAULT_MAX_BYTES` 아래에 체크포인트 경로용 상수를 추가한다.

```java
    private static final String CHECKPOINTS_DIR = "checkpoints";
```

마지막 `@Test`와 `createQueue` 사이에 신규 2개를 추가한다. 필요한 import(`Files`·`Path`·`Map`·`TempDir`·`assertThat`)는 이미 파일에 있다.

```java
    @Test
    @DisplayName("이미 쌓인 큐에 새로 subscribe하면 tail부터 시작한다")
    void newSubscriptionStartsAtTail(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        queue.offer(new Message(new Topic("topic"), Map.of("seq", 1)));
        queue.offer(new Message(new Topic("topic"), Map.of("seq", 2)));

        Offset offset = queue.subscribe("late-dispatcher");

        assertThat(offset.value()).isEqualTo(2L);
        assertThat(queue.peek(offset)).isNull();
    }

    @Test
    @DisplayName("unsubscribe하면 체크포인트가 지워진다")
    void unsubscribeRemovesCheckpoint(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        Offset offset = queue.subscribe("dispatcher-1");
        queue.offer(new Message(new Topic("topic"), Map.of("seq", 1)));
        queue.commit("dispatcher-1", offset);

        queue.unsubscribe("dispatcher-1");

        assertThat(tempDir.resolve("topic").resolve(CHECKPOINTS_DIR).resolve("dispatcher-1.checkpoint"))
                .doesNotExist();
    }
```

재구독이 tail을 받는다는 것은 따로 단정하지 않는다. `newSubscriptionStartsAtTail`의 "체크포인트가 없으면 tail"과 `CheckpointDirectoryTest.deregisterRemovesCheckpoint`의 "deregister하면 맵에서도 빠진다"의 합성이라 새로 지나는 분기가 없다.

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.topicqueue.TopicQueueTest"`
Expected: 컴파일 실패 — `cannot find symbol: method unsubscribe(String)`

- [ ] **Step 3: 구현**

`broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueue.java`의 `subscribe`를 아래로 교체하고 바로 뒤에 `unsubscribe`를 추가한다. `CheckpointFile`·`StorageException`은 이미 import되어 있다.

```java
    public Offset subscribe(String name) {
        CheckpointFile checkpointFile = checkpointDirectory.get(name);
        if (checkpointFile == null) {
            checkpointFile = checkpointDirectory.register(name);
            checkpointFile.write(segmentFileChain.tailOffset());
        }
        return new Offset(checkpointFile.read());
    }

    public void unsubscribe(String name) {
        try {
            checkpointDirectory.deregister(name);
        } catch (StorageException exception) {
            log.error("Failed to remove checkpoint '{}' on topic {}", name, topic, exception);
        }
    }
```

`unsubscribe`가 예외를 삼키고 로그만 남기는 이유: 이 호출은 여러 토픽을 돌며 일어나는데, 한 토픽의 삭제 실패가 예외로 올라가면 호출자의 구독 갱신이 반쯤 끝난 상태로 남는다. 지우다 실패한 체크포인트는 아무도 읽지 않는 파일로 남을 뿐이다.

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.topicqueue.TopicQueueTest"`
Expected: PASS (8 tests)

- [ ] **Step 5: TopicQueueBootstrapperTest를 새 의미론에 맞게 고치기**

`broker/src/test/java/org/mmmq/broker/topicqueue/TopicQueueBootstrapperTest.java`의 `seedTopic`을 아래로 교체한다. 세그먼트만 심으면 복원된 큐의 첫 `subscribe`가 tail을 받아 `peek`이 `null`이 되므로, 구독자가 디스크에 남아 있는 상황을 그대로 심는다.

```java
    private void seedTopic(Path topicsDir, String topicName, Message message) {
        Path topicDir = topicsDir.resolve(topicName);
        try {
            Files.createDirectories(topicDir);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        try (SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, DEFAULT_MAX_BYTES)) {
            segmentFileChain.append(message);
        }
        try (CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(topicDir)) {
            checkpointDirectory.register("dispatcher-1");
        }
    }
```

같은 파일의 `resumesFromLastCommittedOffset`에서 `subscribe`를 두 `offer` 앞으로 옮긴다. 그러면 오프셋이 0에서 출발해 `commit`이 1을 쓰고, 기존 기대값 `isEqualTo(1L)`이 그대로 성립한다.

```java
        TopicQueue queue = new TopicQueue(new Topic("topic-a"), segmentFileChain, checkpointDirectory);
        Offset offset = queue.subscribe("dispatcher-1");
        queue.offer(new Message(new Topic("topic-a"), Map.of("seq", 1)));
        queue.offer(new Message(new Topic("topic-a"), Map.of("seq", 2)));
        queue.peek(offset);
        queue.commit("dispatcher-1", offset);
```

- [ ] **Step 6: 전체 테스트로 회귀 확인**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL

`DispatcherTest`의 drain 관련 테스트는 모두 `subscribe`를 `offer`보다 먼저 호출하므로 tail이 0이라 영향이 없다.

---

## Task 5: 정의를 URL 문자열 포맷으로 바꾸고 DispatcherFactory 도입, 레지스트라 제거

파일과 API가 같은 모양을 쓰도록 host를 URL 문자열 하나로 합친다. 문자열 → `Host`·`ConsumerId`·`TopicPattern` 변환과 검증은 `DispatcherFactory` 한 곳으로 모으고, 역방향은 `DispatcherDefinition.from(Dispatcher)`가 맡는다. `Host`가 주소 문자열을 보존하게 됐으므로 이 왕복은 무손실이다.

`DispatcherFactory.create`는 정적 메서드다. 상태가 없고 호출자는 `DispatcherContainer` 하나뿐이라 빈으로 만들면 컨테이너의 필드와 생성자 인자, 테스트의 필드만 늘어난다.

**포트는 필수로 요구한다.** 스킴 기본값(80/443)으로 대체하면 8080에 있는 소비자를 등록할 때 포트를 빼먹은 요청이 조용히 80으로 향한다. 등록 시점에 거절하는 편이 낫고, 저장·응답 형태가 입력과 같은 모양으로 유지된다.

**`DispatcherBeanRegistrar`를 이 태스크에서 지운다.** `definition.toHost()`가 사라지므로 레지스트라를 살려두려면 빈 정의를 공급자 방식으로 재작성하고 테스트의 JSON 픽스처를 새 포맷으로 고쳐야 하는데, 그 코드는 Task 7에서 컨테이너가 파일을 직접 읽는 순간 전부 삭제된다. 지금 지우면 그 왕복이 없다.

Task 7까지 두 태스크 동안 `DispatcherContainer`는 `Collection<Dispatcher>` 생성자를 그대로 들고 있고, Dispatcher 빈을 등록하는 주체가 없어 브로커는 Dispatcher 0개로 뜬다. 생성자가 하나뿐인 빈의 컬렉션 인자는 후보가 없을 때 스프링이 빈 컬렉션을 넣어주므로 컨텍스트는 그대로 기동한다 — 지금도 `dispatchers.json`이 `[]`인 `BrokerTest`가 같은 경로로 뜬다.

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherDefinition.java`
- Modify: `broker/src/main/java/org/mmmq/broker/dispatcher/Dispatcher.java`
- Modify: `broker/src/main/java/org/mmmq/broker/BrokerConfiguration.java`
- Modify: `broker/src/main/java/org/mmmq/broker/persistence/PersistenceProperties.java`
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherFactory.java`
- Create: `broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherFactoryTest.java`
- Create: `broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherDefinitionTest.java`
- Delete: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherBeanRegistrar.java`
- Delete: `broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java`
- Delete: `broker/src/test/java/org/mmmq/broker/dispatcher/HostDefinitionTest.java`

- [ ] **Step 1: 실패하는 테스트 작성 (DispatcherFactory)**

`broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherFactoryTest.java`:

```java
package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;

class DispatcherFactoryTest {

    @Test
    @DisplayName("URL 문자열을 Host로 파싱한다")
    void parsesUrlIntoHost() {
        Dispatcher dispatcher = DispatcherFactory.create(
                new DispatcherDefinition("order-created", "https://consumer-host:8443", "order.created"));

        assertThat(dispatcher.host().toUri()).isEqualTo("https://consumer-host:8443");
        assertThat(dispatcher.consumerId()).isEqualTo(new ConsumerId("order-created"));
        assertThat(dispatcher.pattern()).isEqualTo(new TopicPattern("order.created"));
    }

    @Test
    @DisplayName("포트가 없는 URL은 예외를 던진다")
    void rejectsMissingPort() {
        assertThatThrownBy(() -> DispatcherFactory.create(
                new DispatcherDefinition("a", "http://consumer-host", "**")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("스킴이 없는 문자열은 예외를 던진다")
    void rejectsRelativeUrl() {
        assertThatThrownBy(() -> DispatcherFactory.create(
                new DispatcherDefinition("a", "consumer-host:8080", "**")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("미지원 스킴은 예외를 던진다")
    void rejectsUnsupportedScheme() {
        assertThatThrownBy(() -> DispatcherFactory.create(
                new DispatcherDefinition("a", "ftp://consumer-host:21", "**")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("host가 비어 있으면 예외를 던진다")
    void rejectsBlankHost() {
        assertThatThrownBy(() -> DispatcherFactory.create(new DispatcherDefinition("a", null, "**")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DispatcherFactory.create(new DispatcherDefinition("a", "  ", "**")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("pattern이 비어 있으면 예외를 던진다")
    void rejectsBlankPattern() {
        assertThatThrownBy(() -> DispatcherFactory.create(
                new DispatcherDefinition("a", "http://consumer-host:8080", null)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DispatcherFactory.create(
                new DispatcherDefinition("a", "http://consumer-host:8080", " ")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("consumerId가 regex에 어긋나면 예외를 던진다")
    void rejectsInvalidConsumerId() {
        assertThatThrownBy(() -> DispatcherFactory.create(
                new DispatcherDefinition("invalid id!", "http://consumer-host:8080", "**")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
```

`host`는 `toUri()` 문자열로 단정한다. Task 1에서 `Host.equals`를 없앴고, 파싱 결과가 실제로 쓰이는 형태가 그 문자열이다.

- [ ] **Step 2: 실패하는 테스트 작성 (DispatcherDefinition 왕복)**

`broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherDefinitionTest.java`:

```java
package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DispatcherDefinitionTest {

    @Test
    @DisplayName("정의로 만든 Dispatcher에서 같은 정의를 복원한다")
    void roundTripsThroughDispatcher() {
        DispatcherDefinition definition =
                new DispatcherDefinition("order-created", "https://consumer-host:8443", "order.created");

        DispatcherDefinition restored = DispatcherDefinition.from(DispatcherFactory.create(definition));

        assertThat(restored).isEqualTo(definition);
    }
}
```

호스트 이름이 IP로 바뀌지 않는다는 것도 이 테스트가 본다. `consumer-host`는 해석되지 않는 이름이라, `Host`가 다시 즉시 해석으로 돌아가면 여기서 터진다.

- [ ] **Step 3: 테스트가 실패하는지 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherFactoryTest" --tests "org.mmmq.broker.dispatcher.DispatcherDefinitionTest"`
Expected: 컴파일 실패 — `DispatcherFactory` 클래스 없음, `DispatcherDefinition` 생성자가 `(String, HostDefinition, String)`이라 인자 타입 불일치

- [ ] **Step 4: 레지스트라와 우회 코드 삭제**

레지스트라를 먼저 지운다. `definition.toHost()`와 `HostDefinition`의 유일한 사용자라, 이 순서면 Step 6에서 정의 레코드를 갈아치울 때 main 소스가 계속 컴파일된다.

```bash
git rm broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherBeanRegistrar.java broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java broker/src/test/java/org/mmmq/broker/dispatcher/HostDefinitionTest.java
```

`broker/src/main/java/org/mmmq/broker/BrokerConfiguration.java` 전체를 아래로 교체:

```java
package org.mmmq.broker;

import org.mmmq.broker.persistence.PersistenceProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

@AutoConfiguration
@EnableConfigurationProperties(PersistenceProperties.class)
@ComponentScan(basePackages = "org.mmmq.broker")
class BrokerConfiguration {

}
```

`broker/src/main/java/org/mmmq/broker/persistence/PersistenceProperties.java`에서 아래 메서드와 두 import(`Binder`, `Environment`)를 제거한다. `ImportBeanDefinitionRegistrar`가 `@ConfigurationProperties` 바인딩보다 먼저 도는 탓에 필요했던 우회이고, 유일한 호출자가 방금 사라졌다.

```java
    public static PersistenceProperties bind(Environment environment) {
        return Binder.get(environment)
                .bind(PREFIX, PersistenceProperties.class)
                .orElseGet(() -> new PersistenceProperties(null, null));
    }
```

- [ ] **Step 5: Dispatcher에 host·pattern 접근자 추가**

`DispatcherDefinition.from`이 두 값을 읽어야 한다. 필드를 밖에서 직접 읽지 않도록 `Dispatcher.java`의 `consumerId()` 뒤에 접근자를 둔다. Step 1의 `parsesUrlIntoHost`가 이 두 메서드를 쓴다.

```java
    public Host host() {
        return host;
    }

    public TopicPattern pattern() {
        return pattern;
    }
```

- [ ] **Step 6: DispatcherDefinition 교체**

`broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherDefinition.java` 전체를 아래로 교체. 중첩 `HostDefinition`과 `toHost()`는 사라진다.

```java
package org.mmmq.broker.dispatcher;

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

- [ ] **Step 7: DispatcherFactory 작성**

`broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherFactory.java`:

```java
package org.mmmq.broker.dispatcher;

import java.net.URI;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;

public class DispatcherFactory {

    public static Dispatcher create(DispatcherDefinition definition) {
        if (definition.host() == null || definition.host().isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (definition.pattern() == null || definition.pattern().isBlank()) {
            throw new IllegalArgumentException("pattern must not be blank");
        }
        URI uri = URI.create(definition.host());
        if (uri.getScheme() == null || uri.getHost() == null) {
            throw new IllegalArgumentException("host must be an absolute URL, but was: " + definition.host());
        }
        if (uri.getPort() == -1) {
            throw new IllegalArgumentException("host must include a port, but was: " + definition.host());
        }
        return new Dispatcher(
                new Host(WebProtocol.from(uri.getScheme()), uri.getHost(), uri.getPort()),
                new ConsumerId(definition.consumerId()),
                new TopicPattern(definition.pattern())
        );
    }
}
```

`URI.create("consumer-host:8080")`은 예외를 던지지 않고 scheme만 채워진 URI를 만들고, 밑줄이 든 호스트명은 `getHost()`가 `null`이다. 그래서 scheme·host를 명시적으로 확인해야 한다.

- [ ] **Step 8: 테스트 통과 확인**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL

`BrokerTest`는 Dispatcher 빈이 0개인 채로 컨텍스트를 띄운다. 파일 기반 등록은 Task 7에서 `DispatcherContainer`가 되살린다.

---

## Task 6: DispatcherFile

`dispatchers.json`을 읽고, 임시 파일에 전체를 쓴 뒤 `ATOMIC_MOVE`로 교체한다. 같은 파일시스템이라 이동이 원자적이고, 반쯤 쓰인 JSON이 최종 경로에 보일 수 없다.

`ObjectMapper`는 주입받지 않고 클래스 상수로 만든다. broker는 라이브러리라 호스트 애플리케이션의 `ObjectMapper` 커스터마이징에 파일 포맷이 휘둘리면 안 된다.

파일·디렉터리가 없을 때의 부팅 동작과 깨진 JSON 검증은 여기서 본다. Task 5에서 삭제한 `DispatcherBeanRegistrarTest`가 컨텍스트 레벨에서 지키던 것을 이 파일 레벨로 내린 것이고, Task 7의 컨테이너 테스트는 같은 것을 다시 보지 않는다.

`createsRootDirWhenMissing`이 root-dir까지 없는 상태를 보므로 "파일만 없는 상태"는 따로 두지 않는다 — `read()`의 같은 분기이고 `Files.createDirectories`는 이미 있는 디렉터리에 무해하다. 쓰기 후 `.tmp`가 남지 않는다는 것도 `Files.move`의 `ATOMIC_MOVE` 계약이라 케이스를 두지 않는다. 이동이 실패하면 `roundTrips`가 `IllegalStateException`으로 먼저 깨진다.

**Files:**
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherFile.java`
- Create: `broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherFileTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherFileTest.java`:

```java
package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.persistence.PersistenceProperties;

class DispatcherFileTest {

    private static final String FILE_NAME = "dispatchers.json";

    @TempDir
    Path tempDir;

    DispatcherFile dispatcherFile;

    @BeforeEach
    void setUp() {
        dispatcherFile = new DispatcherFile(new PersistenceProperties(tempDir.toString(), null));
    }

    @Test
    @DisplayName("쓰고 다시 읽으면 같은 목록이 나온다")
    void roundTrips() {
        List<DispatcherDefinition> definitions = List.of(
                new DispatcherDefinition("order-created", "http://consumer-host:8080", "order.created"),
                new DispatcherDefinition("order-shipped", "https://other-host:8443", "order.shipped")
        );

        dispatcherFile.write(definitions);

        assertThat(dispatcherFile.read()).isEqualTo(definitions);
    }

    @Test
    @DisplayName("root-dir 디렉터리가 없으면 만들고 빈 목록을 반환한다")
    void createsRootDirWhenMissing() {
        Path absentRoot = tempDir.resolve("absent-root");
        DispatcherFile file = new DispatcherFile(new PersistenceProperties(absentRoot.toString(), null));

        assertThat(file.read()).isEmpty();

        assertThat(absentRoot.resolve(FILE_NAME)).exists();
    }

    @Test
    @DisplayName("깨진 JSON은 예외를 던진다")
    void rejectsMalformedJson() throws IOException {
        Files.writeString(tempDir.resolve(FILE_NAME), "not json");

        assertThatThrownBy(dispatcherFile::read)
                .isInstanceOf(IllegalStateException.class);
    }
}
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherFileTest"`
Expected: 컴파일 실패 — `cannot find symbol: class DispatcherFile`

- [ ] **Step 3: 구현**

`broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherFile.java`:

```java
package org.mmmq.broker.dispatcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import org.mmmq.broker.persistence.PersistenceProperties;
import org.springframework.stereotype.Component;

@Component
public class DispatcherFile {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEMP_SUFFIX = ".tmp";
    private static final String EMPTY_ARRAY = "[]";

    private final Path path;

    public DispatcherFile(PersistenceProperties properties) {
        path = properties.dispatchersFile();
    }

    public List<DispatcherDefinition> read() {
        try {
            if (!Files.exists(path)) {
                Files.createDirectories(path.toAbsolutePath().getParent());
                Files.writeString(path, EMPTY_ARRAY);
                return List.of();
            }
            return List.of(OBJECT_MAPPER.readValue(Files.readAllBytes(path), DispatcherDefinition[].class));
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to read dispatcher file: " + path, exception);
        }
    }

    public void write(List<DispatcherDefinition> definitions) {
        Path temp = path.resolveSibling(path.getFileName() + TEMP_SUFFIX);
        try {
            Files.write(temp, OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(definitions));
            Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to write dispatcher file: " + path, exception);
        }
    }
}
```

`JsonProcessingException`은 `IOException`의 하위 타입이라 catch 하나로 충분하다.

`write`는 디렉터리를 만들지 않는다. 유일한 호출자가 `DispatcherContainer`의 뮤테이션이고, 컨테이너 생성자가 이미 `read()`로 디렉터리를 만든 뒤다. 도는 중에 누가 root-dir을 지웠다면 `Files.write`가 `NoSuchFileException`을 던져 `IllegalStateException` → 500이 되는데, 메모리를 건드리기 전이라 상태는 온전하다(검증 → 파일 → 메모리 순서 그대로).

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherFileTest"`
Expected: PASS (3 tests)

---

## Task 7: DispatcherContainer가 Dispatcher를 소유하고 변경한다

컨테이너가 `dispatchers.json`을 직접 읽어 Dispatcher를 만들고, 추가·수정·삭제·조회를 노출한다. Dispatcher가 태어나는 길이 하나가 된다. 뮤테이션은 `ReentrantLock` 하나로 직렬화하고, 핫패스 `getSubscribers`는 무락 읽기를 유지한다. 순서는 언제나 **검증 → 파일 → 메모리**다.

이 태스크에서 `Dispatcher`의 `@PreDestroy`를 컨테이너로 옮긴다. Dispatcher는 더 이상 빈이 아니라 `@PreDestroy`가 걸리지 않으므로, 컨테이너가 생명주기를 책임져야 한다.

`DispatcherFactory.create`는 정적 메서드라 컨테이너 생성자 인자는 `DispatcherFile` 하나다.

**파일에 쓰는 값:** `DispatcherDefinition.from(dispatcher)`로 복원한 값을 쓴다. 실제 등록된 Dispatcher가 유일한 출처라 파일과 API 응답이 메모리 상태와 어긋날 수 없다.

**부트스트랩 검증 범위:** 생성자는 `file.read()`와 `DispatcherFactory.create`를 잇는 7줄이다. 파일 없음·깨진 JSON은 Task 6의 `DispatcherFileTest`가, 미지원 스킴·잘못된 `consumerId`는 Task 5의 `DispatcherFactoryTest`가 이미 같은 코드를 본다. 그래서 컨테이너 수준에서는 와이어링(순서)과 생성자의 유일한 분기(중복 `consumerId`)만 확인한다.

**tail 시작과 등록 순서를 따로 보지 않는 이유:** 새 구독이 tail에서 출발한다는 것은 `wideningPatternSubscribesNewTopicAtTail`이 `paymentQueue`에서 정확히 같은 경로(`rematchAll` → `match` → `dispatcher.subscribe` → `queue.subscribe`)로 본다. `definitions()`의 순서는 `loadsDefinitionsFromFile`이 `containsExactly`로 단정하고, `add`·`remove` 케이스가 `file.read()`를 같은 방식으로 보면서 `dispatchers.values()` 순서를 두 번 더 지난다.

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherContainer.java`
- Modify: `broker/src/main/java/org/mmmq/broker/dispatcher/Dispatcher.java`
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherRoute.java`
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DuplicateConsumerIdException.java`
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherNotFoundException.java`
- Create: `broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherContainerTest.java`

- [ ] **Step 1: 예외 두 개와 DispatcherRoute 작성**

`broker/src/main/java/org/mmmq/broker/dispatcher/DuplicateConsumerIdException.java`:

```java
package org.mmmq.broker.dispatcher;

import org.mmmq.core.identifier.ConsumerId;

public class DuplicateConsumerIdException extends RuntimeException {

    public DuplicateConsumerIdException(ConsumerId consumerId) {
        super("Duplicate consumerId '" + consumerId + "'");
    }
}
```

`broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherNotFoundException.java`:

```java
package org.mmmq.broker.dispatcher;

import org.mmmq.core.identifier.ConsumerId;

public class DispatcherNotFoundException extends RuntimeException {

    public DispatcherNotFoundException(ConsumerId consumerId) {
        super("No dispatcher for consumerId '" + consumerId + "'");
    }
}
```

`broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherRoute.java`:

```java
package org.mmmq.broker.dispatcher;

public record DispatcherRoute(
        String host,
        String pattern
) {
}
```

- [ ] **Step 2: 실패하는 테스트 작성**

`broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherContainerTest.java`:

```java
package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.persistence.PersistenceProperties;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueFactory;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class DispatcherContainerTest {

    private static final String CHECKPOINTS_DIR = "checkpoints";
    private static final String FILE_NAME = "dispatchers.json";
    private static final String HOST = "http://consumer-host:8080";

    @TempDir
    Path tempDir;

    DispatcherFile file;
    TopicQueueFactory topicQueueFactory;
    DispatcherContainer container;

    @BeforeEach
    void setUp() {
        PersistenceProperties properties = new PersistenceProperties(tempDir.toString(), null);
        file = new DispatcherFile(properties);
        topicQueueFactory = new TopicQueueFactory(properties);
        container = new DispatcherContainer(file);
    }

    @Test
    @DisplayName("추가하면 매칭되는 기존 큐의 구독자가 되고, 파일에도 반영된다")
    void addSubscribesToExistingQueueAndPersists() {
        TopicQueue queue = register(new Topic("order.created"));

        container.add(new DispatcherDefinition("order-created", HOST, "order.*"));

        assertThat(container.getSubscribers(queue))
                .extracting(Dispatcher::consumerId)
                .containsExactly(new ConsumerId("order-created"));
        assertThat(file.read())
                .containsExactly(new DispatcherDefinition("order-created", HOST, "order.*"));
    }

    @Test
    @DisplayName("중복 consumerId는 DuplicateConsumerIdException을 던지고 파일을 바꾸지 않는다")
    void rejectsDuplicateConsumerId() {
        container.add(new DispatcherDefinition("order-created", HOST, "order.*"));

        assertThatThrownBy(() -> container.add(new DispatcherDefinition("order-created", HOST, "other.*")))
                .isInstanceOf(DuplicateConsumerIdException.class);
        assertThat(file.read())
                .containsExactly(new DispatcherDefinition("order-created", HOST, "order.*"));
    }

    @Test
    @DisplayName("호스트만 바꾸면 체크포인트가 승계되고 파일이 지워지지 않는다")
    void modifyHostKeepsCheckpoint() {
        TopicQueue queue = register(new Topic("order.created"));
        container.add(new DispatcherDefinition("order-created", HOST, "order.*"));
        queue.offer(new Message(new Topic("order.created"), Map.of("seq", 1)));
        queue.commit("order-created", queue.subscribe("order-created"));

        container.modify(new ConsumerId("order-created"), new DispatcherRoute("http://moved-host:9090", "order.*"));

        assertThat(checkpointOf("order.created", "order-created")).exists();
        assertThat(queue.subscribe("order-created").value()).isEqualTo(1L);
        assertThat(file.read())
                .containsExactly(new DispatcherDefinition("order-created", "http://moved-host:9090", "order.*"));
    }

    @Test
    @DisplayName("패턴을 넓히면 새로 매칭된 토픽을 tail부터 구독한다")
    void wideningPatternSubscribesNewTopicAtTail() {
        TopicQueue orderQueue = register(new Topic("order.created"));
        TopicQueue paymentQueue = register(new Topic("payment.done"));
        container.add(new DispatcherDefinition("consumer", HOST, "order.*"));
        paymentQueue.offer(new Message(new Topic("payment.done"), Map.of("seq", 1)));

        container.modify(new ConsumerId("consumer"), new DispatcherRoute(HOST, "**"));

        assertThat(container.getSubscribers(orderQueue)).hasSize(1);
        assertThat(container.getSubscribers(paymentQueue)).hasSize(1);
        assertThat(paymentQueue.subscribe("consumer").value()).isEqualTo(1L);
    }

    @Test
    @DisplayName("패턴을 좁히면 빠진 토픽의 구독과 체크포인트가 사라진다")
    void narrowingPatternDropsSubscriptionAndCheckpoint() {
        TopicQueue orderQueue = register(new Topic("order.created"));
        TopicQueue paymentQueue = register(new Topic("payment.done"));
        container.add(new DispatcherDefinition("consumer", HOST, "**"));

        container.modify(new ConsumerId("consumer"), new DispatcherRoute(HOST, "order.*"));

        assertThat(container.getSubscribers(orderQueue)).hasSize(1);
        assertThat(container.getSubscribers(paymentQueue)).isEmpty();
        assertThat(checkpointOf("payment.done", "consumer")).doesNotExist();
        assertThat(checkpointOf("order.created", "consumer")).exists();
    }

    @Test
    @DisplayName("삭제하면 구독자에서 빠지고, 구독하던 모든 토픽의 체크포인트가 지워진다")
    void removeDropsSubscriptionsAndCheckpoints() {
        TopicQueue orderQueue = register(new Topic("order.created"));
        TopicQueue paymentQueue = register(new Topic("payment.done"));
        container.add(new DispatcherDefinition("consumer", HOST, "**"));

        container.remove(new ConsumerId("consumer"));

        assertThat(container.getSubscribers(orderQueue)).isEmpty();
        assertThat(container.getSubscribers(paymentQueue)).isEmpty();
        assertThat(checkpointOf("order.created", "consumer")).doesNotExist();
        assertThat(checkpointOf("payment.done", "consumer")).doesNotExist();
        assertThat(file.read()).isEmpty();
    }

    @Test
    @DisplayName("없는 consumerId로 수정하거나 삭제하면 DispatcherNotFoundException을 던진다")
    void rejectsUnknownConsumerId() {
        assertThatThrownBy(() ->
                container.modify(new ConsumerId("absent"), new DispatcherRoute(HOST, "**")))
                .isInstanceOf(DispatcherNotFoundException.class);
        assertThatThrownBy(() -> container.remove(new ConsumerId("absent")))
                .isInstanceOf(DispatcherNotFoundException.class);
    }

    @Test
    @DisplayName("파일의 정의를 순서대로 읽어 Dispatcher를 만든다")
    void loadsDefinitionsFromFile() {
        write("""
                [
                  {"consumerId":"order-created","host":"http://127.0.0.1:8080","pattern":"order.created"},
                  {"consumerId":"order-shipped","host":"http://127.0.0.1:8080","pattern":"order.shipped"}
                ]
                """);

        DispatcherContainer loaded = new DispatcherContainer(file);

        assertThat(loaded.definitions())
                .extracting(DispatcherDefinition::consumerId)
                .containsExactly("order-created", "order-shipped");
    }

    @Test
    @DisplayName("파일에 중복 consumerId가 있으면 생성에 실패한다")
    void failsOnDuplicateConsumerIdInFile() {
        write("""
                [
                  {"consumerId":"dup","host":"http://127.0.0.1:8080","pattern":"a"},
                  {"consumerId":"dup","host":"http://127.0.0.1:8080","pattern":"b"}
                ]
                """);

        assertThatThrownBy(() -> new DispatcherContainer(file))
                .isInstanceOf(DuplicateConsumerIdException.class);
    }

    private TopicQueue register(Topic topic) {
        TopicQueue queue = topicQueueFactory.create(topic);
        container.register(queue);
        return queue;
    }

    private Path checkpointOf(String topicName, String consumerId) {
        return tempDir.resolve("topics")
                .resolve(topicName)
                .resolve(CHECKPOINTS_DIR)
                .resolve(consumerId + ".checkpoint");
    }

    private void write(String json) {
        try {
            Files.writeString(tempDir.resolve(FILE_NAME), json);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to write dispatcher file", exception);
        }
    }
}
```

- [ ] **Step 3: 테스트가 실패하는지 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherContainerTest"`
Expected: 컴파일 실패 — `DispatcherContainer` 생성자 인자 타입 불일치, `add`/`modify`/`remove`/`definitions` 없음

- [ ] **Step 4: Dispatcher에서 @PreDestroy 제거**

`Dispatcher.java`에서 `destroy()` 위의 `@PreDestroy`를 지우고, 쓰이지 않게 된 `import jakarta.annotation.PreDestroy;`도 제거한다.

```java
    public void destroy() {
        workerPool.shutdownAll();
    }
```

- [ ] **Step 5: DispatcherContainer 구현**

`broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherContainer.java` 전체를 아래로 교체:

```java
package org.mmmq.broker.dispatcher;

import jakarta.annotation.PreDestroy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.core.identifier.ConsumerId;
import org.springframework.stereotype.Component;

@Component
public class DispatcherContainer {

    private final DispatcherFile file;
    private final Map<ConsumerId, Dispatcher> dispatchers = new LinkedHashMap<>();
    private final Map<TopicQueue, List<Dispatcher>> subscriptions = new ConcurrentHashMap<>();
    private final ReentrantLock mutationLock = new ReentrantLock();

    public DispatcherContainer(DispatcherFile file) {
        this.file = file;
        file.read().forEach(definition -> {
            Dispatcher dispatcher = DispatcherFactory.create(definition);
            if (dispatchers.putIfAbsent(dispatcher.consumerId(), dispatcher) != null) {
                throw new DuplicateConsumerIdException(dispatcher.consumerId());
            }
        });
    }

    public void register(TopicQueue topicQueue) {
        mutationLock.lock();
        try {
            subscriptions.put(topicQueue, match(topicQueue));
        } finally {
            mutationLock.unlock();
        }
    }

    public List<Dispatcher> getSubscribers(TopicQueue topicQueue) {
        return subscriptions.getOrDefault(topicQueue, List.of());
    }

    public List<DispatcherDefinition> definitions() {
        mutationLock.lock();
        try {
            return dispatchers.values().stream()
                    .map(DispatcherDefinition::from)
                    .toList();
        } finally {
            mutationLock.unlock();
        }
    }

    public DispatcherDefinition add(DispatcherDefinition definition) {
        mutationLock.lock();
        try {
            Dispatcher dispatcher = DispatcherFactory.create(definition);
            if (dispatchers.containsKey(dispatcher.consumerId())) {
                throw new DuplicateConsumerIdException(dispatcher.consumerId());
            }
            DispatcherDefinition registered = DispatcherDefinition.from(dispatcher);
            file.write(Stream.concat(
                    dispatchers.values().stream().map(DispatcherDefinition::from),
                    Stream.of(registered)
            ).toList());
            dispatchers.put(dispatcher.consumerId(), dispatcher);
            rematchAll();
            return registered;
        } finally {
            mutationLock.unlock();
        }
    }

    public DispatcherDefinition modify(ConsumerId consumerId, DispatcherRoute route) {
        mutationLock.lock();
        try {
            Dispatcher previous = dispatchers.get(consumerId);
            if (previous == null) {
                throw new DispatcherNotFoundException(consumerId);
            }
            Dispatcher next = DispatcherFactory.create(
                    new DispatcherDefinition(consumerId.value(), route.host(), route.pattern()));
            DispatcherDefinition registered = DispatcherDefinition.from(next);
            file.write(dispatchers.values().stream()
                    .map(existing -> existing.consumerId().equals(consumerId)
                            ? registered
                            : DispatcherDefinition.from(existing))
                    .toList());
            previous.destroy();
            dispatchers.put(consumerId, next);
            rematchAll();
            return registered;
        } finally {
            mutationLock.unlock();
        }
    }

    public void remove(ConsumerId consumerId) {
        mutationLock.lock();
        try {
            Dispatcher dispatcher = dispatchers.get(consumerId);
            if (dispatcher == null) {
                throw new DispatcherNotFoundException(consumerId);
            }
            file.write(dispatchers.values().stream()
                    .filter(existing -> !existing.consumerId().equals(consumerId))
                    .map(DispatcherDefinition::from)
                    .toList());
            dispatcher.destroy();
            dispatchers.remove(consumerId);
            rematchAll();
        } finally {
            mutationLock.unlock();
        }
    }

    @PreDestroy
    public void destroy() {
        mutationLock.lock();
        try {
            dispatchers.values().forEach(Dispatcher::destroy);
        } finally {
            mutationLock.unlock();
        }
    }

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
            List<ConsumerId> retained = matched.stream().map(Dispatcher::consumerId).toList();
            previous.stream()
                    .map(Dispatcher::consumerId)
                    .filter(consumerId -> !retained.contains(consumerId))
                    .forEach(consumerId -> topicQueue.unsubscribe(consumerId.value()));
            return matched;
        });
    }
}
```

구독을 잃은 짝을 찾는 비교가 `ConsumerId` 기준인 이유: `Dispatcher`는 `equals`가 없어 객체 동일성으로 비교되는데 `modify`는 새 인스턴스로 교체하므로, 객체로 비교하면 호스트만 바꾼 수정에서도 모든 토픽의 체크포인트가 지워진다. `modifyHostKeepsCheckpoint` 테스트가 이 실수를 잡는다.

`destroy()`는 워커만 종료하고 체크포인트는 건드리지 않는다. 애플리케이션 종료는 구독 해제가 아니다.

**검증하지 않는 것:** 수정·삭제 시 옛 Dispatcher의 워커가 실제로 종료됐는지는 테스트로 확인하지 않는다. 인스턴스를 팩토리가 만들기 때문에 스파이를 끼울 수 없고, `ThreadPoolExecutor`의 종료 여부를 밖에서 관찰할 통로도 없다. `previous.destroy()`·`dispatcher.destroy()` 호출 경로는 코드 리뷰로 확인한다.

같은 이유로 테스트에 `@AfterEach container.destroy()`를 두지 않는다. `WorkerPool`은 `dispatch`의 `computeIfAbsent`에서만 채워지는데 이 파일의 9개 테스트 중 `dispatch`를 부르는 것이 없어 풀이 언제나 비어 있고, 그래서 `destroy()`가 아무 일도 하지 않는다.

형식 검증(`not-a-url` 같은 입력) 실패 시 파일이 바뀌지 않는다는 케이스도 따로 두지 않는다. `add`가 파일에 쓰는 값이 `DispatcherDefinition.from(dispatcher)`라서 `file.write`의 인자를 만들려면 `DispatcherFactory.create`가 먼저 성공해야 하고, 그래서 이 순서는 구조적으로 뒤집힐 수 없다. "거절 시 파일 무변경"이라는 성질 자체는 `rejectsDuplicateConsumerId`가 같은 세 단 구조로 붙든다.

동시성 테스트도 두지 않는다. 뮤테이션 락은 한 줄이고, 스레드를 여러 쌍 띄워 확인할 수 있는 것은 특정 인터리빙 한 번뿐이다. 뮤테이션이 만드는 최종 상태는 위 9개가 이미 본다.

- [ ] **Step 6: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherContainerTest"`
Expected: PASS (9 tests)

- [ ] **Step 7: 전체 테스트로 회귀 확인**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL

---

## Task 8: DispatcherController

엔드포인트 4개와 상태 코드 매핑을 붙인다. 전역 `@RestControllerAdvice`는 쓰지 않는다. broker는 라이브러리라 호스트 애플리케이션의 다른 컨트롤러 예외까지 가로챈다.

broker에는 `@SpringBootConfiguration`이 없어 `@WebMvcTest`를 쓸 수 없다. standalone MockMvc로 검증한다.

`IllegalArgumentException` → 400 매핑은 `rejectsInvalidConsumerIdInPath`가 목 없이 실제 입력으로 지나가므로, 목이 예외를 던지게 만드는 별도 케이스는 두지 않는다. 예외 핸들러는 컨트롤러 단위라 어느 엔드포인트로 들어와도 같은 코드다. 같은 이유로 `DispatcherNotFoundException` → 404도 PUT에서만 확인한다. DELETE로 한 번 더 들어가도 지나는 핸들러가 같고, `remove`가 없는 id에서 던진다는 것은 `DispatcherContainerTest.rejectsUnknownConsumerId`가 본다.

**Files:**
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherController.java`
- Create: `broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherControllerTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherControllerTest.java`:

```java
package org.mmmq.broker.dispatcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.identifier.ConsumerId;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class DispatcherControllerTest {

    private static final String HOST = "http://consumer-host:8080";

    DispatcherContainer container;
    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        container = mock(DispatcherContainer.class);
        mockMvc = MockMvcBuilders.standaloneSetup(new DispatcherController(container)).build();
    }

    @Test
    @DisplayName("GET은 200과 현재 정의 목록을 돌려준다")
    void getReturnsDefinitions() throws Exception {
        when(container.definitions())
                .thenReturn(List.of(new DispatcherDefinition("order-created", HOST, "order.*")));

        mockMvc.perform(get("/mmmq/dispatchers"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].consumerId").value("order-created"))
                .andExpect(jsonPath("$[0].host").value(HOST))
                .andExpect(jsonPath("$[0].pattern").value("order.*"));
    }

    @Test
    @DisplayName("POST 성공 시 201과 등록된 정의를 돌려준다")
    void postReturnsCreated() throws Exception {
        when(container.add(any()))
                .thenReturn(new DispatcherDefinition("order-created", HOST, "order.*"));

        mockMvc.perform(post("/mmmq/dispatchers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"consumerId":"order-created","host":"http://consumer-host:8080","pattern":"order.*"}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.consumerId").value("order-created"));
    }

    @Test
    @DisplayName("POST에서 중복 consumerId는 409를 돌려준다")
    void postReturnsConflictOnDuplicate() throws Exception {
        when(container.add(any()))
                .thenThrow(new DuplicateConsumerIdException(new ConsumerId("order-created")));

        mockMvc.perform(post("/mmmq/dispatchers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"consumerId":"order-created","host":"http://consumer-host:8080","pattern":"order.*"}
                                """))
                .andExpect(status().isConflict());
    }

    @Test
    @DisplayName("PUT 성공 시 200과 바뀐 정의를 돌려준다")
    void putReturnsOk() throws Exception {
        when(container.modify(any(), any()))
                .thenReturn(new DispatcherDefinition("order-created", "http://moved-host:9090", "order.*"));

        mockMvc.perform(put("/mmmq/dispatchers/order-created")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"host":"http://moved-host:9090","pattern":"order.*"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.host").value("http://moved-host:9090"));
    }

    @Test
    @DisplayName("PUT에서 없는 consumerId는 404를 돌려준다")
    void putReturnsNotFound() throws Exception {
        when(container.modify(any(), any()))
                .thenThrow(new DispatcherNotFoundException(new ConsumerId("absent")));

        mockMvc.perform(put("/mmmq/dispatchers/absent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"host":"http://consumer-host:8080","pattern":"order.*"}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("DELETE 성공 시 204를 돌려준다")
    void deleteReturnsNoContent() throws Exception {
        mockMvc.perform(delete("/mmmq/dispatchers/order-created"))
                .andExpect(status().isNoContent());
    }

    @Test
    @DisplayName("경로 변수의 consumerId가 regex에 어긋나면 400을 돌려준다")
    void rejectsInvalidConsumerIdInPath() throws Exception {
        mockMvc.perform(delete("/mmmq/dispatchers/invalid+id"))
                .andExpect(status().isBadRequest());
    }
}
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherControllerTest"`
Expected: 컴파일 실패 — `cannot find symbol: class DispatcherController`

- [ ] **Step 3: 구현**

`broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherController.java`:

```java
package org.mmmq.broker.dispatcher;

import java.util.List;
import org.mmmq.core.identifier.ConsumerId;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/mmmq/dispatchers")
public class DispatcherController {

    private final DispatcherContainer container;

    public DispatcherController(DispatcherContainer container) {
        this.container = container;
    }

    @GetMapping
    public List<DispatcherDefinition> getDispatchers() {
        return container.definitions();
    }

    @PostMapping
    public ResponseEntity<DispatcherDefinition> postDispatcher(@RequestBody DispatcherDefinition definition) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(container.add(definition));
    }

    @PutMapping("/{consumerId}")
    public DispatcherDefinition putDispatcher(
            @PathVariable String consumerId,
            @RequestBody DispatcherRoute route
    ) {
        return container.modify(new ConsumerId(consumerId), route);
    }

    @DeleteMapping("/{consumerId}")
    public ResponseEntity<Void> deleteDispatcher(@PathVariable String consumerId) {
        container.remove(new ConsumerId(consumerId));
        return ResponseEntity.noContent().build();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<String> handleBadRequest(IllegalArgumentException exception) {
        return ResponseEntity.badRequest()
                .body(exception.getMessage());
    }

    @ExceptionHandler(DuplicateConsumerIdException.class)
    ResponseEntity<String> handleConflict(DuplicateConsumerIdException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(exception.getMessage());
    }

    @ExceptionHandler(DispatcherNotFoundException.class)
    ResponseEntity<String> handleNotFound(DispatcherNotFoundException exception) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(exception.getMessage());
    }
}
```

409·404를 예외 클래스의 `@ResponseStatus`로 대신하지 않는 이유: `ResponseStatusExceptionResolver.applyStatusAndReason`은 `reason`이 비어도 `setStatus`가 아니라 `response.sendError(statusCode)`를 부른다(spring-webmvc 6.1.1 바이트코드 확인). 서블릿 컨테이너의 에러 페이지 디스패치가 돌아서 응답 본문은 호스트 애플리케이션의 `/error` 처리(기본은 Boot의 에러 JSON, 커스텀 에러 뷰가 있으면 그쪽)가 만든다. 라이브러리가 응답을 통째로 남의 설정에 넘기게 되고, 400만 예외 메시지를 돌려주는 비대칭도 생긴다. 세 핸들러를 컨트롤러 안에 두면 이 API의 실패 응답이 브로커 안에서 끝난다.

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherControllerTest"`
Expected: PASS (7 tests)

- [ ] **Step 5: 전체 테스트 확인**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL

---

## Task 9: 문서 갱신

`CLAUDE.md`의 `Broker Dispatcher Registration` 절이 삭제된 `DispatcherBeanRegistrar`와 옛 파일 포맷을 설명하고 있다. 문서 사이트의 `docs/index.html`·`docs/quickstart.html`도 중첩 host 포맷을 그대로 보여주는데, 이 포맷은 새 `DispatcherDefinition`에 바인딩되지 않아 그대로 따라 쓰면 브로커가 뜨지 못한다.

`docs/docs/0.0.2/broker.html`은 릴리스 스냅샷이라 손대지 않는다.

**Files:**
- Modify: `CLAUDE.md`
- Modify: `docs/index.html`
- Modify: `docs/quickstart.html`

- [ ] **Step 1: Broker Dispatcher Registration 절 교체**

`CLAUDE.md`의 `## Broker Dispatcher Registration` 절 전체를 아래로 교체:

````markdown
## Broker Dispatcher Registration

Dispatchers are defined in a JSON file at `{mmmq.broker.persistence.root-dir}/dispatchers.json` (root-dir default `./mmmq`); the path is fixed and not individually configurable. `DispatcherContainer` reads it at construction and owns every `Dispatcher` instance — Dispatchers are not Spring beans. The top level is an array; one entry maps to exactly one `consumerId` and one pattern (1 id = 1 Dispatcher = 1 HE).

```json
[
  {
    "consumerId": "order-created",
    "host": "http://consumer-host:8080",
    "pattern": "order.created"
  }
]
```

`host` is an absolute URL; the scheme must be `http` or `https` (case-insensitive) and the port is required — a consumer usually listens on a non-standard port, so a missing port is rejected instead of silently falling back to 80/443. When the file is absent, an empty `[]` file is created and the broker boots with no dispatchers. Invalid definitions — duplicate `consumerId`, unsupported scheme, malformed JSON — fail context startup (fail-fast). Each Dispatcher gets its own `<consumerId>.checkpoint` file under the per-topic storage directory.

### Runtime management

`DispatcherController` exposes CRUD over the same definitions. Changes are written to `dispatchers.json` (temp file + `ATOMIC_MOVE`) before the in-memory state is touched, so they survive a restart.

```
GET    /mmmq/dispatchers               200
POST   /mmmq/dispatchers               201 / 400 / 409
PUT    /mmmq/dispatchers/{consumerId}  200 / 400 / 404
DELETE /mmmq/dispatchers/{consumerId}  204 / 404
```

PUT takes only `host` and `pattern` in the body — `consumerId` is the identifier, not a mutable field. Mutations are serialized by a single `ReentrantLock` inside `DispatcherContainer`; the message hot path (`getSubscribers`) stays lock-free.

A new subscription starts at the log **tail**, so attaching a consumer at runtime does not replay the existing backlog. When a subscription ends — dispatcher deleted, or pattern narrowed so a topic drops out — its `<consumerId>.checkpoint` is deleted too.
````

- [ ] **Step 2: Message Flow 절의 Bootstrap 문장 갱신**

`## Message Flow` 절 끝의 Bootstrap 문단을 아래로 교체:

```markdown
Bootstrap: `DispatcherContainer` reads `dispatchers.json` in its constructor and creates every `Dispatcher`. `TopicQueueBootstrapper` (SmartInitializingSingleton) then restores persisted topic queues at startup and calls `TopicQueueContainer.getOrCreate` for each, which in turn calls `DispatcherContainer.register(queue)` to bind matched Dispatchers.
```

- [ ] **Step 3: Key Design Points의 관련 항목 갱신**

`- **Uniqueness on both sides:**` 항목의 후반부를 아래로 교체:

```markdown
- **Uniqueness on both sides:** Consumer rejects duplicate HE ids at registration (`HandlerExecutionContainer.add`). Broker rejects duplicate consumerIds when `DispatcherContainer` loads the file and on every runtime addition (`DuplicateConsumerIdException`).
```

- [ ] **Step 4: 문서 사이트의 dispatchers.json 예시 교체**

`docs/index.html`의 `dispatchers.json` 코드 블록에서 host 줄을 아래로 교체한다.

```html
  <span class="tk-str">"host"</span>: <span class="tk-str">"http://consumer-host:8080"</span>,
```

`docs/quickstart.html`의 `dispatchers.json` 코드 블록에서 host 줄을 아래로 교체한다(들여쓰기가 한 단 더 깊다).

```html
    <span class="tk-str">"host"</span>: <span class="tk-str">"http://localhost:8082"</span>,
```

- [ ] **Step 5: 문서가 코드와 맞는지 확인**

Run: `grep -n "DispatcherBeanRegistrar\|HostDefinition" CLAUDE.md; grep -n '"protocol"' docs/index.html docs/quickstart.html`
Expected: 두 grep 모두 출력 없음 (첫 grep이 빈 결과로 종료 코드 1을 내므로 `&&`로 잇지 않는다)

- [ ] **Step 6: 커밋**

```bash
git add CLAUDE.md docs/index.html docs/quickstart.html
git commit -F - <<'MSGEOF'
docs: Dispatcher 등록 설명을 새 구조에 맞게 갱신

- 삭제된 DispatcherBeanRegistrar와 옛 중첩 host 포맷 설명을 걷어내고 URL 문자열 포맷으로 교체.
- 런타임 관리 엔드포인트와 tail 시작·체크포인트 삭제 규칙을 추가.
- 그대로 따라 쓰면 브로커가 뜨지 못하는 문서 사이트의 dispatchers.json 예시도 함께 교체.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## 완료 확인

- [ ] `./gradlew build` — BUILD SUCCESSFUL
- [ ] `git status -s` — Task 1~8의 코드 변경이 커밋되지 않은 채 남아 있다 (문서 커밋 1개만 존재)
- [ ] `grep -rn "DispatcherBeanRegistrar\|HostDefinition" --include="*.java" broker core` — 출력 없음
- [ ] `grep -rn '"protocol"' CLAUDE.md docs/index.html docs/quickstart.html` — 출력 없음 (옛 중첩 host 포맷 잔존 확인)
