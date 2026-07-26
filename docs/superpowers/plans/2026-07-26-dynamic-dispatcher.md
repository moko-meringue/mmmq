# 런타임 Dispatcher 관리 API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 브로커가 도는 중에 HTTP API로 Dispatcher를 추가·수정·삭제·조회할 수 있게 하고, 그 변경이 `dispatchers.json`에 원자적으로 반영돼 재기동 후에도 유지되게 한다.

**Architecture:** `DispatcherContainer`가 Dispatcher의 소유자가 되어 `dispatchers.json`을 직접 읽고 쓴다. 뮤테이션(추가·수정·삭제·새 토픽 등록)은 컨테이너 내부의 `ReentrantLock` 하나로 직렬화하고, 메시지 핫패스인 `getSubscribers`는 `ConcurrentHashMap` 무락 읽기를 유지한다. 파일은 임시 파일에 전체를 쓰고 `ATOMIC_MOVE`로 교체한다. 새 구독은 로그 tail부터 시작하고, 구독이 끝나면 그 체크포인트 파일도 지운다.

**Tech Stack:** Java 17, Spring Boot 3.2, Gradle 멀티모듈, JUnit 5, AssertJ, Mockito, spring-test(MockMvc standalone), Jackson

**Spec:** `docs/superpowers/specs/2026-07-26-dynamic-dispatcher-design.md`

---

## 파일 구조

### 신규

| 경로 | 책임 |
|---|---|
| `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherFactory.java` | `DispatcherDefinition` → `Dispatcher` 변환과 입력 검증의 단일 지점 |
| `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherFile.java` | `dispatchers.json` 읽기 + 원자적 쓰기 |
| `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherRoute.java` | PUT 본문 (host·pattern) |
| `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherController.java` | 런타임 관리 REST 엔드포인트 4개 |
| `broker/src/main/java/org/mmmq/broker/dispatcher/DuplicateConsumerIdException.java` | 409 매핑용 |
| `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherNotFoundException.java` | 404 매핑용 |

### 수정

| 경로 | 변경 |
|---|---|
| `core/src/main/java/org/mmmq/core/WebProtocol.java` | 프로토콜별 기본 포트 |
| `core/src/main/java/org/mmmq/core/Host.java` | `InetAddress` → 원본 주소 문자열, `equals`/`hashCode` 전 필드 반영 |
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

### 삭제

- `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherBeanRegistrar.java`
- `broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java`
- `broker/src/test/java/org/mmmq/broker/dispatcher/HostDefinitionTest.java`

---

## Task 1: WebProtocol에 기본 포트 추가

URL에서 포트가 생략됐을 때 쓸 값이 필요하다. 기본 포트를 아는 주체는 프로토콜이다.

**Files:**
- Modify: `core/src/main/java/org/mmmq/core/WebProtocol.java`
- Create: `core/src/test/java/org/mmmq/core/WebProtocolTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`core/src/test/java/org/mmmq/core/WebProtocolTest.java`:

```java
package org.mmmq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WebProtocolTest {

    @Test
    @DisplayName("스킴 문자열은 대소문자를 가리지 않고 변환된다")
    void convertsSchemeCaseInsensitively() {
        assertThat(WebProtocol.from("HTTP")).isEqualTo(WebProtocol.HTTP);
        assertThat(WebProtocol.from("https")).isEqualTo(WebProtocol.HTTPS);
    }

    @Test
    @DisplayName("알 수 없는 스킴은 예외를 던진다")
    void rejectsUnknownScheme() {
        assertThatThrownBy(() -> WebProtocol.from("ftp"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("프로토콜마다 기본 포트를 안다")
    void knowsDefaultPort() {
        assertThat(WebProtocol.HTTP.getDefaultPort()).isEqualTo(80);
        assertThat(WebProtocol.HTTPS.getDefaultPort()).isEqualTo(443);
    }
}
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `./gradlew :core:test --tests "org.mmmq.core.WebProtocolTest"`
Expected: 컴파일 실패 — `cannot find symbol: method getDefaultPort()`

- [ ] **Step 3: 구현**

`core/src/main/java/org/mmmq/core/WebProtocol.java` 전체를 아래로 교체:

```java
package org.mmmq.core;

public enum WebProtocol {

    HTTP("http", 80),
    HTTPS("https", 443);

    private final String scheme;
    private final int defaultPort;

    WebProtocol(String scheme, int defaultPort) {
        this.scheme = scheme;
        this.defaultPort = defaultPort;
    }

    public static WebProtocol from(String scheme) {
        for (WebProtocol protocol : values()) {
            if (protocol.scheme.equalsIgnoreCase(scheme)) {
                return protocol;
            }
        }
        throw new IllegalArgumentException("Unknown scheme: " + scheme);
    }

    public String getScheme() {
        return scheme;
    }

    public int getDefaultPort() {
        return defaultPort;
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :core:test --tests "org.mmmq.core.WebProtocolTest"`
Expected: PASS (3 tests)

- [ ] **Step 5: 커밋**

```bash
git add core/src/main/java/org/mmmq/core/WebProtocol.java core/src/test/java/org/mmmq/core/WebProtocolTest.java
git commit -F - <<'MSGEOF'
feat: WebProtocol에 프로토콜별 기본 포트 추가

- URL에서 포트가 생략됐을 때 쓸 기본값을 프로토콜이 직접 알도록 추가.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## Task 2: Host가 원본 주소 문자열을 보존

`Host`가 생성자에서 `InetAddress.getByName()`으로 DNS를 즉시 해석하는 탓에, DNS에 아직 없는 소비자를 런타임에 등록할 수 없고 소비자 IP가 바뀌어도 재기동 전까지 옛 IP로 보낸다. 주소 문자열을 그대로 들고, 해석은 `Sender`의 `RestClient`가 요청할 때 하게 한다.

`equals`/`hashCode`가 지금 `address`만 비교해서 포트가 달라도 같다고 나온다. 비교 대상 필드를 손대는 김에 전 필드를 반영한다.

**Files:**
- Modify: `core/src/main/java/org/mmmq/core/Host.java`
- Modify: `core/src/test/java/org/mmmq/core/HostTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`core/src/test/java/org/mmmq/core/HostTest.java` 전체를 아래로 교체. 기존 `createWithUnknownHost`("invalid..host..name"이면 예외)는 DNS 해석을 없애면 성립하지 않으므로 사라지고, 그 자리를 형식 검증 케이스가 대신한다.

```java
package org.mmmq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HostTest {

    @Test
    @DisplayName("유효한 호스트 이름으로 Host를 생성할 수 있다.")
    void createWithValidHost() {
        assertThatCode(() -> new Host(WebProtocol.HTTP, "localhost", 8080))
                .doesNotThrowAnyException();
    }

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

    @Test
    @DisplayName("포트가 다르면 다른 Host로 취급한다.")
    void distinguishesByPort() {
        Host first = new Host(WebProtocol.HTTP, "localhost", 8080);
        Host second = new Host(WebProtocol.HTTP, "localhost", 9090);

        assertThat(first).isNotEqualTo(second);
    }
}
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `./gradlew :core:test --tests "org.mmmq.core.HostTest"`
Expected: FAIL — `keepsOriginalAddressInUri`가 `"https://consumer-host:8443"` 대신 해석된 IP를 받아 실패하거나 `UnknownHostException` 기반 `IllegalArgumentException`을 던진다. `rejectsBlankAddress`·`distinguishesByPort`도 실패.

- [ ] **Step 3: 구현**

`core/src/main/java/org/mmmq/core/Host.java` 전체를 아래로 교체:

```java
package org.mmmq.core;

import java.util.Objects;

public class Host {

    final WebProtocol protocol;
    final String address;
    final int port;

    public Host(WebProtocol webProtocol, String address, int port) {
        if (webProtocol == null) {
            throw new IllegalArgumentException("protocol must not be null");
        }
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

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Host host)) {
            return false;
        }
        return port == host.port
                && protocol == host.protocol
                && Objects.equals(address, host.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(protocol, address, port);
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :core:test --tests "org.mmmq.core.HostTest"`
Expected: PASS (5 tests)

- [ ] **Step 5: 전체 테스트로 회귀 확인**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL

`SenderTest`와 `GatewayTest`는 `RestClient`의 baseUrl과 `requestTo(...)` 양쪽에 같은 `host.toUri()`를 쓰기 때문에 `"http://127.0.0.1:8080"`이 `"http://localhost:8080"`으로 바뀌어도 함께 움직여 통과한다. `HostDefinitionTest`는 주소를 이미 `"127.0.0.1"`로 주고 있어 기대 문자열이 그대로 성립한다.

- [ ] **Step 6: 커밋**

```bash
git add core/src/main/java/org/mmmq/core/Host.java core/src/test/java/org/mmmq/core/HostTest.java
git commit -F - <<'MSGEOF'
refactor: Host가 원본 주소 문자열을 보존하도록 변경

- 생성자의 DNS 즉시 해석 때문에 아직 뜨지 않은 소비자를 등록할 수 없었던 문제를 해결.
- 이름 해석을 요청 시점으로 미뤄 소비자 IP 변경도 재기동 없이 따라가게 함.
- 포트가 달라도 같다고 판정하던 equals·hashCode를 전 필드 기준으로 수정.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## Task 3: SegmentFileChain.tailOffset()

새 구독을 로그 끝에서 시작시키려면 다음에 쓰일 절대 오프셋을 알아야 한다. `append`가 로테이션할 때 쓰던 계산식과 같은 값이라 그쪽도 이 메서드를 쓴다.

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/topicqueue/storage/SegmentFileChain.java`
- Modify: `broker/src/test/java/org/mmmq/broker/topicqueue/storage/SegmentFileChainTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`SegmentFileChainTest`의 마지막 `@Test` 뒤에 아래 3개를 추가한다. 필요한 import(`Message`, `Topic`, `Map`, `TempDir`, `assertThat`)와 `DEFAULT_MAX_BYTES` 상수는 이미 파일에 있다.

```java
    @Test
    @DisplayName("빈 체인의 tailOffset은 0이다")
    void tailOffsetOfEmptyChainIsZero(@TempDir Path tempDir) {
        try (SegmentFileChain chain = SegmentFileChain.open(tempDir, DEFAULT_MAX_BYTES)) {
            assertThat(chain.tailOffset()).isZero();
        }
    }

    @Test
    @DisplayName("append한 개수만큼 tailOffset이 증가한다")
    void tailOffsetAdvancesWithAppends(@TempDir Path tempDir) {
        try (SegmentFileChain chain = SegmentFileChain.open(tempDir, DEFAULT_MAX_BYTES)) {
            chain.append(new Message(new Topic("topic"), Map.of("seq", 1)));
            chain.append(new Message(new Topic("topic"), Map.of("seq", 2)));

            assertThat(chain.tailOffset()).isEqualTo(2L);
        }
    }

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

- [ ] **Step 5: 커밋**

```bash
git add broker/src/main/java/org/mmmq/broker/topicqueue/storage/SegmentFileChain.java broker/src/test/java/org/mmmq/broker/topicqueue/storage/SegmentFileChainTest.java
git commit -F - <<'MSGEOF'
feat: SegmentFileChain에 tailOffset 추가

- 새 구독을 로그 끝에서 시작시키기 위해 다음에 쓰일 절대 오프셋을 노출.
- 로테이션 시 쓰던 동일한 계산식을 이 메서드로 통일.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## Task 4: 체크포인트 삭제 경로

Dispatcher가 사라졌는데 그 `consumerId`의 읽기 위치가 디스크에 남으면 아무도 소유하지 않는 상태가 된다. 파일을 지우는 경로를 만든다.

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/topicqueue/storage/CheckpointFile.java`
- Modify: `broker/src/main/java/org/mmmq/broker/topicqueue/storage/CheckpointDirectory.java`
- Modify: `broker/src/test/java/org/mmmq/broker/topicqueue/storage/CheckpointFileTest.java`
- Create: `broker/src/test/java/org/mmmq/broker/topicqueue/storage/CheckpointDirectoryTest.java`

- [ ] **Step 1: 실패하는 테스트 작성 (CheckpointFile)**

`CheckpointFileTest`의 마지막 `@Test` 뒤에 추가:

```java
    @Test
    @DisplayName("delete는 핸들을 닫고 파일을 지운다")
    void deleteRemovesFile(@TempDir Path tempDir) {
        CheckpointFile store = CheckpointFile.open(tempDir, "dispatcher-a");
        store.write(7L);

        store.delete();

        assertThat(tempDir.resolve("dispatcher-a.checkpoint")).doesNotExist();
    }
```

- [ ] **Step 2: 실패하는 테스트 작성 (CheckpointDirectory)**

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
    @DisplayName("register한 체크포인트는 get으로 찾을 수 있다")
    void registersCheckpoint(@TempDir Path tempDir) {
        CheckpointDirectory directory = CheckpointDirectory.open(tempDir);

        directory.register("dispatcher-a");

        assertThat(directory.get("dispatcher-a")).isNotNull();
        directory.close();
    }

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
    @DisplayName("deregister 후 close해도 이미 닫힌 파일을 다시 닫지 않는다")
    void closeAfterDeregisterDoesNotFail(@TempDir Path tempDir) {
        CheckpointDirectory directory = CheckpointDirectory.open(tempDir);
        directory.register("dispatcher-a");
        directory.deregister("dispatcher-a");

        assertThatCode(directory::close).doesNotThrowAnyException();
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

- [ ] **Step 3: 테스트가 실패하는지 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.topicqueue.storage.CheckpointFileTest" --tests "org.mmmq.broker.topicqueue.storage.CheckpointDirectoryTest"`
Expected: 컴파일 실패 — `cannot find symbol: method delete()`, `cannot find symbol: method deregister(String)`

- [ ] **Step 4: CheckpointFile 구현**

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

- [ ] **Step 5: CheckpointDirectory 구현**

`CheckpointDirectory.java`의 `get` 메서드 뒤, `close` 앞에 추가:

```java
    public void deregister(String name) {
        CheckpointFile checkpointFile = checkpoints.remove(name);
        if (checkpointFile != null) {
            checkpointFile.delete();
        }
    }
```

맵에서 먼저 빼기 때문에 뒤이은 `close()`가 이미 닫힌 파일을 다시 닫지 않는다.

- [ ] **Step 6: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.topicqueue.storage.CheckpointFileTest" --tests "org.mmmq.broker.topicqueue.storage.CheckpointDirectoryTest"`
Expected: PASS (CheckpointFileTest 4개, CheckpointDirectoryTest 4개)

- [ ] **Step 7: 커밋**

```bash
git add broker/src/main/java/org/mmmq/broker/topicqueue/storage/CheckpointFile.java broker/src/main/java/org/mmmq/broker/topicqueue/storage/CheckpointDirectory.java broker/src/test/java/org/mmmq/broker/topicqueue/storage/CheckpointFileTest.java broker/src/test/java/org/mmmq/broker/topicqueue/storage/CheckpointDirectoryTest.java
git commit -F - <<'MSGEOF'
feat: 체크포인트 삭제 경로 추가

- Dispatcher가 사라진 뒤에도 읽기 위치 파일이 남아 아무도 소유하지 않는 상태를 없애기 위해 추가.
- CheckpointFile은 자기 파일을 스스로 지우고, CheckpointDirectory는 맵에서 먼저 빼 이중 close를 막음.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## Task 5: TopicQueue의 구독 시작점과 구독 해제

신규 구독은 tail부터 시작한다. 이미 체크포인트가 있으면 손대지 않으므로 재기동은 영향이 없다. `register`가 `computeIfAbsent`라 새로 만든 건지 알 수 없어서 `get`이 `null`인지로 신규를 판별한다.

**이 태스크는 기존 `TopicQueueTest` 5개를 깨뜨린다.** 모두 "offer 먼저, subscribe 나중" 순서라 tail이 0이 아니게 된다. 순서를 뒤집는 것이 새 의미론에 맞는 표현이므로 테스트를 그렇게 고친다.

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueue.java`
- Modify: `broker/src/test/java/org/mmmq/broker/topicqueue/TopicQueueTest.java`

- [ ] **Step 1: 기존 테스트 5개를 새 의미론에 맞게 고치고 신규 3개 추가**

`broker/src/test/java/org/mmmq/broker/topicqueue/TopicQueueTest.java` 전체를 아래로 교체. 지역변수의 `final`은 프로젝트 규칙에 따라 걷어냈다.

```java
package org.mmmq.broker.topicqueue;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class TopicQueueTest {

    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;
    private static final String CHECKPOINTS_DIR = "checkpoints";

    @Test
    @DisplayName("offer 성공 시 true 반환")
    void offerReturnsTrueOnSuccess(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        Message message = new Message(new Topic("topic"), Map.of("k", "v"));

        assertThat(queue.offer(message)).isTrue();
    }

    @Test
    @DisplayName("subscribe 후 peek은 첫 메시지를 반환한다")
    void peekReturnsFirstMessage(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        Message message = new Message(new Topic("topic"), Map.of("k", "v"));

        Offset offset = queue.subscribe("dispatcher-1");
        queue.offer(message);
        Message peeked = queue.peek(offset);

        assertThat(peeked).isEqualTo(message);
    }

    @Test
    @DisplayName("commit 없이 peek만 반복하면 같은 메시지가 반환된다 (at-least-once)")
    void peekWithoutCommitReturnsSameMessage(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        Message message = new Message(new Topic("topic"), Map.of("k", "v"));

        Offset offset = queue.subscribe("dispatcher-1");
        queue.offer(message);
        Message first = queue.peek(offset);
        Message second = queue.peek(offset);

        assertThat(first).isEqualTo(second);
    }

    @Test
    @DisplayName("commit 후 peek은 다음 메시지로 이동한다")
    void commitAdvancesOffset(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        Message first = new Message(new Topic("topic"), Map.of("seq", 1));
        Message second = new Message(new Topic("topic"), Map.of("seq", 2));

        Offset offset = queue.subscribe("dispatcher-1");
        queue.offer(first);
        queue.offer(second);

        assertThat(queue.peek(offset)).isEqualTo(first);
        offset = queue.commit("dispatcher-1", offset);
        assertThat(queue.peek(offset)).isEqualTo(second);
    }

    @Test
    @DisplayName("재시작 후 subscribe는 마지막 commit 위치부터 재개된다")
    void resumesFromCommittedOffsetAfterRestart(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        Message first = new Message(new Topic("topic"), Map.of("seq", 1));
        Message second = new Message(new Topic("topic"), Map.of("seq", 2));

        Offset offset = queue.subscribe("dispatcher-1");
        queue.offer(first);
        queue.offer(second);
        queue.peek(offset);
        queue.commit("dispatcher-1", offset);

        TopicQueue restarted = createQueue(tempDir, "topic");
        Offset restoredOffset = restarted.subscribe("dispatcher-1");

        assertThat(restoredOffset.value()).isEqualTo(1L);
        assertThat(restarted.peek(restoredOffset)).isEqualTo(second);
    }

    @Test
    @DisplayName("commit 전 재시작 시 같은 메시지가 다시 peek된다")
    void redeliversAfterCrashBeforeCommit(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        Message message = new Message(new Topic("topic"), Map.of("k", "v"));

        Offset offset = queue.subscribe("dispatcher-1");
        queue.offer(message);
        queue.peek(offset);

        TopicQueue restarted = createQueue(tempDir, "topic");
        Offset restoredOffset = restarted.subscribe("dispatcher-1");

        assertThat(restoredOffset.value()).isZero();
        assertThat(restarted.peek(restoredOffset)).isEqualTo(message);
    }

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
    @DisplayName("체크포인트가 이미 있으면 tail이 아니라 저장된 오프셋을 쓴다")
    void existingCheckpointWinsOverTail(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        Offset offset = queue.subscribe("dispatcher-1");
        queue.offer(new Message(new Topic("topic"), Map.of("seq", 1)));
        queue.offer(new Message(new Topic("topic"), Map.of("seq", 2)));
        queue.commit("dispatcher-1", offset);

        Offset resubscribed = queue.subscribe("dispatcher-1");

        assertThat(resubscribed.value()).isEqualTo(1L);
    }

    @Test
    @DisplayName("unsubscribe하면 체크포인트가 지워지고 다시 subscribe하면 tail부터 시작한다")
    void unsubscribeRemovesCheckpoint(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        Offset offset = queue.subscribe("dispatcher-1");
        queue.offer(new Message(new Topic("topic"), Map.of("seq", 1)));
        queue.commit("dispatcher-1", offset);

        queue.unsubscribe("dispatcher-1");

        assertThat(tempDir.resolve("topic").resolve(CHECKPOINTS_DIR).resolve("dispatcher-1.checkpoint"))
                .doesNotExist();

        queue.offer(new Message(new Topic("topic"), Map.of("seq", 2)));
        Offset resubscribed = queue.subscribe("dispatcher-1");

        assertThat(resubscribed.value()).isEqualTo(2L);
    }

    private TopicQueue createQueue(Path baseDir, String topicName) {
        Path topicDir = baseDir.resolve(topicName);
        try {
            Files.createDirectories(topicDir);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, DEFAULT_MAX_BYTES);
        CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(topicDir);

        return new TopicQueue(new Topic(topicName), segmentFileChain, checkpointDirectory);
    }
}
```

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
Expected: PASS (9 tests)

- [ ] **Step 5: 전체 테스트로 회귀 확인**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL

`DispatcherTest`의 drain 관련 테스트는 모두 `subscribe`를 `offer`보다 먼저 호출하므로 tail이 0이라 영향이 없다.

- [ ] **Step 6: 커밋**

```bash
git add broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueue.java broker/src/test/java/org/mmmq/broker/topicqueue/TopicQueueTest.java
git commit -F - <<'MSGEOF'
feat: 신규 구독을 로그 tail부터 시작하고 구독 해제를 추가

- 런타임에 붙은 소비자가 기존 백로그를 전부 받아버리는 문제를 막기 위해 신규 구독을 tail로 초기화.
- 기존 체크포인트가 있으면 손대지 않으므로 재기동 동작은 그대로 유지.
- 구독이 끝날 때 체크포인트를 지우는 unsubscribe를 추가하고, 삭제 실패는 로그만 남기게 함.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## Task 6: Dispatcher의 host·pattern 접근자

파일에 쓸 정의를 `Dispatcher`에서 복원하려면 두 값을 읽을 수 있어야 한다. 필드를 밖에서 직접 읽지 않도록 접근자를 둔다.

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/dispatcher/Dispatcher.java`
- Modify: `broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`DispatcherTest`의 `consumerIdGetter` 테스트 뒤에 추가:

```java
    @Test
    @DisplayName("host와 pattern 접근자가 생성자 인자를 그대로 반환한다")
    void exposesHostAndPattern() {
        Dispatcher dispatcher = new Dispatcher(host, new ConsumerId("order-dispatcher"), new TopicPattern("order.*"));

        assertThat(dispatcher.host()).isEqualTo(host);
        assertThat(dispatcher.pattern()).isEqualTo(new TopicPattern("order.*"));
    }
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherTest"`
Expected: 컴파일 실패 — `cannot find symbol: method host()`

- [ ] **Step 3: 구현**

`Dispatcher.java`의 `consumerId()` 메서드 뒤에 추가:

```java
    public Host host() {
        return host;
    }

    public TopicPattern pattern() {
        return pattern;
    }
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherTest"`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add broker/src/main/java/org/mmmq/broker/dispatcher/Dispatcher.java broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherTest.java
git commit -F - <<'MSGEOF'
feat: Dispatcher에 host·pattern 접근자 추가

- 파일에 쓸 정의를 Dispatcher에서 복원할 수 있도록 두 값을 노출.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## Task 7: 정의를 URL 문자열 포맷으로 바꾸고 DispatcherFactory 도입

파일과 API가 같은 모양을 쓰도록 host를 URL 문자열 하나로 합친다. 문자열 → `Host`·`ConsumerId`·`TopicPattern` 변환과 검증은 `DispatcherFactory` 한 곳으로 모으고, 역방향은 `DispatcherDefinition.from(Dispatcher)`가 맡는다. `Host`가 주소 문자열을 보존하게 됐으므로 이 왕복은 무손실이다.

`DispatcherBeanRegistrar`는 Task 10에서 삭제되지만 그 전까지 컴파일과 테스트가 통과해야 하므로, 빈 정의를 공급자(supplier) 방식으로 바꿔 팩토리에 위임한다.

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherDefinition.java`
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherFactory.java`
- Modify: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherBeanRegistrar.java`
- Create: `broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherFactoryTest.java`
- Create: `broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherDefinitionTest.java`
- Modify: `broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java`
- Delete: `broker/src/test/java/org/mmmq/broker/dispatcher/HostDefinitionTest.java`

- [ ] **Step 1: 실패하는 테스트 작성 (DispatcherFactory)**

`broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherFactoryTest.java`:

```java
package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;

class DispatcherFactoryTest {

    DispatcherFactory factory = new DispatcherFactory();

    @Test
    @DisplayName("URL 문자열을 Host로 파싱한다")
    void parsesUrlIntoHost() {
        Dispatcher dispatcher = factory.create(
                new DispatcherDefinition("order-created", "https://consumer-host:8443", "order.created"));

        assertThat(dispatcher.host()).isEqualTo(new Host(WebProtocol.HTTPS, "consumer-host", 8443));
        assertThat(dispatcher.consumerId()).isEqualTo(new ConsumerId("order-created"));
        assertThat(dispatcher.pattern()).isEqualTo(new TopicPattern("order.created"));
    }

    @Test
    @DisplayName("포트를 생략하면 스킴의 기본 포트를 쓴다")
    void fallsBackToDefaultPort() {
        Dispatcher http = factory.create(new DispatcherDefinition("a", "http://consumer-host", "**"));
        Dispatcher https = factory.create(new DispatcherDefinition("b", "https://consumer-host", "**"));

        assertThat(http.host().toUri()).isEqualTo("http://consumer-host:80");
        assertThat(https.host().toUri()).isEqualTo("https://consumer-host:443");
    }

    @Test
    @DisplayName("스킴이 없는 문자열은 예외를 던진다")
    void rejectsRelativeUrl() {
        assertThatThrownBy(() -> factory.create(new DispatcherDefinition("a", "consumer-host:8080", "**")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("미지원 스킴은 예외를 던진다")
    void rejectsUnsupportedScheme() {
        assertThatThrownBy(() -> factory.create(new DispatcherDefinition("a", "ftp://consumer-host:21", "**")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("host가 비어 있으면 예외를 던진다")
    void rejectsBlankHost() {
        assertThatThrownBy(() -> factory.create(new DispatcherDefinition("a", null, "**")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> factory.create(new DispatcherDefinition("a", "  ", "**")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("pattern이 비어 있으면 예외를 던진다")
    void rejectsBlankPattern() {
        assertThatThrownBy(() -> factory.create(new DispatcherDefinition("a", "http://consumer-host:8080", null)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> factory.create(new DispatcherDefinition("a", "http://consumer-host:8080", " ")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("consumerId가 regex에 어긋나면 예외를 던진다")
    void rejectsInvalidConsumerId() {
        assertThatThrownBy(() ->
                factory.create(new DispatcherDefinition("invalid id!", "http://consumer-host:8080", "**")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
```

- [ ] **Step 2: 실패하는 테스트 작성 (DispatcherDefinition 왕복)**

`broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherDefinitionTest.java`:

```java
package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DispatcherDefinitionTest {

    DispatcherFactory factory = new DispatcherFactory();

    @Test
    @DisplayName("정의로 만든 Dispatcher에서 같은 정의를 복원한다")
    void roundTripsThroughDispatcher() {
        DispatcherDefinition definition =
                new DispatcherDefinition("order-created", "https://consumer-host:8443", "order.created");

        DispatcherDefinition restored = DispatcherDefinition.from(factory.create(definition));

        assertThat(restored).isEqualTo(definition);
    }

    @Test
    @DisplayName("호스트 이름은 IP로 바뀌지 않고 그대로 복원된다")
    void keepsHostName() {
        DispatcherDefinition definition = new DispatcherDefinition("a", "http://localhost:8080", "**");

        DispatcherDefinition restored = DispatcherDefinition.from(factory.create(definition));

        assertThat(restored.host()).isEqualTo("http://localhost:8080");
    }

    @Test
    @DisplayName("포트를 생략한 입력은 기본 포트가 채워진 형태로 복원된다")
    void normalizesOmittedPort() {
        DispatcherDefinition definition = new DispatcherDefinition("a", "http://consumer-host", "**");

        DispatcherDefinition restored = DispatcherDefinition.from(factory.create(definition));

        assertThat(restored.host()).isEqualTo("http://consumer-host:80");
    }
}
```

- [ ] **Step 3: 테스트가 실패하는지 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherFactoryTest" --tests "org.mmmq.broker.dispatcher.DispatcherDefinitionTest"`
Expected: 컴파일 실패 — `DispatcherFactory` 클래스 없음, `DispatcherDefinition` 생성자가 `(String, HostDefinition, String)`이라 인자 타입 불일치

- [ ] **Step 4: DispatcherDefinition 교체**

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

- [ ] **Step 5: DispatcherFactory 작성**

`broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherFactory.java`:

```java
package org.mmmq.broker.dispatcher;

import java.net.URI;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;
import org.springframework.stereotype.Component;

@Component
public class DispatcherFactory {

    public Dispatcher create(DispatcherDefinition definition) {
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
}
```

`URI.create("consumer-host:8080")`은 예외를 던지지 않고 scheme만 채워진 URI를 만들고, 밑줄이 든 호스트명은 `getHost()`가 `null`이다. 그래서 scheme·host를 명시적으로 확인해야 한다.

- [ ] **Step 6: DispatcherBeanRegistrar를 팩토리에 위임 (임시, Task 10에서 삭제)**

`DispatcherBeanRegistrar.java`의 `register` 메서드를 아래로 교체하고, 클래스 상단 `log` 상수 뒤에 팩토리 상수를 추가한다.

```java
    private static final DispatcherFactory FACTORY = new DispatcherFactory();
```

```java
    private void register(DispatcherDefinition definition, BeanDefinitionRegistry registry) {
        AbstractBeanDefinition beanDefinition = BeanDefinitionBuilder
                .genericBeanDefinition(Dispatcher.class, () -> FACTORY.create(definition))
                .getBeanDefinition();
        BeanDefinitionReaderUtils.registerWithGeneratedName(beanDefinition, registry);
    }
```

`ConsumerId`·`TopicPattern` import가 더 이상 쓰이지 않으므로 제거한다. 검증은 빈 생성 시점에 일어나므로 컨텍스트 기동 실패(fail-fast)는 유지된다.

- [ ] **Step 7: 레지스트라 테스트의 JSON 픽스처를 새 포맷으로 갱신**

`broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java`에서 `write(...)`에 넘기는 JSON 5곳을 새 포맷으로 바꾼다.

`registersDispatchersFromFile`:

```java
        write("""
                [
                  {"consumerId":"order-created","host":"http://127.0.0.1:8080","pattern":"order.created"},
                  {"consumerId":"order-shipped","host":"http://127.0.0.1:8080","pattern":"order.shipped"}
                ]
                """);
```

`failsOnDuplicateConsumerId`:

```java
        write("""
                [
                  {"consumerId":"dup","host":"http://127.0.0.1:8080","pattern":"a"},
                  {"consumerId":"dup","host":"http://127.0.0.1:8080","pattern":"b"}
                ]
                """);
```

`failsOnInvalidConsumerId`:

```java
        write("""
                [
                  {"consumerId":"invalid id!","host":"http://127.0.0.1:8080","pattern":"a"}
                ]
                """);
```

`failsOnUnknownProtocol`:

```java
        write("""
                [
                  {"consumerId":"x","host":"ftp://127.0.0.1:8080","pattern":"a"}
                ]
                """);
```

`failsOnEmptyFile`은 `write("")` 그대로 둔다.

- [ ] **Step 8: HostDefinitionTest 삭제**

```bash
git rm broker/src/test/java/org/mmmq/broker/dispatcher/HostDefinitionTest.java
```

- [ ] **Step 9: 테스트 통과 확인**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL

- [ ] **Step 10: 커밋**

```bash
git add broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherDefinition.java broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherFactory.java broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherBeanRegistrar.java broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherFactoryTest.java broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherDefinitionTest.java broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java
git commit -F - <<'MSGEOF'
refactor: Dispatcher 정의의 host를 URL 문자열로 통일하고 DispatcherFactory 도입

- 파일과 API가 같은 모양을 쓰도록 중첩 HostDefinition을 URL 문자열 하나로 합침.
- 문자열에서 Host·ConsumerId·TopicPattern으로 가는 변환과 검증을 팩토리 한 곳으로 모음.
- Host가 주소 문자열을 보존하므로 Dispatcher에서 정의를 무손실로 복원하도록 from()을 추가.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## Task 8: DispatcherFile

`dispatchers.json`을 읽고, 임시 파일에 전체를 쓴 뒤 `ATOMIC_MOVE`로 교체한다. 같은 파일시스템이라 이동이 원자적이고, 반쯤 쓰인 JSON이 최종 경로에 보일 수 없다.

`ObjectMapper`는 주입받지 않고 클래스 상수로 만든다. broker는 라이브러리라 호스트 애플리케이션의 `ObjectMapper` 커스터마이징에 파일 포맷이 휘둘리면 안 된다.

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
    @DisplayName("파일이 없으면 빈 배열 파일을 만들고 빈 목록을 반환한다")
    void createsEmptyFileWhenMissing() {
        assertThat(dispatcherFile.read()).isEmpty();

        assertThat(tempDir.resolve(FILE_NAME)).exists();
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
    @DisplayName("쓰기 후 임시 파일이 남지 않는다")
    void leavesNoTempFile() {
        dispatcherFile.write(List.of(
                new DispatcherDefinition("order-created", "http://consumer-host:8080", "order.created")));

        assertThat(tempDir.resolve(FILE_NAME + ".tmp")).doesNotExist();
    }

    @Test
    @DisplayName("root-dir 디렉터리가 없으면 만들고 쓴다")
    void createsRootDirWhenMissing() {
        Path absentRoot = tempDir.resolve("absent-root");
        DispatcherFile file = new DispatcherFile(new PersistenceProperties(absentRoot.toString(), null));

        file.write(List.of(new DispatcherDefinition("x", "http://consumer-host:8080", "a")));

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
            Files.createDirectories(path.toAbsolutePath().getParent());
            Files.write(temp, OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(definitions));
            Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to write dispatcher file: " + path, exception);
        }
    }
}
```

`JsonProcessingException`은 `IOException`의 하위 타입이라 catch 하나로 충분하다.

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherFileTest"`
Expected: PASS (5 tests)

- [ ] **Step 5: 커밋**

```bash
git add broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherFile.java broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherFileTest.java
git commit -F - <<'MSGEOF'
feat: dispatchers.json 읽기·원자적 쓰기를 담당하는 DispatcherFile 추가

- 런타임 변경 중 프로세스가 죽어도 설정 파일이 깨지지 않도록 임시 파일 + ATOMIC_MOVE로 교체.
- 호스트 애플리케이션 설정에 파일 포맷이 휘둘리지 않도록 ObjectMapper를 자체 보유.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## Task 9: DispatcherContainer 가변화

컨테이너에 추가·수정·삭제·조회를 넣는다. 뮤테이션은 `ReentrantLock` 하나로 직렬화하고, 핫패스 `getSubscribers`는 무락 읽기를 유지한다. 순서는 언제나 **검증 → 파일 → 메모리**다.

이 태스크에서 `Dispatcher`의 `@PreDestroy`를 컨테이너로 옮긴다. 런타임에 추가된 Dispatcher는 스프링 빈이 아니라 `@PreDestroy`가 걸리지 않으므로, 컨테이너가 생명주기를 책임져야 한다.

생성자는 이 태스크에서 `Collection<Dispatcher>`를 유지한다(Task 10에서 교체). 그래서 중간 상태에서도 빌드와 테스트가 통과한다.

**저장 시 정규화:** 파일에는 `DispatcherDefinition.from(dispatcher)`로 복원한 값을 쓴다. 포트를 생략한 입력(`http://consumer-host`)이 기본 포트가 채워진 형태(`http://consumer-host:80`)로 저장되고, API 응답도 실제 등록된 값을 돌려준다.

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherContainer.java`
- Modify: `broker/src/main/java/org/mmmq/broker/dispatcher/Dispatcher.java`
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherRoute.java`
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DuplicateConsumerIdException.java`
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherNotFoundException.java`
- Modify: `broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java`
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
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
    private static final String HOST = "http://consumer-host:8080";

    @TempDir
    Path tempDir;

    DispatcherFactory factory;
    DispatcherFile file;
    TopicQueueFactory topicQueueFactory;
    DispatcherContainer container;

    @BeforeEach
    void setUp() {
        PersistenceProperties properties = new PersistenceProperties(tempDir.toString(), null);
        factory = new DispatcherFactory();
        file = new DispatcherFile(properties);
        topicQueueFactory = new TopicQueueFactory(properties);
        container = new DispatcherContainer(List.of(), factory, file);
    }

    @AfterEach
    void tearDown() {
        container.destroy();
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
    @DisplayName("이미 쌓인 큐에 추가하면 tail부터 시작한다")
    void addStartsAtTail() {
        TopicQueue queue = register(new Topic("order.created"));
        queue.offer(new Message(new Topic("order.created"), Map.of("seq", 1)));
        queue.offer(new Message(new Topic("order.created"), Map.of("seq", 2)));

        container.add(new DispatcherDefinition("order-created", HOST, "order.*"));

        assertThat(queue.subscribe("order-created").value()).isEqualTo(2L);
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
    @DisplayName("검증에 실패하면 파일이 바뀌지 않는다")
    void invalidDefinitionLeavesFileUntouched() {
        container.add(new DispatcherDefinition("order-created", HOST, "order.*"));

        assertThatThrownBy(() -> container.add(new DispatcherDefinition("other", "not-a-url", "**")))
                .isInstanceOf(IllegalArgumentException.class);
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
    @DisplayName("definitions는 등록 순서대로 현재 정의를 돌려준다")
    void definitionsReflectRegistrationOrder() {
        container.add(new DispatcherDefinition("first", HOST, "a.*"));
        container.add(new DispatcherDefinition("second", HOST, "b.*"));

        assertThat(container.definitions())
                .containsExactly(
                        new DispatcherDefinition("first", HOST, "a.*"),
                        new DispatcherDefinition("second", HOST, "b.*"));
    }

    @Test
    @DisplayName("register와 add를 동시에 호출해도 최종 상태가 일관된다")
    void concurrentRegisterAndAddStayConsistent() throws InterruptedException {
        int count = 8;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(count * 2);
        Queue<TopicQueue> queues = new ConcurrentLinkedQueue<>();
        ExecutorService executor = Executors.newFixedThreadPool(count * 2);

        for (int index = 0; index < count; index++) {
            String suffix = String.valueOf(index);
            executor.submit(() -> {
                await(start);
                TopicQueue queue = topicQueueFactory.create(new Topic("topic-" + suffix));
                queues.add(queue);
                container.register(queue);
                done.countDown();
            });
            executor.submit(() -> {
                await(start);
                container.add(new DispatcherDefinition("consumer-" + suffix, HOST, "**"));
                done.countDown();
            });
        }
        start.countDown();

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdownNow();

        assertThat(container.definitions()).hasSize(count);
        assertThat(file.read()).hasSize(count);
        assertThat(queues).hasSize(count);
        queues.forEach(queue -> assertThat(container.getSubscribers(queue)).hasSize(count));
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

    private void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }
}
```

- [ ] **Step 3: 테스트가 실패하는지 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherContainerTest"`
Expected: 컴파일 실패 — `DispatcherContainer` 생성자 인자 개수 불일치, `add`/`modify`/`remove`/`definitions`/`destroy` 없음

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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.core.identifier.ConsumerId;
import org.springframework.stereotype.Component;

@Component
public class DispatcherContainer {

    private final DispatcherFactory factory;
    private final DispatcherFile file;
    private final Map<ConsumerId, Dispatcher> dispatchers = new LinkedHashMap<>();
    private final Map<TopicQueue, List<Dispatcher>> subscriptions = new ConcurrentHashMap<>();
    private final ReentrantLock mutationLock = new ReentrantLock();

    public DispatcherContainer(Collection<Dispatcher> dispatchers, DispatcherFactory factory, DispatcherFile file) {
        this.factory = factory;
        this.file = file;
        dispatchers.forEach(dispatcher -> {
            if (this.dispatchers.putIfAbsent(dispatcher.consumerId(), dispatcher) != null) {
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
            Dispatcher dispatcher = factory.create(definition);
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
            Dispatcher next = factory.create(
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
}
```

구독을 잃은 짝을 찾는 비교가 `ConsumerId` 기준인 이유: `Dispatcher`는 `equals`가 없어 객체 동일성으로 비교되는데 `modify`는 새 인스턴스로 교체하므로, 객체로 비교하면 호스트만 바꾼 수정에서도 모든 토픽의 체크포인트가 지워진다. `modifyHostKeepsCheckpoint` 테스트가 이 실수를 잡는다.

`destroy()`는 워커만 종료하고 체크포인트는 건드리지 않는다. 애플리케이션 종료는 구독 해제가 아니다.

**검증하지 않는 것:** 수정·삭제 시 옛 Dispatcher의 워커가 실제로 종료됐는지는 테스트로 확인하지 않는다. 인스턴스를 팩토리가 만들기 때문에 스파이를 끼울 수 없고, `ThreadPoolExecutor`의 종료 여부를 밖에서 관찰할 통로도 없다. `previous.destroy()`·`dispatcher.destroy()` 호출 경로는 코드 리뷰로 확인한다.

- [ ] **Step 6: 레지스트라 테스트의 컨테이너 생성 부분 갱신**

`DispatcherBeanRegistrarTest`의 `TestConfig`를 아래로 교체한다. 새 생성자 시그니처에 맞추고, 테스트용 `PersistenceProperties`는 컨텍스트가 바인딩한 값을 그대로 쓴다.

```java
    @Configuration
    @EnableConfigurationProperties(PersistenceProperties.class)
    @Import(DispatcherBeanRegistrar.class)
    static class TestConfig {

        @Bean
        DispatcherFactory dispatcherFactory() {
            return new DispatcherFactory();
        }

        @Bean
        DispatcherFile dispatcherFile(PersistenceProperties properties) {
            return new DispatcherFile(properties);
        }

        @Bean
        DispatcherContainer dispatcherContainer(
                Collection<Dispatcher> dispatchers,
                DispatcherFactory factory,
                DispatcherFile file) {
            return new DispatcherContainer(dispatchers, factory, file);
        }
    }
```

추가 import:

```java
import org.mmmq.broker.dispatcher.DispatcherFactory;
import org.mmmq.broker.dispatcher.DispatcherFile;
import org.mmmq.broker.persistence.PersistenceProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
```

- [ ] **Step 7: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherContainerTest"`
Expected: PASS (11 tests)

- [ ] **Step 8: 전체 테스트로 회귀 확인**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL

- [ ] **Step 9: 커밋**

```bash
git add broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherContainer.java broker/src/main/java/org/mmmq/broker/dispatcher/Dispatcher.java broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherRoute.java broker/src/main/java/org/mmmq/broker/dispatcher/DuplicateConsumerIdException.java broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherNotFoundException.java broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherContainerTest.java broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java
git commit -F - <<'MSGEOF'
feat: DispatcherContainer에 추가·수정·삭제·조회를 추가

- 뮤테이션을 ReentrantLock 하나로 직렬화하고 핫패스 getSubscribers는 무락 읽기를 유지.
- 검증 → 파일 → 메모리 순서로 처리해 검증 실패 시 아무것도 바뀌지 않게 함.
- 구독을 잃은 짝은 ConsumerId 기준으로 찾아 체크포인트까지 정리.
- 런타임 추가분에는 @PreDestroy가 걸리지 않으므로 생명주기 책임을 컨테이너로 이동.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## Task 10: Dispatcher 소유권을 컨테이너로 이전

`DispatcherContainer`가 파일을 직접 읽어 Dispatcher를 만든다. 그러면 Dispatcher가 태어나는 길이 하나가 되고, `ImportBeanDefinitionRegistrar`가 `@ConfigurationProperties` 바인딩보다 먼저 도는 탓에 필요했던 `PersistenceProperties.bind(Environment)` 우회도 사라진다.

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherContainer.java`
- Modify: `broker/src/main/java/org/mmmq/broker/BrokerConfiguration.java`
- Modify: `broker/src/main/java/org/mmmq/broker/persistence/PersistenceProperties.java`
- Modify: `broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherContainerTest.java`
- Delete: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherBeanRegistrar.java`
- Delete: `broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java`

- [ ] **Step 1: 부트스트랩 테스트를 컨테이너 테스트로 이관**

`DispatcherContainerTest`의 `setUp`에서 생성자 호출을 새 시그니처로 바꾸고, `properties`를 필드로 올린다.

```java
    PersistenceProperties properties;
    DispatcherFactory factory;
    DispatcherFile file;
    TopicQueueFactory topicQueueFactory;
    DispatcherContainer container;

    @BeforeEach
    void setUp() {
        properties = new PersistenceProperties(tempDir.toString(), null);
        factory = new DispatcherFactory();
        file = new DispatcherFile(properties);
        topicQueueFactory = new TopicQueueFactory(properties);
        container = new DispatcherContainer(factory, file);
    }
```

그리고 `DispatcherBeanRegistrarTest`에 있던 부트스트랩 케이스를 아래 7개로 옮겨 `DispatcherContainerTest` 마지막에 추가한다.

```java
    @Test
    @DisplayName("파일의 정의 2개를 읽어 Dispatcher 2개를 만든다")
    void loadsDefinitionsFromFile() {
        write("""
                [
                  {"consumerId":"order-created","host":"http://127.0.0.1:8080","pattern":"order.created"},
                  {"consumerId":"order-shipped","host":"http://127.0.0.1:8080","pattern":"order.shipped"}
                ]
                """);

        DispatcherContainer loaded = new DispatcherContainer(factory, file);

        assertThat(loaded.definitions())
                .extracting(DispatcherDefinition::consumerId)
                .containsExactly("order-created", "order-shipped");
        loaded.destroy();
    }

    @Test
    @DisplayName("파일이 비어 있으면 0개로 기동한다")
    void bootsEmptyWhenFileHasNoDefinitions() {
        write("[]");

        DispatcherContainer loaded = new DispatcherContainer(factory, file);

        assertThat(loaded.definitions()).isEmpty();
        loaded.destroy();
    }

    @Test
    @DisplayName("root-dir 디렉터리가 없으면 디렉터리와 빈 파일을 만들고 기동한다")
    void createsRootDirWhenMissing() {
        Path absentRoot = tempDir.resolve("absent-root");
        DispatcherFile absentFile = new DispatcherFile(new PersistenceProperties(absentRoot.toString(), null));

        DispatcherContainer loaded = new DispatcherContainer(factory, absentFile);

        assertThat(loaded.definitions()).isEmpty();
        assertThat(absentRoot.resolve("dispatchers.json")).exists();
        loaded.destroy();
    }

    @Test
    @DisplayName("중복 consumerId면 생성에 실패한다")
    void failsOnDuplicateConsumerIdInFile() {
        write("""
                [
                  {"consumerId":"dup","host":"http://127.0.0.1:8080","pattern":"a"},
                  {"consumerId":"dup","host":"http://127.0.0.1:8080","pattern":"b"}
                ]
                """);

        assertThatThrownBy(() -> new DispatcherContainer(factory, file))
                .isInstanceOf(DuplicateConsumerIdException.class);
    }

    @Test
    @DisplayName("consumerId가 regex에 어긋나면 생성에 실패한다")
    void failsOnInvalidConsumerIdInFile() {
        write("""
                [
                  {"consumerId":"invalid id!","host":"http://127.0.0.1:8080","pattern":"a"}
                ]
                """);

        assertThatThrownBy(() -> new DispatcherContainer(factory, file))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("미지원 스킴이면 생성에 실패한다")
    void failsOnUnsupportedSchemeInFile() {
        write("""
                [
                  {"consumerId":"x","host":"ftp://127.0.0.1:8080","pattern":"a"}
                ]
                """);

        assertThatThrownBy(() -> new DispatcherContainer(factory, file))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("깨진 JSON이면 생성에 실패한다")
    void failsOnMalformedFile() {
        write("");

        assertThatThrownBy(() -> new DispatcherContainer(factory, file))
                .isInstanceOf(IllegalStateException.class);
    }
```

그리고 아래 헬퍼를 `DispatcherContainerTest`의 private 메서드 영역에 추가한다.

```java
    private void write(String json) {
        try {
            Files.writeString(tempDir.resolve("dispatchers.json"), json);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to write dispatcher file", exception);
        }
    }
```

`import java.io.IOException;`을 추가한다.

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherContainerTest"`
Expected: 컴파일 실패 — `DispatcherContainer(DispatcherFactory, DispatcherFile)` 생성자 없음

- [ ] **Step 3: 컨테이너 생성자 교체**

`DispatcherContainer.java`의 생성자를 아래로 교체하고, 쓰이지 않게 된 `import java.util.Collection;`을 제거한다.

```java
    public DispatcherContainer(DispatcherFactory factory, DispatcherFile file) {
        this.factory = factory;
        this.file = file;
        file.read().forEach(definition -> {
            Dispatcher dispatcher = factory.create(definition);
            if (dispatchers.putIfAbsent(dispatcher.consumerId(), dispatcher) != null) {
                throw new DuplicateConsumerIdException(dispatcher.consumerId());
            }
        });
    }
```

- [ ] **Step 4: 레지스트라와 우회 코드 삭제**

```bash
git rm broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherBeanRegistrar.java broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java
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

`broker/src/main/java/org/mmmq/broker/persistence/PersistenceProperties.java`에서 아래 메서드와 두 import(`Binder`, `Environment`)를 제거한다.

```java
    public static PersistenceProperties bind(Environment environment) {
        return Binder.get(environment)
                .bind(PREFIX, PersistenceProperties.class)
                .orElseGet(() -> new PersistenceProperties(null, null));
    }
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL

- [ ] **Step 6: 커밋**

```bash
git add -u
git add broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherContainer.java broker/src/main/java/org/mmmq/broker/BrokerConfiguration.java broker/src/main/java/org/mmmq/broker/persistence/PersistenceProperties.java broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherContainerTest.java
git commit -F - <<'MSGEOF'
refactor: Dispatcher 소유권을 DispatcherContainer로 이전

- 런타임에 추가한 Dispatcher는 빈이 될 수 없어 생성 경로가 둘로 갈리던 문제를 해결.
- 컨테이너가 dispatchers.json을 직접 읽어 Dispatcher를 만들도록 바꾸고 DispatcherBeanRegistrar를 제거.
- 레지스트라 때문에 필요했던 PersistenceProperties.bind 우회도 함께 제거.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## Task 11: DispatcherController

엔드포인트 4개와 상태 코드 매핑을 붙인다. 전역 `@RestControllerAdvice`는 쓰지 않는다. broker는 라이브러리라 호스트 애플리케이션의 다른 컨트롤러 예외까지 가로챈다.

broker에는 `@SpringBootConfiguration`이 없어 `@WebMvcTest`를 쓸 수 없다. standalone MockMvc로 검증한다.

**Files:**
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherController.java`
- Create: `broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherControllerTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherControllerTest.java`:

```java
package org.mmmq.broker.dispatcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
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
    @DisplayName("POST에서 잘못된 입력은 400을 돌려준다")
    void postReturnsBadRequestOnInvalidInput() throws Exception {
        when(container.add(any())).thenThrow(new IllegalArgumentException("host must not be blank"));

        mockMvc.perform(post("/mmmq/dispatchers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"consumerId":"order-created","host":"","pattern":"order.*"}
                                """))
                .andExpect(status().isBadRequest());
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
    @DisplayName("DELETE에서 없는 consumerId는 404를 돌려준다")
    void deleteReturnsNotFound() throws Exception {
        doThrow(new DispatcherNotFoundException(new ConsumerId("absent")))
                .when(container).remove(any());

        mockMvc.perform(delete("/mmmq/dispatchers/absent"))
                .andExpect(status().isNotFound());
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

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherControllerTest"`
Expected: PASS (9 tests)

- [ ] **Step 5: 전체 테스트 확인**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL

- [ ] **Step 6: 커밋**

```bash
git add broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherController.java broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherControllerTest.java
git commit -F - <<'MSGEOF'
feat: 런타임 Dispatcher 관리 엔드포인트 추가

- 재기동 없이 Dispatcher를 추가·수정·삭제·조회할 수 있도록 /mmmq/dispatchers를 노출.
- 라이브러리 모듈이 호스트 애플리케이션의 예외까지 가로채지 않도록 컨트롤러 내부 @ExceptionHandler로 한정.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## Task 12: CLAUDE.md 갱신

`Broker Dispatcher Registration` 절이 삭제된 `DispatcherBeanRegistrar`와 옛 파일 포맷을 설명하고 있다.

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Broker Dispatcher Registration 절 교체**

`CLAUDE.md`의 `## Broker Dispatcher Registration` 절 전체를 아래로 교체:

```markdown
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

`host` is an absolute URL; the scheme must be `http` or `https` (case-insensitive) and the port falls back to the scheme default (80/443) when omitted. When the file is absent, an empty `[]` file is created and the broker boots with no dispatchers. Invalid definitions — duplicate `consumerId`, unsupported scheme, malformed JSON — fail context startup (fail-fast). Each Dispatcher gets its own `<consumerId>.checkpoint` file under the per-topic storage directory.

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
```

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

- [ ] **Step 4: 문서가 코드와 맞는지 확인**

Run: `grep -n "DispatcherBeanRegistrar\|HostDefinition" CLAUDE.md`
Expected: 출력 없음

- [ ] **Step 5: 커밋**

```bash
git add CLAUDE.md
git commit -F - <<'MSGEOF'
docs: CLAUDE.md의 Dispatcher 등록 설명을 새 구조에 맞게 갱신

- 삭제된 DispatcherBeanRegistrar와 옛 중첩 host 포맷 설명을 걷어내고 URL 문자열 포맷으로 교체.
- 런타임 관리 엔드포인트와 tail 시작·체크포인트 삭제 규칙을 추가.

Co-authored-by: songsunkook <songsunkook@gmail.com>
MSGEOF
```

---

## 완료 확인

- [ ] `./gradlew build` — BUILD SUCCESSFUL
- [ ] `git log --oneline -12` — 태스크별 커밋 12개
- [ ] `grep -rn "DispatcherBeanRegistrar\|HostDefinition" --include="*.java" broker core` — 출력 없음
