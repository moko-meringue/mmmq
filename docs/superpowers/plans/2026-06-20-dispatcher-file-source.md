# 파일 기반 Dispatcher 빈 등록 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dispatcher 정의의 소스를 Java `@Bean` 코드에서 전용 JSON 파일로 옮기되, Dispatcher는 여전히 진짜 스프링 빈으로 등록한다.

**Architecture:** JSON 파일(`mmmq.broker.dispatchers.file`, 기본 `./dispatchers.json`)을 두 개의 DTO record(`DispatcherDefinition`, `HostDefinition`)로 역직렬화하고, `ImportBeanDefinitionRegistrar`(`DispatcherBeanRegistrar`)가 부팅 시(설정 클래스 파싱 단계) 각 정의를 인스턴스 서플라이어 빈으로 등록한다. `DispatcherContainer(Collection<Dispatcher>)`는 변경 없이 등록된 빈을 그대로 수집한다.

**Tech Stack:** Java 17, Spring Boot 3.2.0, Jackson(이미 `spring-boot-starter-web`에 포함), JUnit 5 + AssertJ, Spring Boot `ApplicationContextRunner`.

**Spec:** `docs/superpowers/specs/2026-06-20-dispatcher-file-source-design.md`

**커밋 규약:** 모든 커밋은 프로젝트 커밋 스킬(`.claude/skills/commit/SKILL.md`)을 따른다 — 커밋 전 `./gradlew test` 통과 확인, 관련 파일만 staging, 한국어 명령형 `<type>: <subject>`, 본문 뒤 빈 줄 + `Co-authored-by: songsunkook <songsunkook@gmail.com>`(현재 git 사용자 `cookie-meringue` 기준).

---

## File Structure

생성/수정할 파일과 각 책임:

| 파일 | 책임 |
|---|---|
| `broker/src/main/java/org/mmmq/broker/dispatcher/HostDefinition.java` (생성) | `host` JSON 객체 → core `Host` 변환 DTO. protocol/address/port 형식 검증. |
| `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherDefinition.java` (생성) | dispatcher entry JSON → 도메인 `Dispatcher` 변환 DTO. |
| `broker/src/main/java/org/mmmq/broker/config/DispatcherBeanRegistrar.java` (생성) | 파일을 읽어 각 `Dispatcher`를 빈 정의로 등록하는 `ImportBeanDefinitionRegistrar`. |
| `broker/src/main/java/org/mmmq/broker/config/BrokerConfiguration.java` (수정) | `@Import(DispatcherBeanRegistrar.class)` 추가. |
| `broker/src/test/java/org/mmmq/broker/dispatcher/HostDefinitionTest.java` (생성) | `HostDefinition` 단위 테스트. |
| `broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherDefinitionTest.java` (생성) | `DispatcherDefinition` 단위 테스트. |
| `broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java` (생성) | `ApplicationContextRunner` 기반 등록/실패 시나리오 통합 테스트. |

**변경하지 않음:** `DispatcherContainer`, `Dispatcher`, `Sender`, `TopicQueue`, 체크포인트 저장소. (spec §3.4)

---

## Task 1: `HostDefinition` DTO

`host` JSON 객체를 core `Host`로 바꾸는 record. protocol은 `String`으로 받아 대소문자 무관하게 변환하고, address 공백·port 범위를 컴팩트 생성자에서 막는다.

**Files:**
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/HostDefinition.java`
- Test: `broker/src/test/java/org/mmmq/broker/dispatcher/HostDefinitionTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`broker/src/test/java/org/mmmq/broker/dispatcher/HostDefinitionTest.java`:

```java
package org.mmmq.broker.dispatcher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.Host;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HostDefinitionTest {

    @Test
    @DisplayName("정의는 Host로 변환되며 protocol은 대소문자를 가리지 않는다")
    void convertsToHostCaseInsensitively() {
        HostDefinition definition = new HostDefinition("http", "127.0.0.1", 8080);

        Host host = definition.toHost();

        assertThat(host.toUri()).isEqualTo("http://127.0.0.1:8080");
    }

    @Test
    @DisplayName("알 수 없는 protocol은 예외를 던진다")
    void rejectsUnknownProtocol() {
        HostDefinition definition = new HostDefinition("ftp", "127.0.0.1", 8080);

        assertThatThrownBy(definition::toHost)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("address가 비어 있으면 예외를 던진다")
    void rejectsBlankAddress() {
        assertThatThrownBy(() -> new HostDefinition("HTTP", " ", 8080))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("port가 범위를 벗어나면 예외를 던진다")
    void rejectsPortOutOfRange() {
        assertThatThrownBy(() -> new HostDefinition("HTTP", "127.0.0.1", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.HostDefinitionTest"`
Expected: 컴파일 실패 — `HostDefinition` 심볼을 찾을 수 없음.

- [ ] **Step 3: 최소 구현 작성**

`broker/src/main/java/org/mmmq/broker/dispatcher/HostDefinition.java`:

```java
package org.mmmq.broker.dispatcher;

import java.util.Locale;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;

public record HostDefinition(

        String protocol,
        String address,
        int port
) {

    public HostDefinition {
        if (address == null || address.isBlank()) {
            throw new IllegalArgumentException("host.address must not be null or blank");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("host.port must be between 1 and 65535, but was: " + port);
        }
    }

    public Host toHost() {
        return new Host(WebProtocol.valueOf(protocol.toUpperCase(Locale.ROOT)), address, port);
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.HostDefinitionTest"`
Expected: PASS (4개 테스트).

- [ ] **Step 5: 커밋** (커밋 스킬 준수)

```bash
./gradlew test
git add broker/src/main/java/org/mmmq/broker/dispatcher/HostDefinition.java \
        broker/src/test/java/org/mmmq/broker/dispatcher/HostDefinitionTest.java
git commit -m "$(cat <<'EOF'
feat: Host 정의 JSON DTO 추가

- dispatchers.json의 host 객체를 core Host로 변환하는 HostDefinition record 추가.
- protocol을 String으로 받아 대소문자 무관하게 변환하고, address 공백과 port 범위를 검증.

Co-authored-by: songsunkook <songsunkook@gmail.com>
EOF
)"
```

---

## Task 2: `DispatcherDefinition` DTO

dispatcher entry JSON을 도메인 `Dispatcher`로 바꾸는 record. `toDispatcher()`가 `ConsumerId`/`Host`/`TopicPattern`을 생성하므로 호출 시점에 형식 검증이 일괄 수행된다.

**Files:**
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherDefinition.java`
- Test: `broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherDefinitionTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherDefinitionTest.java`:

```java
package org.mmmq.broker.dispatcher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.identifier.ConsumerId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DispatcherDefinitionTest {

    @Test
    @DisplayName("정의는 consumerId를 가진 Dispatcher로 변환된다")
    void convertsToDispatcher() {
        DispatcherDefinition definition = new DispatcherDefinition(
                "order-created",
                new HostDefinition("HTTP", "127.0.0.1", 8080),
                "order.created"
        );

        Dispatcher dispatcher = definition.toDispatcher();

        assertThat(dispatcher.consumerId()).isEqualTo(new ConsumerId("order-created"));
    }

    @Test
    @DisplayName("consumerId가 regex에 어긋나면 예외를 던진다")
    void rejectsInvalidConsumerId() {
        DispatcherDefinition definition = new DispatcherDefinition(
                "invalid id!",
                new HostDefinition("HTTP", "127.0.0.1", 8080),
                "order.created"
        );

        assertThatThrownBy(definition::toDispatcher)
                .isInstanceOf(IllegalArgumentException.class);
    }
}
```

> 참고: `Dispatcher` 생성자는 `WorkerPool` executor를 `submit` 시점에만 만든다(`computeIfAbsent`). 테스트에서 `dispatch`를 호출하지 않으므로 스레드가 생성되지 않아 정리가 필요 없다.

- [ ] **Step 2: 테스트 실패 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherDefinitionTest"`
Expected: 컴파일 실패 — `DispatcherDefinition` 심볼을 찾을 수 없음.

- [ ] **Step 3: 최소 구현 작성**

`broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherDefinition.java`:

```java
package org.mmmq.broker.dispatcher;

import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;

public record DispatcherDefinition(

        String consumerId,
        HostDefinition host,
        String pattern
) {

    public Dispatcher toDispatcher() {
        return new Dispatcher(
                host.toHost(),
                new ConsumerId(consumerId),
                new TopicPattern(pattern)
        );
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.dispatcher.DispatcherDefinitionTest"`
Expected: PASS (2개 테스트).

- [ ] **Step 5: 커밋** (커밋 스킬 준수)

```bash
./gradlew test
git add broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherDefinition.java \
        broker/src/test/java/org/mmmq/broker/dispatcher/DispatcherDefinitionTest.java
git commit -m "$(cat <<'EOF'
feat: Dispatcher 정의 JSON DTO 추가

- dispatchers.json의 한 entry를 도메인 Dispatcher로 변환하는 DispatcherDefinition record 추가.
- toDispatcher 호출 시 ConsumerId, Host, TopicPattern 생성으로 형식 검증을 일괄 수행.

Co-authored-by: songsunkook <songsunkook@gmail.com>
EOF
)"
```

---

## Task 3: `DispatcherBeanRegistrar` + `BrokerConfiguration` 배선

JSON 파일을 읽어 각 `Dispatcher`를 인스턴스 서플라이어 빈으로 등록하는 `ImportBeanDefinitionRegistrar`. 테스트는 `ApplicationContextRunner`로 등록 성공과 기동 실패 시나리오를 메서드 단위로 단언한다.

**Files:**
- Create: `broker/src/main/java/org/mmmq/broker/config/DispatcherBeanRegistrar.java`
- Modify: `broker/src/main/java/org/mmmq/broker/config/BrokerConfiguration.java`
- Test: `broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java`:

```java
package org.mmmq.broker.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.dispatcher.DispatcherContainer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

class DispatcherBeanRegistrarTest {

    @TempDir
    Path tempDir;

    ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    @DisplayName("유효한 2개 정의 → Dispatcher 빈 2개가 등록된다")
    void registersDispatchersFromFile() throws IOException {
        Path file = write("""
                [
                  {"consumerId":"order-created","host":{"protocol":"HTTP","address":"127.0.0.1","port":8080},"pattern":"order.created"},
                  {"consumerId":"order-shipped","host":{"protocol":"HTTP","address":"127.0.0.1","port":8080},"pattern":"order.shipped"}
                ]
                """);

        runner.withPropertyValues("mmmq.broker.dispatchers.file=" + file)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBeansOfType(Dispatcher.class)).hasSize(2);
                });
    }

    @Test
    @DisplayName("파일이 없으면 → 0개로 정상 기동한다")
    void bootsWithNoDispatchersWhenFileMissing() {
        runner.withPropertyValues("mmmq.broker.dispatchers.file=" + tempDir.resolve("absent.json"))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBeansOfType(Dispatcher.class)).isEmpty();
                });
    }

    @Test
    @DisplayName("중복 consumerId → 기동에 실패한다")
    void failsOnDuplicateConsumerId() throws IOException {
        Path file = write("""
                [
                  {"consumerId":"dup","host":{"protocol":"HTTP","address":"127.0.0.1","port":8080},"pattern":"a"},
                  {"consumerId":"dup","host":{"protocol":"HTTP","address":"127.0.0.1","port":8080},"pattern":"b"}
                ]
                """);

        runner.withPropertyValues("mmmq.broker.dispatchers.file=" + file)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    @DisplayName("알 수 없는 protocol → 기동에 실패한다")
    void failsOnUnknownProtocol() throws IOException {
        Path file = write("""
                [
                  {"consumerId":"x","host":{"protocol":"ftp","address":"127.0.0.1","port":8080},"pattern":"a"}
                ]
                """);

        runner.withPropertyValues("mmmq.broker.dispatchers.file=" + file)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    @DisplayName("address 누락 → 기동에 실패한다")
    void failsOnMissingAddress() throws IOException {
        Path file = write("""
                [
                  {"consumerId":"x","host":{"protocol":"HTTP","port":8080},"pattern":"a"}
                ]
                """);

        runner.withPropertyValues("mmmq.broker.dispatchers.file=" + file)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    @DisplayName("빈 파일 → 기동에 실패한다")
    void failsOnEmptyFile() throws IOException {
        Path file = write("");

        runner.withPropertyValues("mmmq.broker.dispatchers.file=" + file)
                .run(context -> assertThat(context).hasFailed());
    }

    private Path write(String json) throws IOException {
        Path file = tempDir.resolve("dispatchers.json");
        Files.writeString(file, json);
        return file;
    }

    @Configuration
    @Import(DispatcherBeanRegistrar.class)
    static class TestConfig {

        @Bean
        DispatcherContainer dispatcherContainer(Collection<Dispatcher> dispatchers) {
            return new DispatcherContainer(dispatchers);
        }
    }
}
```

> 동작 근거:
> - `TestConfig`가 registrar를 `@Import`하고 `DispatcherContainer`를 함께 둔다. registrar가 등록한
>   `Dispatcher` 빈들이 `Collection<Dispatcher>`로 주입된다(0개면 빈 컬렉션).
> - 중복 consumerId는 registrar가 아니라 `DispatcherContainer` 생성자에서 잡힌다(빈 이름은 고유 생성).
> - protocol 오류는 raw `IllegalArgumentException`으로, address/빈 파일 오류는 `IllegalStateException`
>   (Jackson 예외가 `IOException`을 상속하므로 registrar의 catch가 감쌈)으로 기동을 실패시킨다.
>   어느 경우든 `ApplicationContextRunner`가 startup failure로 포착하므로 `hasFailed()`가 성립한다.

- [ ] **Step 2: 테스트 실패 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.config.DispatcherBeanRegistrarTest"`
Expected: 컴파일 실패 — `DispatcherBeanRegistrar` 심볼을 찾을 수 없음.

- [ ] **Step 3: registrar 구현 작성**

`broker/src/main/java/org/mmmq/broker/config/DispatcherBeanRegistrar.java`:

```java
package org.mmmq.broker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.dispatcher.DispatcherDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

class DispatcherBeanRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

    private static final Logger log = LoggerFactory.getLogger(DispatcherBeanRegistrar.class);

    private static final String FILE_PROPERTY = "mmmq.broker.dispatchers.file";
    private static final String DEFAULT_FILE = "./dispatchers.json";

    private Environment environment;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void registerBeanDefinitions(AnnotationMetadata metadata, BeanDefinitionRegistry registry) {
        Path path = Path.of(environment.getProperty(FILE_PROPERTY, DEFAULT_FILE));

        if (!Files.exists(path)) {
            log.warn("Dispatcher file not found at {}. No dispatchers registered.", path);
            return;
        }

        readDispatchers(path).forEach(dispatcher ->
                BeanDefinitionReaderUtils.registerWithGeneratedName(
                        BeanDefinitionBuilder.genericBeanDefinition(Dispatcher.class, () -> dispatcher)
                                .getBeanDefinition(),
                        registry
                ));
    }

    private List<Dispatcher> readDispatchers(Path path) {
        try {
            DispatcherDefinition[] definitions = new ObjectMapper()
                    .readValue(Files.readAllBytes(path), DispatcherDefinition[].class);
            return Arrays.stream(definitions)
                    .map(DispatcherDefinition::toDispatcher)
                    .toList();
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to read dispatcher file: " + path, exception);
        }
    }
}
```

- [ ] **Step 4: registrar 테스트 통과 확인**

Run: `./gradlew :broker:test --tests "org.mmmq.broker.config.DispatcherBeanRegistrarTest"`
Expected: PASS (6개 테스트).

- [ ] **Step 5: 실제 배선 추가 (`BrokerConfiguration`)**

`broker/src/main/java/org/mmmq/broker/config/BrokerConfiguration.java`를 아래로 만든다. import에 `Import` 추가, 어노테이션 스택에 `@Import`를 `@ComponentScan` 위(피라미드: 짧은 줄 → 긴 줄)로 삽입:

```java
package org.mmmq.broker.config;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

@AutoConfiguration
@EnableConfigurationProperties({
        StorageProperties.class,
        SegmentProperties.class
})
@Import(DispatcherBeanRegistrar.class)
@ComponentScan(basePackages = "org.mmmq.broker")
class BrokerConfiguration {

}
```

- [ ] **Step 6: 전체 broker 테스트 통과 확인 (실제 배선 + 회귀 없음)**

Run: `./gradlew :broker:test`
Expected: PASS. 특히 `BrokerTest`가 통과해야 한다 — 실제 앱에서 `@Import`된 registrar가 `./dispatchers.json` 부재 시 warn + 0개로 처리하고 컨텍스트가 정상 기동함을 확인(회귀 없음).

- [ ] **Step 7: 커밋** (커밋 스킬 준수)

```bash
./gradlew test
git add broker/src/main/java/org/mmmq/broker/config/DispatcherBeanRegistrar.java \
        broker/src/main/java/org/mmmq/broker/config/BrokerConfiguration.java \
        broker/src/test/java/org/mmmq/broker/config/DispatcherBeanRegistrarTest.java
git commit -m "$(cat <<'EOF'
feat: JSON 파일에서 Dispatcher를 빈으로 등록

- dispatchers.json을 읽어 각 Dispatcher를 인스턴스 서플라이어 빈으로 등록하는
  DispatcherBeanRegistrar(ImportBeanDefinitionRegistrar) 추가.
- BrokerConfiguration에 @Import로 배선. 파일 부재 시 0개로 정상 기동, 잘못된 정의는 기동 실패.
- 기존 @Bean 기반 Dispatcher 등록을 파일 소스로 대체.

Co-authored-by: songsunkook <songsunkook@gmail.com>
EOF
)"
```

---

## Self-Review

**1. Spec coverage**

| Spec 항목 | 구현 위치 |
|---|---|
| §3.1 파일 포맷/경로 property | Task 3 registrar(`FILE_PROPERTY`/`DEFAULT_FILE`) + 테스트 JSON |
| §3.2 `DispatcherDefinition` | Task 2 |
| §3.2 `HostDefinition`(protocol String + address/port 검증) | Task 1 |
| §3.2 `DispatcherBeanRegistrar`(Environment, 파싱 선행·등록 후행, 인스턴스 서플라이어) | Task 3 |
| §3.3 `BrokerConfiguration` `@Import` | Task 3 Step 5 |
| §3.4 `DispatcherContainer` 무변경 + 중복 검사 | 변경 없음, Task 3 테스트 (c)로 검증 |
| §3.5 파일 없음 → 0개 | Task 3 테스트 (b) |
| §3.5 빈/깨진 파일 → 실패 | Task 3 테스트 (빈 파일) |
| §3.5 잘못된 protocol → 실패 | Task 3 테스트 (d) |
| §3.5 address 누락 → 실패 | Task 3 테스트 (e) |
| §3.5 중복 consumerId → 실패 | Task 3 테스트 (c) |
| §3.5 DNS 해석 불가 → 실패 | 코드 경로 동일(`Host` 생성자). 네트워크 의존이라 자동 테스트는 제외(의도적). |
| §5 단위/통합 테스트 | Task 1·2(단위), Task 3(통합) |
| §7 코드 스타일 | 각 구현 코드에 반영(record 빈 줄, Lombok 미사용 명시 Logger, 인라인 검증, `Stream.toList()`, 지역변수 final 없음) |

빠진 spec 요구사항 없음. (§6 알려진 한계는 "코드 없음"으로 명시된 비목표 — 작업 불필요.)

**2. Placeholder scan:** TBD/TODO/"적절히 처리" 등 없음. 모든 코드 스텝에 완전한 코드 포함.

**3. Type consistency:** `HostDefinition(String protocol, String address, int port)` / `toHost()`, `DispatcherDefinition(String consumerId, HostDefinition host, String pattern)` / `toDispatcher()`, `DispatcherBeanRegistrar`의 `FILE_PROPERTY`·`readDispatchers` 시그니처가 Task 1→2→3에서 일관. `Dispatcher.consumerId()`(기존 public), `DispatcherContainer(Collection<Dispatcher>)`(기존), `Host.toUri()`(기존) 시그니처와 일치.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-20-dispatcher-file-source.md`. 두 가지 실행 방식:

1. **Subagent-Driven (추천)** — Task마다 새 subagent를 띄우고 Task 사이에 리뷰, 빠른 반복.
2. **Inline Execution** — 이 세션에서 executing-plans로 체크포인트 단위 일괄 실행.

어느 방식으로 진행할까요?
