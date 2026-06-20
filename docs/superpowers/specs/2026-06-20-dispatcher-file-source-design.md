# 파일 기반 Dispatcher 빈 등록 설계

- 날짜: 2026-06-20
- 대상 모듈: `broker`
- 상태: 설계 확정 (구현 전)

## 1. 배경 / 목표

기존에는 라이브러리 사용자가 `@Configuration` 클래스에 `@Bean public Dispatcher ...()` 메서드로
Dispatcher를 정의했고, `DispatcherContainer(Collection<Dispatcher>)` 가 스프링이 수집한 모든
Dispatcher 빈을 주입받았다.

이 설계는 Dispatcher 정의의 **소스를 Java 코드에서 전용 JSON 파일로 이동**한다. Dispatcher는
여전히 진짜 스프링 빈으로 등록된다. 즉 "어떻게 빈이 되는가"가 아니라 "정의가 어디서 오는가"만 바뀐다.

목표 아키텍처는 파일을 영속적 진실 원천으로 두고, 장차 런타임 추가/삭제 + 파일 write-back으로
확장하는 것이다. 다만 이번 작업의 범위는 아래 §2에서 좁힌다.

## 2. 범위

### 이번 작업
- 부팅 시 JSON 파일을 읽어 각 Dispatcher를 스프링 빈으로 등록.
- 기존 `@Bean` 기반 등록을 *요구하지 않는* 구조로 전환(파일이 소스).
- 파일은 미래 write-back을 막지 않도록 설계(쓰기 가능한 디스크 `Path`, round-trip 가능한 DTO,
  단일 등록 경계).

### 보류 (이번 작업에서 구현하지 않음)
- 런타임 추가/삭제 트리거(REST 등) 및 파일 write-back 로직. 런타임 변경이 없으므로 쓰기 코드도
  자연히 함께 보류된다. 현재 파일은 손으로 작성하고 읽기 전용이다.
- `README.md` / `CLAUDE.md` 등 제품 문서 업데이트. (사용자가 별도로 진행한다.)

## 3. 확정 설계

### 3.1 파일 포맷 / 위치

- 전용 JSON 파일. 경로 property: `mmmq.broker.dispatchers.file`, 기본값 `./dispatchers.json`.
- 최상위는 정의 배열이며, 한 entry는 하나의 consumerId·하나의 패턴에 대응한다(현재 `Dispatcher`
  모델 그대로). 여러 패턴이 필요하면 consumerId를 달리하는 entry를 추가한다.

```json
[
  {
    "consumerId": "order-created",
    "host": { "protocol": "HTTP", "address": "consumer-host", "port": 8080 },
    "pattern": "order.created"
  }
]
```

- `protocol` 값은 `HTTP` 또는 `HTTPS`이며 대소문자를 가리지 않는다(§3.4에서 `toUpperCase` 변환).
- 파일 스키마는 정확히 일치해야 한다. 정의에 없는 필드가 있으면 기동 시 실패한다(오타 조기 발견).

### 3.2 신규 컴포넌트

DTO는 `org.mmmq.broker.dispatcher` 패키지(도메인 `Dispatcher`와 응집), registrar는
`org.mmmq.broker.config` 패키지(`BrokerConfiguration`과 동일, package-private)에 둔다.

#### `DispatcherDefinition` (record, JSON DTO)

```java
package org.mmmq.broker.dispatcher;

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

- `toDispatcher()`는 도메인 값객체(`ConsumerId` 정규식, `Host` DNS 해석, `TopicPattern`)를 모두
  생성하므로, 호출 시점에 형식 검증이 일괄 수행된다.

#### `HostDefinition` (record, JSON DTO)

```java
package org.mmmq.broker.dispatcher;

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

- `protocol`을 `String`으로 받는 이유:
  1. Jackson 기본 enum 역직렬화는 대소문자를 구분한다. enum 이름(`HTTP`)과 scheme 값(`http`)이
     달라 사용자가 소문자를 쓰기 쉽다. `valueOf(... .toUpperCase())`로 흡수한다.
  2. core 모듈의 `WebProtocol`에 Jackson 어노테이션을 붙이지 않는다(core는 외부 Spring/Jackson
     의존 금지 원칙).
- `address`/`port` 검증을 컴팩트 생성자에 인라인으로 둔다. 미설정 시 `InetAddress.getByName(null)`이
  조용히 loopback을 반환하는 묵시적 위험과, `port=0`으로 잘못된 URL이 생성되는 문제를 차단한다.

#### `DispatcherBeanRegistrar` (`ImportBeanDefinitionRegistrar`, `EnvironmentAware`)

```java
package org.mmmq.broker.config;

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

- 경로는 `environment.getProperty(FILE_PROPERTY, DEFAULT_FILE)`로 읽는다. 환경변수
  (`MMMQ_BROKER_DISPATCHERS_FILE`)는 `SystemEnvironmentPropertySource`가 relaxed로 해석하므로
  별도 `Binder` 없이도 처리된다.
- 파싱·검증을 **먼저 전부 끝낸 뒤**(`readDispatchers`에서 `List<Dispatcher>` 완성) 등록 루프를
  돈다. 한 entry라도 잘못되면 아무 빈도 등록되기 전에 중단되어 부분 등록 상태가 남지 않는다.
- 파일 읽기와 JSON 파싱 실패를 하나의 `catch (IOException)`이 함께 감싼다
  (`JsonProcessingException`이 `IOException`을 상속). present 파일이 유효한 JSON 배열이 아니면
  기동이 실패한다.
- 로컬 `new ObjectMapper()`를 쓴다. registrar는 설정 클래스 파싱 단계에 실행되어 컨텍스트의
  `ObjectMapper` 빈을 주입받을 수 없다. (단순 DTO 매핑이라 표준 매퍼로 충분.)
- 인스턴스 서플라이어 `() -> dispatcher`는 미리 만든 인스턴스를 그대로 돌려준다. `@PreDestroy`는
  빈 인스턴스 클래스 기준으로 처리되므로 정상 호출된다(§4 검증).

### 3.3 변경 컴포넌트

- `BrokerConfiguration`: `@Import(DispatcherBeanRegistrar.class)` 한 줄 추가.

### 3.4 변경하지 않는 컴포넌트

- `DispatcherContainer`: 그대로. `Collection<Dispatcher>` 주입으로 등록된 빈을 수집하고, 중복
  consumerId 검사·`@PreDestroy`도 그대로. 빈 이름이 `registerWithGeneratedName`로 고유 생성되므로
  빈-override 예외가 컨테이너 검사보다 먼저 터지지 않는다 → 중복 시 컨테이너의 명확한 도메인
  메시지(`Duplicate consumerId '...'`)가 그대로 노출된다.
- `Dispatcher`, `Sender`, `TopicQueue`, 체크포인트 저장소 등 전부 무관.

### 3.5 에러 / 엣지 정책 (fail-fast)

| 상황 | 동작 |
|---|---|
| 파일 없음 | 0개 등록 + warn 로그. 부팅 성공(수신·영속화는 정상, 전달만 없음). |
| 파일은 있으나 유효한 JSON 배열 아님 (빈 파일·공백·`null`·깨짐·trailing) | 파싱 예외 → 기동 실패. |
| 정의에 없는 필드(오타) | 파싱 예외 → 기동 실패. |
| 잘못된 protocol(예: `ftp`) | `WebProtocol.valueOf` 예외 → 기동 실패. |
| `address` 누락/공백, `port` 범위 밖 | `HostDefinition` 생성자 예외 → 기동 실패. |
| DNS 해석 불가 host | `Host` 생성자 예외 → 기동 실패. |
| 파일 내 중복 consumerId | `DispatcherContainer` 예외 → 기동 실패(명확한 메시지). |

## 4. 적대적 리뷰 결과 (검증 완료)

4개 렌즈(Spring 메커니즘 / 기동 순서·생명주기 / 에러·엣지 / 제약·스코프·스타일)로 실제 소스와
대조해 검증했다.

### 그대로 진행해도 되는 것 (근거 확인됨)
- `@AutoConfiguration` 위의 `@Import`는 일반 `@Configuration`과 동일하게 처리되어
  `ImportBeanDefinitionRegistrar`가 honored 된다.
- `EnvironmentAware.setEnvironment`는 `ParserStrategyUtils.invokeAwareMethods`에 의해
  `registerBeanDefinitions`보다 항상 먼저 호출된다(코드 레벨 순서 보장).
- `BeanDefinitionBuilder.genericBeanDefinition(Class, Supplier)` →
  `BeanDefinitionReaderUtils.registerWithGeneratedName(AbstractBeanDefinition, ...)` 타입 체인 정합.
- 인스턴스 서플라이어로 등록된 빈에도 `@PreDestroy`가 `InitDestroyAnnotationBeanPostProcessor`에
  의해 정상 호출된다(빈 인스턴스 클래스 기준 스캔).
- **Dispatcher 빈 0개일 때 `Collection<Dispatcher>`에 빈 컬렉션이 주입되어 정상 기동** — 기존
  `BrokerTest`(Dispatcher 0개로 컨텍스트 기동 + POST 200)가 초록불임을 실행해 확인. "파일 없음→
  정상 기동"에 `DispatcherContainer` 수정 불필요.
- 부팅 순서: registrar가 설정 파싱 단계에서 Dispatcher 빈 정의를 등록 → 싱글톤 단계에서
  `DispatcherContainer`가 수집 → `SmartInitializingSingleton`(`TopicQueueBootstrapper`)이 그 뒤에
  실행되어 복원 토픽 큐에 subscribe. 순서 역전 없음.

### 채택한 수정 (실제 위험에 대응 — §3에 이미 반영)
- protocol을 `String`으로 받아 `toUpperCase` 후 `valueOf` 변환(대소문자 함정 + core 오염 차단).
- `HostDefinition`에서 `address` 공백·`port` 범위 검증.
- registrar에서 파싱·검증을 선행하고 등록을 후행(부분 등록 방지).

### 기각한 권고
- `FAIL_ON_UNKNOWN_PROPERTIES` 비활성화 / `@JsonIgnoreProperties` → **기각.** 엄격 파싱이 손으로
  쓴 설정의 오타를 기동 시 잡아준다. 미래 필드 추가 호환은 write-back 작업에서 함께 다룬다(YAGNI).
- `DispatcherContainer`를 `@Nullable`/setter 주입으로 변경 → **불필요.** 빈 컬렉션 주입이
  실증으로 확인됨.
- `setDestroyMethodName("destroy")` 명시 → **불필요.** `@PreDestroy`가 이미 동작함이 확인됨.

## 5. 테스트 계획

`broker` 모듈, `@SpringBootTest` + `@DynamicPropertySource`로 임시 JSON 파일 경로를 주입한다.

- (a) 유효한 2개 entry → `Dispatcher` 빈 2개가 컨텍스트에 존재(또는 `DispatcherContainer`가 2개
  보유).
- (b) 존재하지 않는 경로 지정 → 컨텍스트 정상 기동, `Dispatcher` 빈 0개.
- (c) 파일 내 중복 consumerId → 컨텍스트 기동 실패(`IllegalStateException`).
- (d) 잘못된 protocol(`ftp` 등) → 컨텍스트 기동 실패. (소문자 `http`는 `toUpperCase`로 성공해야
  하므로 별도 케이스로 확인.)
- (e) `address` 누락/공백 → 컨텍스트 기동 실패.
- (선택) 빈/공백 파일 → 컨텍스트 기동 실패(유효한 JSON 배열 아님).

기존 `BrokerTest`는 dispatcher 미설정 경로라 영향 없음(이미 0개로 기동).

## 6. 알려진 한계 (문서화, 코드 없음)

- dispatchers.json에서 특정 consumerId를 제거해도 그 consumerId의 `.checkpoint` 파일은 디스크에
  남아 고아가 된다. 정리는 보류된 런타임 관리 기능 영역이며, 현재는 수동 정리 대상이다.
- 사용자가 경로를 잘못 지정(오타)하면 파일 없음으로 간주되어 0개·warn으로 조용히 부팅된다.
  명시적으로 설정된 경로의 부재를 기동 실패로 강화하는 것은 추후 하드닝 후보.

## 7. 코드 스타일 체크리스트 (구현 시)

- 지역변수에 `final` 금지.
- record: 컴포넌트마다 새 줄, 메서드가 있으면 `{` 다음 빈 줄.
- `broker` 모듈은 Lombok 금지 — 명시적 `Logger` 선언.
- 검증 로직은 생성자/팩토리에 인라인(별도 `validateXxx` 헬퍼 추출 금지).
- 불변 컬렉션 반환(`Stream.toList()`).
- 신규 의존성 추가 없음(Jackson은 `spring-boot-starter-web`로 이미 존재).
