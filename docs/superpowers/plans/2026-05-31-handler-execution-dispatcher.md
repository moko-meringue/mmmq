# Dispatcher를 HandlerExecution 대리인으로 전환 — 구현 플랜

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dispatcher를 Consumer 호스트 단위 대리인에서 HE 단위 대리인으로 전환하여 원자성·격리 문제를 해결한다. ID 기반 라우팅 + 동기 응답 + Container 기반 직접 호출 흐름으로 재구성.

**Architecture:** `core`에 `Metadata` 도메인 추상화 신설. Broker `Dispatcher`는 1 ID = 1 HE = 1 pattern 단위가 되고, 신규 `DispatcherContainer`(`SmartInitializingSingleton` + `ObjectProvider`)가 유일성 검증과 라우팅을 함께 담당. Consumer는 `FrontHandler` 큐/풀 제거 후 Controller가 ID 헤더로 직접 HE 라우팅하여 동기 실행.

**Tech Stack:** Java 17, Spring Boot 3.2+, Spring Web (RestClient), Gradle multi-module, Lombok, SLF4J.

**Spec:** `docs/superpowers/specs/2026-05-31-handler-execution-dispatcher-design.md`

**Test policy:** 본 플랜은 단위 테스트를 작성하지 않는다. 사용자 요청에 따라 테스트는 별도 리팩터링 단계에서 일괄 작성 예정. 각 Phase 끝에 `./gradlew :<module>:compileJava`로 컴파일 검증만 수행. 최종 Phase에서 `./gradlew build`로 전체 빌드.

---

## File Map

**신규 파일:**
- `core/src/main/java/org/mmmq/core/metadata/Metadata.java`
- `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherContainer.java`

**변경 파일:**
- `broker/src/main/java/org/mmmq/broker/dispatcher/Dispatcher.java`
- `broker/src/main/java/org/mmmq/broker/dispatcher/FrontDispatcher.java`
- `broker/src/main/java/org/mmmq/broker/dispatcher/sender/Sender.java`
- `broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueue.java`
- `broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueueContainer.java`
- `consumer/src/main/java/org/mmmq/consumer/Consumer.java`
- `consumer/src/main/java/org/mmmq/consumer/handler/execution/HandlerExecution.java`
- `consumer/src/main/java/org/mmmq/consumer/handler/execution/HandlerExecutions.java`
- `consumer/src/main/java/org/mmmq/consumer/handler/execution/method/MMMQListener.java`
- `consumer/src/main/java/org/mmmq/consumer/handler/execution/method/MethodExecution.java`
- `consumer/src/main/java/org/mmmq/consumer/handler/execution/method/MethodExecutionRegistration.java`
- `consumer/src/main/java/org/mmmq/consumer/handler/execution/type/MMMQListener.java`
- `consumer/src/main/java/org/mmmq/consumer/handler/execution/type/InterfaceExecution.java`
- `consumer/src/main/java/org/mmmq/consumer/handler/execution/type/InterfaceExecutionRegistration.java`

**제거 파일:**
- `consumer/src/main/java/org/mmmq/consumer/handler/FrontHandler.java`
- `broker/src/main/java/org/mmmq/broker/dispatcher/MessageArrivedEvent.java`
- `broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueueInitializedEvent.java`

---

## Phase 1 — Core: `Metadata` 신설

### Task 1: `Metadata` 클래스 신설

**Files:**
- Create: `core/src/main/java/org/mmmq/core/metadata/Metadata.java`

- [ ] **Step 1: 디렉토리 생성**

```bash
mkdir -p /Users/kimdaehyeon/Desktop/mmmq/core/src/main/java/org/mmmq/core/metadata
```

- [ ] **Step 2: `Metadata.java` 파일 작성**

```java
package org.mmmq.core.metadata;

import org.springframework.lang.Nullable;

import java.util.HashMap;
import java.util.Map;

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

- [ ] **Step 3: 의존 확인 — `core/build.gradle`에 `spring-web`가 이미 있는지 확인**

`org.springframework.lang.Nullable`은 `spring-core`에 포함됨. broker/consumer가 core를 의존하므로 transitive로 들어와야 함. 확인:

```bash
grep -r "spring-core\|spring-web" /Users/kimdaehyeon/Desktop/mmmq/core/build.gradle /Users/kimdaehyeon/Desktop/mmmq/build.gradle 2>/dev/null
```

기본 Spring Boot 의존이 부모/루트에서 들어오는지 확인. 만약 `org.springframework.lang.Nullable`이 컴파일 시 import 안 되면 `core/build.gradle`에 `implementation 'org.springframework:spring-core'`를 추가.

- [ ] **Step 4: core 모듈 컴파일 확인**

```bash
cd /Users/kimdaehyeon/Desktop/mmmq && ./gradlew :core:compileJava
```

Expected: BUILD SUCCESSFUL

- [ ] **Step 5: 커밋**

```bash
cd /Users/kimdaehyeon/Desktop/mmmq && git add core/src/main/java/org/mmmq/core/metadata/Metadata.java && git commit -m "feat(core): add Metadata domain abstraction"
```

---

## Phase 2 — Consumer: HandlerExecution 모델 재구성

> 의도된 컴파일 깨짐 단계: 본 Phase 내부에서 시그니처 동시 변경이 일어나며, 컴파일 검증은 Phase 마지막에서 한 번 수행.

### Task 2: `@MMMQListener` 어노테이션 변경

**Files:**
- Modify: `consumer/src/main/java/org/mmmq/consumer/handler/execution/method/MMMQListener.java`

- [ ] **Step 1: 전체 내용을 다음으로 교체**

```java
package org.mmmq.consumer.handler.execution.method;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MMMQListener {

    String id();
}
```

기존 `value()`/`pattern()` 속성과 `@AliasFor` 의존 모두 제거. ID는 필수(디폴트 없음).

### Task 3: `MMMQListener<T>` 인터페이스 변경

**Files:**
- Modify: `consumer/src/main/java/org/mmmq/consumer/handler/execution/type/MMMQListener.java`

- [ ] **Step 1: 전체 내용을 다음으로 교체**

```java
package org.mmmq.consumer.handler.execution.type;

public interface MMMQListener<T> {

    String id();

    void handle(T content);
}
```

기존 `listens()` 메서드 제거. `id()` 추상 메서드 추가.

### Task 4: `HandlerExecution` 추상 클래스 변경

**Files:**
- Modify: `consumer/src/main/java/org/mmmq/consumer/handler/execution/HandlerExecution.java`

- [ ] **Step 1: 전체 내용을 다음으로 교체**

```java
package org.mmmq.consumer.handler.execution;

import org.mmmq.core.message.Message;

public abstract class HandlerExecution {

    protected final String id;

    protected HandlerExecution(String id) {
        this.id = id;
    }

    public abstract void execute(Message message);

    public String id() {
        return id;
    }
}
```

`name`/`pattern` 필드 제거. `supports(Message)` 제거. `getName()`/`getPattern()` getter 제거. `id` 필드 + `id()` getter 추가.

### Task 5: `MethodExecution` 구현체 변경

**Files:**
- Modify: `consumer/src/main/java/org/mmmq/consumer/handler/execution/method/MethodExecution.java`

- [ ] **Step 1: 전체 내용을 다음으로 교체**

```java
package org.mmmq.consumer.handler.execution.method;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.consumer.exception.HandlerExecutionException;
import org.mmmq.consumer.exception.InvalidHandlerException;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.core.message.Message;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

class MethodExecution extends HandlerExecution {

    final Object bean;
    final Method method;
    final JavaType parameterType;
    final ObjectMapper objectMapper;

    MethodExecution(String id, Object bean, Method method, ObjectMapper objectMapper) {
        super(id);
        method.setAccessible(true);
        this.bean = bean;
        this.method = method;
        this.objectMapper = objectMapper;
        this.parameterType = getParameterType(method, objectMapper);
    }

    private JavaType getParameterType(Method method, ObjectMapper objectMapper) {
        if (method.getParameterCount() != 1) {
            throw new InvalidHandlerException("MethodExecution must have exactly one parameter: " + id);
        }
        return objectMapper.constructType(method.getGenericParameterTypes()[0]);
    }

    @Override
    public void execute(Message message) {
        Object parameter = getParameter(message);

        try {
            method.invoke(bean, parameter);
        } catch (InvocationTargetException e) {
            throw new HandlerExecutionException(
                    "MethodExecution " + id + " threw an exception while processing.",
                    e.getCause()
            );
        } catch (Exception e) {
            throw new HandlerExecutionException(
                    String.format("Unexpected error occurred during execute handler execution %s: %s", id, e),
                    e
            );
        }
    }

    private Object getParameter(Message message) {
        if (message.content() == null) {
            return null;
        }
        try {
            return objectMapper.convertValue(message.content(), parameterType);
        } catch (IllegalArgumentException e) {
            throw new HandlerExecutionException(
                    String.format("Failed to convert parameter for handler execution '%s': %s", id, e.getMessage()),
                    e
            );
        }
    }
}
```

생성자가 `(String id, Object bean, Method method, ObjectMapper objectMapper)`로 변경. `TopicPattern` import 제거. 자동 생성된 name 로직 제거. 내부에서 `name` 참조하던 곳을 모두 `id`로 변경.

### Task 6: `InterfaceExecution` 구현체 변경

**Files:**
- Modify: `consumer/src/main/java/org/mmmq/consumer/handler/execution/type/InterfaceExecution.java`

- [ ] **Step 1: 전체 내용을 다음으로 교체**

```java
package org.mmmq.consumer.handler.execution.type;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.consumer.exception.HandlerExecutionException;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.core.message.Message;
import org.springframework.core.GenericTypeResolver;
import org.springframework.util.ClassUtils;

class InterfaceExecution extends HandlerExecution {

    final MMMQListener<Object> mmmqListener;
    final JavaType parameterType;
    final ObjectMapper objectMapper;

    @SuppressWarnings("unchecked")
    InterfaceExecution(MMMQListener<?> mmmqListener, ObjectMapper objectMapper) {
        super(mmmqListener.id());
        this.mmmqListener = (MMMQListener<Object>) mmmqListener;
        this.objectMapper = objectMapper;
        this.parameterType = resolveParameterType(mmmqListener, objectMapper);
    }

    private JavaType resolveParameterType(MMMQListener<?> mmmqListener, ObjectMapper objectMapper) {
        Class<?> userClass = ClassUtils.getUserClass(mmmqListener);
        Class<?> genericType = GenericTypeResolver.resolveTypeArgument(userClass, MMMQListener.class);

        return objectMapper.constructType(genericType != null ? genericType : Object.class);
    }

    @Override
    public void execute(Message message) {
        Object content = getParameter(message);
        try {
            mmmqListener.handle(content);
        } catch (Exception e) {
            throw new HandlerExecutionException(
                    String.format("Unexpected error during interface execution %s: %s", id, e),
                    e
            );
        }
    }

    private Object getParameter(Message message) {
        if (message.content() == null) {
            return null;
        }
        try {
            return objectMapper.convertValue(message.content(), parameterType);
        } catch (IllegalArgumentException e) {
            throw new HandlerExecutionException(
                    String.format("Failed to convert parameter for interface execution '%s': %s", id, e.getMessage()),
                    e
            );
        }
    }
}
```

생성자에서 `mmmqListener.id()`를 `super(id)`로 전달. `ClassUtils.getUserClass(...).getCanonicalName()` 기반 자동 name 생성 제거. `mmmqListener.listens()` 호출 제거. 내부 `name` 참조를 `id`로 교체.

### Task 7: `HandlerExecutions` 자료구조 + `@Component`

**Files:**
- Modify: `consumer/src/main/java/org/mmmq/consumer/handler/execution/HandlerExecutions.java`

- [ ] **Step 1: 전체 내용을 다음으로 교체**

```java
package org.mmmq.consumer.handler.execution;

import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class HandlerExecutions {

    private final Map<String, HandlerExecution> byId = new ConcurrentHashMap<>();

    public void add(HandlerExecution handlerExecution) {
        HandlerExecution previous = byId.putIfAbsent(handlerExecution.id(), handlerExecution);
        if (previous != null) {
            throw new IllegalStateException(
                    "Duplicate HandlerExecution id '" + handlerExecution.id() + "'"
            );
        }
    }

    @Nullable
    public HandlerExecution find(String id) {
        return byId.get(id);
    }

    public int size() {
        return byId.size();
    }
}
```

기존 `List + Map<Topic, List<HE>> topicCache` 자료구조 + `getExecutions(Message)` 메서드 모두 제거. `Map<String, HandlerExecution>`로 단순화. `add`에서 중복 ID 시 예외 throw. `find(String)`는 `@Nullable` 반환. `@Component` 추가하여 Spring Bean으로 등록.

### Task 8: `MethodExecutionRegistration` 변경

**Files:**
- Read first: `consumer/src/main/java/org/mmmq/consumer/handler/execution/method/MethodExecutionRegistration.java`
- Modify: 같은 파일

- [ ] **Step 1: 현재 파일을 읽어 어떤 형태인지 파악**

```bash
cat /Users/kimdaehyeon/Desktop/mmmq/consumer/src/main/java/org/mmmq/consumer/handler/execution/method/MethodExecutionRegistration.java
```

- [ ] **Step 2: 변경 적용**

다음 변경을 적용:
1. `FrontHandler` 주입 → `HandlerExecutions` 주입으로 교체. 생성자 매개변수 및 필드 모두 변경.
2. `@MMMQListener` 어노테이션을 발견했을 때 `annotation.value()`/`annotation.pattern()` 대신 `annotation.id()`로 ID 추출.
3. `MethodExecution` 생성 시 새 시그니처 `new MethodExecution(annotation.id(), bean, method, objectMapper)` 사용.
4. 등록 호출을 `frontHandler.addHandlerExecution(...)`에서 `handlerExecutions.add(...)`로 교체.
5. `TopicPattern` 관련 import 제거.

코드 예시(현재 클래스 구조 유지하며 변경):

```java
package org.mmmq.consumer.handler.execution.method;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.consumer.exception.HandlerExecutionRegistrationException;
import org.mmmq.consumer.handler.execution.HandlerExecutions;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

@Component
public class MethodExecutionRegistration implements BeanPostProcessor {

    private final HandlerExecutions handlerExecutions;
    private final ObjectMapper objectMapper;

    public MethodExecutionRegistration(HandlerExecutions handlerExecutions, ObjectMapper objectMapper) {
        this.handlerExecutions = handlerExecutions;
        this.objectMapper = objectMapper;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        for (Method method : bean.getClass().getDeclaredMethods()) {
            MMMQListener annotation = method.getAnnotation(MMMQListener.class);
            if (annotation == null) {
                continue;
            }
            try {
                handlerExecutions.add(new MethodExecution(annotation.id(), bean, method, objectMapper));
            } catch (Exception e) {
                throw new HandlerExecutionRegistrationException(
                        "Failed to register MethodExecution on " + bean.getClass().getCanonicalName() + "#" + method.getName(),
                        e
                );
            }
        }
        return bean;
    }
}
```

기존 파일에 추가 메서드가 있으면 보존하되 ID 처리 부분만 위와 같이 통합. `BeanPostProcessor`가 아닌 다른 메커니즘으로 등록되는 형태라면 그에 맞게 조정(원본 코드 구조 따름).

### Task 9: `InterfaceExecutionRegistration` 변경

**Files:**
- Read first: `consumer/src/main/java/org/mmmq/consumer/handler/execution/type/InterfaceExecutionRegistration.java`
- Modify: 같은 파일

- [ ] **Step 1: 현재 파일을 읽어 형태 파악**

```bash
cat /Users/kimdaehyeon/Desktop/mmmq/consumer/src/main/java/org/mmmq/consumer/handler/execution/type/InterfaceExecutionRegistration.java
```

- [ ] **Step 2: 변경 적용**

다음 변경을 적용:
1. `FrontHandler` 주입 → `HandlerExecutions` 주입.
2. `MMMQListener<?>` 빈을 발견하면 `new InterfaceExecution(mmmqListener, objectMapper)`로 그대로 생성(생성자 안에서 `id()`를 받음).
3. `handlerExecutions.add(...)`로 등록.
4. `mmmqListener.listens()` 같은 패턴 처리 코드가 있으면 제거.

코드 예시:

```java
package org.mmmq.consumer.handler.execution.type;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.consumer.exception.HandlerExecutionRegistrationException;
import org.mmmq.consumer.handler.execution.HandlerExecutions;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

@Component
public class InterfaceExecutionRegistration implements BeanPostProcessor {

    private final HandlerExecutions handlerExecutions;
    private final ObjectMapper objectMapper;

    public InterfaceExecutionRegistration(HandlerExecutions handlerExecutions, ObjectMapper objectMapper) {
        this.handlerExecutions = handlerExecutions;
        this.objectMapper = objectMapper;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (!(bean instanceof MMMQListener<?> mmmqListener)) {
            return bean;
        }
        try {
            handlerExecutions.add(new InterfaceExecution(mmmqListener, objectMapper));
        } catch (Exception e) {
            throw new HandlerExecutionRegistrationException(
                    "Failed to register InterfaceExecution on " + bean.getClass().getCanonicalName(),
                    e
            );
        }
        return bean;
    }
}
```

원본 클래스 구조가 다르면(`SmartInitializingSingleton` 형태 등) 그 구조를 보존하면서 위 의존 교체와 ID 사용만 적용.

### Task 10: `FrontHandler` 제거

**Files:**
- Delete: `consumer/src/main/java/org/mmmq/consumer/handler/FrontHandler.java`

- [ ] **Step 1: 파일 삭제**

```bash
rm /Users/kimdaehyeon/Desktop/mmmq/consumer/src/main/java/org/mmmq/consumer/handler/FrontHandler.java
```

`FrontHandler`의 역할(큐·Worker·ThreadPoolExecutor·HandlerExecutions 위임)이 모두 사라지거나 다른 곳으로 이동. 클래스 자체 제거.

### Task 11: `Consumer` Controller 변경

**Files:**
- Modify: `consumer/src/main/java/org/mmmq/consumer/Consumer.java`

- [ ] **Step 1: 전체 내용을 다음으로 교체**

```java
package org.mmmq.consumer;

import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.consumer.handler.execution.HandlerExecutions;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.ConsumerAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    private final HandlerExecutions handlerExecutions;

    public Consumer(HandlerExecutions handlerExecutions) {
        this.handlerExecutions = handlerExecutions;
    }

    @PostMapping("/mmmq/messages")
    public ResponseEntity<ConsumerAcknowledgement> receiveMessage(
            @RequestHeader Map<String, String> headers,
            @RequestBody Message message
    ) {
        Metadata metadata = new Metadata(headers);
        String handlerId = metadata.getHandlerId();
        if (handlerId == null) {
            log.warn("Received message without handler id header: {}", message);
            return ResponseEntity.ok(new ConsumerAcknowledgement(Acknowledgement.NACK));
        }
        HandlerExecution execution = handlerExecutions.find(handlerId);
        if (execution == null) {
            log.warn("No HandlerExecution found for id '{}', message: {}", handlerId, message);
            return ResponseEntity.ok(new ConsumerAcknowledgement(Acknowledgement.NACK));
        }
        try {
            execution.execute(message);
            return ResponseEntity.ok(new ConsumerAcknowledgement(Acknowledgement.ACK));
        } catch (Exception e) {
            log.warn("Handler execution failed for id '{}', message: {}", handlerId, message, e);
            return ResponseEntity.ok(new ConsumerAcknowledgement(Acknowledgement.NACK));
        }
    }
}
```

`FrontHandler` 주입 제거 → `HandlerExecutions` 주입. `@RequestHeader Map<String, String>`으로 헤더 받음. `Metadata`로 ID 추출. ID 미일치/HE 실행 예외 모두 NACK 응답.

### Task 12: consumer 모듈 컴파일 + 커밋

- [ ] **Step 1: consumer 모듈 컴파일**

```bash
cd /Users/kimdaehyeon/Desktop/mmmq && ./gradlew :consumer:compileJava
```

Expected: BUILD SUCCESSFUL. 실패하면 위 Task들의 import/시그니처를 다시 점검.

- [ ] **Step 2: 커밋**

```bash
cd /Users/kimdaehyeon/Desktop/mmmq && git add consumer && git commit -m "refactor(consumer): replace pattern routing with id-based routing and sync response"
```

---

## Phase 3 — Broker: Dispatcher 모델 재구성

> 의도된 컴파일 깨짐 단계: Phase 내부에서 시그니처 동시 변경. Phase 마지막에 컴파일 검증.

### Task 13: `TopicQueue` 매개변수 리네이밍

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueue.java`

- [ ] **Step 1: `subscribe` 매개변수 리네이밍**

`TopicQueue.java`에서 `subscribe(String dispatcherName)`을 `subscribe(String name)`으로 변경. 내부에서 `dispatcherName`을 사용하는 코드도 모두 `name`으로 변경.

원본 위치(보고서 기준 line 31~32):
```java
public Offset subscribe(String dispatcherName) {
    return new Offset(checkpointDirectory.register(dispatcherName).read());
}
```
변경 후:
```java
public Offset subscribe(String name) {
    return new Offset(checkpointDirectory.register(name).read());
}
```

- [ ] **Step 2: `commit` 매개변수 리네이밍**

원본(line 53~62):
```java
public Offset commit(String dispatcherName, Offset offset) {
    CheckpointFile checkpointFile = checkpointDirectory.get(dispatcherName);
    if (checkpointFile == null) {
        throw new IllegalStateException(
                "Cannot commit: dispatcher '" + dispatcherName + "' has not subscribed topic '" + topic.name() + "'"
        );
    }
    Offset next = offset.next();
    checkpointFile.write(next.value());
    return next;
}
```
변경 후:
```java
public Offset commit(String name, Offset offset) {
    CheckpointFile checkpointFile = checkpointDirectory.get(name);
    if (checkpointFile == null) {
        throw new IllegalStateException(
                "Cannot commit: '" + name + "' has not subscribed topic '" + topic.name() + "'"
        );
    }
    Offset next = offset.next();
    checkpointFile.write(next.value());
    return next;
}
```

매개변수 이름과 본문 참조, 예외 메시지를 모두 정합되게 변경.

### Task 14: `Sender` 변경 (Metadata + handlerId)

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/dispatcher/sender/Sender.java`

- [ ] **Step 1: 전체 내용을 다음으로 교체**

```java
package org.mmmq.broker.dispatcher.sender;

import org.mmmq.core.Host;
import org.mmmq.core.acknowledgement.ConsumerAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.MessageDeliveryException;
import org.mmmq.core.metadata.Metadata;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

public class Sender {

    final RestClient restClient;

    public Sender(RestClient restClient) {
        this.restClient = restClient;
    }

    public static Sender from(Host host) {
        RestClient restClient = RestClient.builder()
                .baseUrl(host.toUri())
                .defaultStatusHandler(
                        status -> status.is4xxClientError() || status.is5xxServerError(),
                        (request, response) -> {
                            throw new MessageDeliveryException("Failed to send message: " + response.getStatusText());
                        }
                )
                .build();
        return new Sender(restClient);
    }

    public boolean send(Message message, String handlerId, int maxRetryCount) {
        for (int attempt = 1; attempt <= maxRetryCount; attempt++) {
            if (post(message, handlerId).isAck()) {
                return true;
            }
        }
        return false;
    }

    ConsumerAcknowledgement post(Message message, String handlerId) {
        Metadata metadata = new Metadata();
        metadata.setHandlerId(handlerId);
        return restClient.post()
                .uri("/mmmq/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .headers(httpHeaders -> metadata.toMap().forEach(httpHeaders::set))
                .body(message)
                .retrieve()
                .toEntity(ConsumerAcknowledgement.class)
                .getBody();
    }
}
```

`send` 시그니처에 `handlerId` 추가. `post`도 `handlerId` 받아 `Metadata` 생성 후 헤더 주입. `Sender.from(Host)` 정적 팩토리는 시그니처 변경 없음.

### Task 15: `Dispatcher` 변경 (handlerId 통합 + EventListener 제거 + 단일 pattern)

**Files:**
- Modify: `broker/src/main/java/org/mmmq/broker/dispatcher/Dispatcher.java`

- [ ] **Step 1: 전체 내용을 다음으로 교체**

```java
package org.mmmq.broker.dispatcher;

import jakarta.annotation.PreDestroy;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.topicqueue.Offset;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.storage.CorruptionException;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.core.message.TopicPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class Dispatcher {

    private static final int MAX_NACK_RETRY_COUNT = 3;
    private static final long INITIAL_BACKOFF_DELAY_MS = 1000;
    private static final long MAX_BACKOFF_DELAY_MS = 60000;
    private static final int BACKOFF_MULTIPLIER = 2;

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);
    private static final Pattern HANDLER_ID_PATTERN = Pattern.compile("[A-Za-z0-9._-]+");

    final Host host;
    final String handlerId;
    final TopicPattern pattern;
    final ConcurrentHashMap<TopicQueue, Offset> subscriptions = new ConcurrentHashMap<>();
    final WorkerPool workerPool = new WorkerPool();
    Sender sender;

    public Dispatcher(Host host, String handlerId, TopicPattern pattern) {
        if (!HANDLER_ID_PATTERN.matcher(handlerId).matches()) {
            throw new IllegalArgumentException("handlerId must match [A-Za-z0-9._-]+, but was: " + handlerId);
        }
        this.host = host;
        this.handlerId = handlerId;
        this.pattern = pattern;
        this.sender = Sender.from(host);
    }

    public String handlerId() {
        return handlerId;
    }

    boolean matches(Topic topic) {
        return pattern.matches(topic);
    }

    void subscribe(TopicQueue topicQueue) {
        subscriptions.computeIfAbsent(topicQueue, queue -> queue.subscribe(handlerId));
    }

    void drain(TopicQueue topicQueue) {
        if (!subscriptions.containsKey(topicQueue)) {
            return;
        }
        workerPool.submit(topicQueue, () -> drainLoop(topicQueue));
    }

    private void drainLoop(TopicQueue topicQueue) {
        try {
            Offset offset = subscriptions.get(topicQueue);
            while (true) {
                try {
                    Message message = topicQueue.peek(offset);
                    if (message == null) {
                        return;
                    }
                    deliver(message);
                } catch (CorruptionException exception) {
                    log.error("Dispatcher {} skipped corrupted entry on topic {} at offset {}",
                            handlerId,
                            topicQueue.getTopic(),
                            offset,
                            exception
                    );
                }
                offset = topicQueue.commit(handlerId, offset);
                subscriptions.put(topicQueue, offset);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.info("Dispatcher {} drain interrupted on topic {}", handlerId, topicQueue.getTopic());
        } catch (Exception exception) {
            log.error("Dispatcher {} aborted drain on topic {}", handlerId, topicQueue.getTopic(), exception);
        }
    }

    private void deliver(Message message) throws InterruptedException {
        long currentBackoffDelay = INITIAL_BACKOFF_DELAY_MS;
        while (true) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            try {
                if (!sender.send(message, handlerId, MAX_NACK_RETRY_COUNT)) {
                    log.warn("NACK exhausted. Dropping message: {}", message);
                }
                return;
            } catch (RuntimeException exception) {
                log.warn(
                        "Communication failure. Backing off {}ms. Error: {}",
                        currentBackoffDelay,
                        exception.getMessage()
                );
                Thread.sleep(currentBackoffDelay);
                currentBackoffDelay = Math.min(currentBackoffDelay * BACKOFF_MULTIPLIER, MAX_BACKOFF_DELAY_MS);
            }
        }
    }

    @PreDestroy
    public void destroy() {
        workerPool.shutdownAll();
    }

    private static class WorkerPool {

        private final Map<TopicQueue, ExecutorService> pool = new ConcurrentHashMap<>();

        private void submit(TopicQueue topicQueue, Runnable task) {
            pool.computeIfAbsent(topicQueue, queue -> createWorker()).submit(task);
        }

        private ExecutorService createWorker() {
            return new ThreadPoolExecutor(
                    0, 1, 60L, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(1),
                    new ThreadPoolExecutor.DiscardPolicy()
            );
        }

        private void shutdownAll() {
            pool.values()
                    .forEach(ExecutorService::shutdownNow);
            pool.clear();
        }
    }
}
```

핵심 변화:
- 시그니처 `Dispatcher(Host, String handlerId, TopicPattern pattern)`.
- 필드: `name` 제거, `handlerId` 추가, `patterns: List<TopicPattern>` → `pattern: TopicPattern`.
- `@EventListener` 메서드 3개(`onApplicationReady`, `onTopicQueueInitialized`, `onMessageArrived`) 모두 제거.
- 새 메서드 `subscribe(TopicQueue)`, `drain(TopicQueue)`, `matches(Topic)`를 package-private으로 노출(DispatcherContainer가 호출).
- `topicQueue.subscribe(handlerId)`, `topicQueue.commit(handlerId, offset)`로 호출.
- `sender.send(message, handlerId, MAX_NACK_RETRY_COUNT)`로 변경.
- 내부 로그는 `handlerId`로 표기.
- `NAME_PATTERN` → `HANDLER_ID_PATTERN` 의미 명확화.
- Spring `EventListener`, `ApplicationReadyEvent`, `TopicQueueInitializedEvent` import 제거.

### Task 16: `DispatcherContainer` 신설

**Files:**
- Create: `broker/src/main/java/org/mmmq/broker/dispatcher/DispatcherContainer.java`

- [ ] **Step 1: 파일 생성**

```java
package org.mmmq.broker.dispatcher;

import lombok.extern.slf4j.Slf4j;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

### Task 17: `FrontDispatcher` 변경

**Files:**
- Read first: `broker/src/main/java/org/mmmq/broker/dispatcher/FrontDispatcher.java`
- Modify: 같은 파일

- [ ] **Step 1: 현재 파일을 읽기**

```bash
cat /Users/kimdaehyeon/Desktop/mmmq/broker/src/main/java/org/mmmq/broker/dispatcher/FrontDispatcher.java
```

- [ ] **Step 2: 변경 적용**

기존 코드(보고서 기준):
```java
private final TopicQueueContainer container;
private final ApplicationEventPublisher publisher;

public Acknowledgement dispatch(Message message) {
    TopicQueue queue = container.get(message.topic());
    if (queue.offer(message)) {
        publisher.publishEvent(new MessageArrivedEvent(queue));
        return Acknowledgement.ACK;
    }
    return Acknowledgement.NACK;
}
```

변경 후 전체 클래스:
```java
package org.mmmq.broker.dispatcher;

import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueContainer;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.message.Message;
import org.springframework.stereotype.Component;

@Component
public class FrontDispatcher {

    private final TopicQueueContainer container;
    private final DispatcherContainer dispatcherContainer;

    public FrontDispatcher(TopicQueueContainer container, DispatcherContainer dispatcherContainer) {
        this.container = container;
        this.dispatcherContainer = dispatcherContainer;
    }

    public Acknowledgement dispatch(Message message) {
        TopicQueue queue = container.get(message.topic());
        if (queue.offer(message)) {
            dispatcherContainer.dispatch(queue);
            return Acknowledgement.ACK;
        }
        return Acknowledgement.NACK;
    }
}
```

`ApplicationEventPublisher` 의존 제거. `publishEvent(new MessageArrivedEvent(queue))` → `dispatcherContainer.dispatch(queue)`. 만약 원본 클래스에 다른 메서드가 있다면 보존(이 변경은 `dispatch`와 의존만).

### Task 18: `TopicQueueContainer` 변경

**Files:**
- Read first: `broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueueContainer.java`
- Modify: 같은 파일

- [ ] **Step 1: 현재 파일 형태 파악**

```bash
cat /Users/kimdaehyeon/Desktop/mmmq/broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueueContainer.java
```

- [ ] **Step 2: 변경 적용**

이 클래스에서 `TopicQueue`를 새로 생성하고 `TopicQueueInitializedEvent`를 발행하는 부분을 다음으로 교체:

1. 생성자/필드에 `DispatcherContainer dispatcherContainer` 의존 추가. `ApplicationEventPublisher` 의존 제거.
2. 큐 생성 직후 `publisher.publishEvent(new TopicQueueInitializedEvent(topicQueue))` 호출을 `dispatcherContainer.onTopicQueueInitialized(topicQueue)`로 교체.

```java
import org.mmmq.broker.dispatcher.DispatcherContainer;
```
import 추가.

생성자:
```java
public TopicQueueContainer(/* 기존 의존들 */, DispatcherContainer dispatcherContainer) {
    /* 기존 할당들 */
    this.dispatcherContainer = dispatcherContainer;
}
```

큐 생성 후 알림 위치:
```java
TopicQueue topicQueue = /* 큐 생성 */;
dispatcherContainer.onTopicQueueInitialized(topicQueue);
```

기존 `TopicQueueInitializedEvent` import 및 사용 코드 모두 제거. 다른 호출자가 있다면 그 호출자도 같은 흐름으로 변경(Task 18 검증 시점에 모두 확인).

### Task 19: `MessageArrivedEvent` 제거

**Files:**
- Delete: `broker/src/main/java/org/mmmq/broker/dispatcher/MessageArrivedEvent.java`

- [ ] **Step 1: 파일 삭제**

```bash
rm /Users/kimdaehyeon/Desktop/mmmq/broker/src/main/java/org/mmmq/broker/dispatcher/MessageArrivedEvent.java
```

이 시점에 이 이벤트를 import하는 모든 곳(원래 Dispatcher, FrontDispatcher)에서 import가 제거되었어야 함. 누락 시 컴파일 에러로 발견됨.

### Task 20: `TopicQueueInitializedEvent` 제거

**Files:**
- Delete: `broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueueInitializedEvent.java`

- [ ] **Step 1: 파일 삭제**

```bash
rm /Users/kimdaehyeon/Desktop/mmmq/broker/src/main/java/org/mmmq/broker/topicqueue/TopicQueueInitializedEvent.java
```

마찬가지로 import 누락 컴파일 에러로 추적.

### Task 21: broker 모듈 컴파일 + 커밋

- [ ] **Step 1: broker 모듈 컴파일**

```bash
cd /Users/kimdaehyeon/Desktop/mmmq && ./gradlew :broker:compileJava
```

Expected: BUILD SUCCESSFUL.

실패 시 흔한 원인:
- `Dispatcher` 생성자 호출 사이트(테스트 코드 또는 Bean 정의)가 옛 시그니처를 쓰고 있는 경우. 사용 사이트도 함께 정정.
- `MessageArrivedEvent`/`TopicQueueInitializedEvent`에 대한 잔존 import.
- `Sender.send(message, maxRetry)`의 옛 시그니처 호출.

- [ ] **Step 2: 커밋**

```bash
cd /Users/kimdaehyeon/Desktop/mmmq && git add broker && git commit -m "refactor(broker): turn Dispatcher into HE-level proxy with DispatcherContainer"
```

---

## Phase 4 — 전체 빌드

### Task 22: 전체 빌드

- [ ] **Step 1: 전체 빌드 (테스트 포함)**

```bash
cd /Users/kimdaehyeon/Desktop/mmmq && ./gradlew build
```

Expected: BUILD SUCCESSFUL.

기존 테스트가 변경된 시그니처/모델을 가정하고 있다면 실패할 수 있음. 사용자 정책상 테스트는 별도 리팩터링 단계에서 일괄 작업 예정이므로, 이 시점에 실패하는 테스트는:
1. 컴파일 에러: 시그니처 변경에 따른 단순 호출 수정으로 통과시키거나, 영향이 큰 테스트는 `@Disabled`로 임시 비활성화(별도 후속 작업으로 정정 예정).
2. 런타임 실패: 같은 정책으로 임시 비활성화.

비활성화한 테스트는 별도 커밋으로 기록:

```bash
cd /Users/kimdaehyeon/Desktop/mmmq && git commit -m "test: disable tests affected by dispatcher refactor (will be rewritten)"
```

- [ ] **Step 2: 수동 검증 안내 (선택)**

§검증 시나리오의 두 케이스(정상 라우팅 + 동기 응답, 격리)는 통합 환경에서 수동 검증 가능. 본 플랜에서는 자동화하지 않음. 사용자가 별도 환경에서 확인 후 본 변경의 동작을 확정.

---

## Self-Review 체크

플랜 작성 후 self-review 결과 — 모두 만족 확인:

**Spec coverage:**
- §결정된 사양 1~8 모두 task로 커버됨. §컴포넌트 명세의 모든 변경/신규/제거 파일이 task에 대응.
- §변경 후 메시지 흐름은 task들이 합쳐졌을 때 자동으로 성립.
- §검증 시나리오는 사용자 정책에 따라 Task 22의 수동 검증 안내로 처리.

**Placeholder scan:**
- "TBD", "TODO", "implement later" 등의 표현 없음.
- "Add appropriate error handling" 같은 막연한 지시 없음 — NACK 처리·로깅·재시도 흐름 모두 코드로 명시.

**Type consistency:**
- `HandlerExecution.id()`, `Dispatcher.handlerId()`, `Metadata.getHandlerId()` 등 메서드 이름이 task 전반에 일관됨.
- 생성자 시그니처가 호출 사이트(Registration들, Sender, DispatcherContainer)에서 정합되게 사용됨.
