package org.mmmq.consumer.handler.execution.method;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import org.mmmq.consumer.exception.HandlerExecutionException;
import org.mmmq.consumer.exception.InvalidHandlerException;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.Message;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.BridgeMethodResolver;

class MethodExecution implements HandlerExecution {

    private final ConsumerId id;
    private final Object bean;
    private final Method method;
    private final JavaType parameterType;
    private final ObjectMapper objectMapper;

    MethodExecution(ConsumerId id, Object bean, Method targetMethod, ObjectMapper objectMapper) {
        this.id = id;
        this.bean = bean;
        this.method = getInvocableMethod(bean, targetMethod);
        this.method.setAccessible(true);
        this.objectMapper = objectMapper;
        this.parameterType = getParameterType(targetMethod, objectMapper);
    }

    private Method getInvocableMethod(Object bean, Method targetMethod) {
        try {
            // bean에서 targetMethod를 호출할 수 있는 실제 메서드를 가져온다.
            // 프록시가 적용되지 않은 경우, targetMethod가 그대로 반환된다.
            // 프록시가 적용된 경우, targetMethod에 대응하는 실제 호출 가능한 메서드를 찾는다.
            return AopUtils.selectInvocableMethod(targetMethod, bean.getClass());
        } catch (IllegalStateException exception) {
            // OrderListener implements Listener<Order>라면, targetMethod는 OrderListener.onMessage(Order)이다.
            // JDK 프록시에는 타입 소거된 Listener.onMessage(Object)만 노출되므로, targetMethod는 호출할 수 없어 IllegalStateException이 발생한다.

            // targetMethod에 대응하는 Bridge 메서드를 찾아서, Bridge 메서드의 시그니처로 프록시 클래스에서 Listener.onMessage(Object)를 선택한다.
            return findBridgeMethod(targetMethod)
                    .map(bridgeMethod -> AopUtils.selectInvocableMethod(bridgeMethod, bean.getClass()))
                    // targetMethod가 구현 클래스에만 존재하는 메서드라면, JDK 프록시 클래스에는 해당 메서드가 존재하지 않으므로 등록에 실패한다.
                    .orElseThrow(() -> exception);
        }
    }

    // 제네릭 타입 소거로 생성된 Bridge 메서드 중 targetMethod에 대응하는 것을 찾는다.
    // targetMethod가 onMessage(Order) 라면, Bridge 메서드는 onMessage(Object) 이다.
    private Optional<Method> findBridgeMethod(Method targetMethod) {
        return Arrays.stream(targetMethod.getDeclaringClass().getDeclaredMethods())
                .filter(Method::isBridge)
                .filter(bridge -> BridgeMethodResolver.findBridgedMethod(bridge).equals(targetMethod))
                .findFirst();
    }

    private JavaType getParameterType(Method method, ObjectMapper objectMapper) {
        if (method.getParameterCount() != 1) {
            throw new InvalidHandlerException("MethodExecution must have exactly one parameter: " + id);
        }
        return objectMapper.constructType(method.getGenericParameterTypes()[0]);
    }

    @Override
    public ConsumerId id() {
        return id;
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
