package org.mmmq.consumer.handler.execution.method;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import org.mmmq.consumer.exception.HandlerExecutionRegistrationException;
import org.mmmq.consumer.handler.execution.HandlerExecutionContainer;
import org.mmmq.core.identifier.ConsumerId;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class MethodExecutionRegistration implements SmartInitializingSingleton {

    private final ApplicationContext applicationContext;
    private final HandlerExecutionContainer handlerExecutionContainer;
    private final ObjectMapper objectMapper;

    public MethodExecutionRegistration(
            ApplicationContext applicationContext,
            HandlerExecutionContainer handlerExecutionContainer,
            ObjectMapper objectMapper
    ) {
        this.applicationContext = applicationContext;
        this.handlerExecutionContainer = handlerExecutionContainer;
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterSingletonsInstantiated() {
        try {
            Arrays.stream(applicationContext.getBeanNamesForType(Object.class, false, false))
                    .map(applicationContext::getBean)
                    .forEach(this::registerMethodExecutions);
        } catch (Exception exception) {
            throw new HandlerExecutionRegistrationException("Failed to register MethodExecutions", exception);
        }
    }

    private void registerMethodExecutions(Object bean) {
        try {
            Arrays.stream(AopUtils.getTargetClass(bean).getDeclaredMethods())
                    // 제네릭 타입 소거나 공변 반환 타입으로 인해 생성된 Bridge 메서드는 제외하고 실제 메서드만 등록되도록 필터링한다.
                    // JDK7u80 이후 javac는 Bridge 메서에도 원본 메서드의 어노테이션을 붙이기 때문에 핸들러 중복 등록을 방지하기 위함.
                    .filter(method -> !method.isBridge())
                    .filter(method -> !Modifier.isPrivate(method.getModifiers()) && !Modifier.isFinal(
                            method.getModifiers()))
                    .filter(method -> method.isAnnotationPresent(MMMQListener.class))
                    .map(method -> createMethodExecution(bean, method))
                    .forEach(handlerExecutionContainer::add);
        } catch (Exception exception) {
            throw new HandlerExecutionRegistrationException(
                    "Failed to register MethodExecutions for bean: " + bean.getClass().getCanonicalName(),
                    exception
            );
        }
    }

    private MethodExecution createMethodExecution(Object bean, Method method) {
        return new MethodExecution(
                new ConsumerId(method.getAnnotation(MMMQListener.class).id()),
                bean,
                method,
                objectMapper
        );
    }
}
