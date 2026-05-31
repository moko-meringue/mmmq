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
