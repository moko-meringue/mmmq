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
