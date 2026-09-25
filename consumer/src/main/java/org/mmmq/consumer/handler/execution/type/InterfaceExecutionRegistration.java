package org.mmmq.consumer.handler.execution.type;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import org.mmmq.consumer.exception.HandlerExecutionRegistrationException;
import org.mmmq.consumer.handler.execution.HandlerExecutionContainer;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class InterfaceExecutionRegistration implements SmartInitializingSingleton {

    private final ApplicationContext applicationContext;
    private final HandlerExecutionContainer handlerExecutionContainer;
    private final ObjectMapper objectMapper;

    public InterfaceExecutionRegistration(
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
            Arrays.stream(applicationContext.getBeanNamesForType(MMMQListener.class, false, false))
                    .map(beanName -> applicationContext.getBean(beanName, MMMQListener.class))
                    .forEach(this::registerInterfaceExecution);
        } catch (Exception exception) {
            throw new HandlerExecutionRegistrationException("Failed to register InterfaceExecutions", exception);
        }
    }

    private void registerInterfaceExecution(MMMQListener<?> mmmqListener) {
        try {
            handlerExecutionContainer.add(new InterfaceExecution(mmmqListener, objectMapper));
        } catch (Exception exception) {
            throw new HandlerExecutionRegistrationException(
                    "Failed to register InterfaceExecution for bean: " + mmmqListener.getClass().getCanonicalName(),
                    exception
            );
        }
    }
}
