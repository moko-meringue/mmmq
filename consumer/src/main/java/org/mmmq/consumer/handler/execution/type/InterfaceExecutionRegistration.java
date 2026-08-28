package org.mmmq.consumer.handler.execution.type;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.consumer.exception.HandlerExecutionRegistrationException;
import org.mmmq.consumer.handler.execution.HandlerExecutionContainer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class InterfaceExecutionRegistration implements BeanPostProcessor, SmartInitializingSingleton {

    private final List<MMMQListener<?>> candidates = new CopyOnWriteArrayList<>();
    private final ObjectProvider<HandlerExecutionContainer> handlerExecutionContainerProvider;
    private final ObjectProvider<ObjectMapper> objectMapperProvider;

    public InterfaceExecutionRegistration(
            ObjectProvider<HandlerExecutionContainer> handlerExecutionContainerProvider,
            ObjectProvider<ObjectMapper> objectMapperProvider
    ) {
        this.handlerExecutionContainerProvider = handlerExecutionContainerProvider;
        this.objectMapperProvider = objectMapperProvider;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (bean instanceof MMMQListener<?> mmmqListener) {
            candidates.add(mmmqListener);
        }
        return bean;
    }

    @Override
    public void afterSingletonsInstantiated() {
        HandlerExecutionContainer handlerExecutionContainer = handlerExecutionContainerProvider.getObject();
        ObjectMapper objectMapper = objectMapperProvider.getObject();
        candidates.forEach(candidate -> register(candidate, handlerExecutionContainer, objectMapper));
        candidates.clear();
    }

    private void register(
            MMMQListener<?> mmmqListener,
            HandlerExecutionContainer handlerExecutionContainer,
            ObjectMapper objectMapper
    ) {
        try {
            handlerExecutionContainer.add(new InterfaceExecution(mmmqListener, objectMapper));
        } catch (Exception e) {
            throw new HandlerExecutionRegistrationException(
                    "Failed to register InterfaceExecution on " + mmmqListener.getClass().getCanonicalName(),
                    e
            );
        }
    }
}
