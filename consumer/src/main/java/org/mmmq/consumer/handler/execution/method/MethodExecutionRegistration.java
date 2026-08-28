package org.mmmq.consumer.handler.execution.method;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.consumer.exception.HandlerExecutionRegistrationException;
import org.mmmq.consumer.handler.execution.HandlerExecutionContainer;
import org.mmmq.core.identifier.ConsumerId;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class MethodExecutionRegistration implements BeanPostProcessor, SmartInitializingSingleton {

    private final List<Candidate> candidates = new CopyOnWriteArrayList<>();
    private final ObjectProvider<HandlerExecutionContainer> handlerExecutionContainerProvider;
    private final ObjectProvider<ObjectMapper> objectMapperProvider;

    public MethodExecutionRegistration(
            ObjectProvider<HandlerExecutionContainer> handlerExecutionContainerProvider,
            ObjectProvider<ObjectMapper> objectMapperProvider
    ) {
        this.handlerExecutionContainerProvider = handlerExecutionContainerProvider;
        this.objectMapperProvider = objectMapperProvider;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        for (Method method : bean.getClass().getDeclaredMethods()) {
            MMMQListener annotation = method.getAnnotation(MMMQListener.class);
            if (annotation != null) {
                candidates.add(new Candidate(bean, method, annotation.id()));
            }
        }
        return bean;
    }

    @Override
    public void afterSingletonsInstantiated() {
        HandlerExecutionContainer handlerExecutionContainer = handlerExecutionContainerProvider.getObject();
        ObjectMapper objectMapper = objectMapperProvider.getObject();
        candidates.forEach(candidate -> candidate.register(handlerExecutionContainer, objectMapper));
        candidates.clear();
    }

    private record Candidate(
            Object bean,
            Method method,
            String id
    ) {

        private void register(HandlerExecutionContainer handlerExecutionContainer, ObjectMapper objectMapper) {
            try {
                handlerExecutionContainer.add(new MethodExecution(new ConsumerId(id), bean, method, objectMapper));
            } catch (Exception e) {
                throw new HandlerExecutionRegistrationException(
                        "Failed to register MethodExecution on " + bean.getClass().getCanonicalName()
                                + "#" + method.getName(),
                        e
                );
            }
        }
    }
}
