package org.mmmq.consumer.handler.execution.type;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.consumer.exception.HandlerExecutionRegistrationException;
import org.mmmq.consumer.handler.FrontHandler;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
class InterfaceExecutionRegistration implements BeanPostProcessor, SmartInitializingSingleton {

    final ObjectMapper objectMapper = new ObjectMapper();
    final List<InterfaceExecution> interfaceExecutions = new ArrayList<>();
    final ObjectProvider<FrontHandler> frontHandlerObjectProvider;

    InterfaceExecutionRegistration(ObjectProvider<FrontHandler> frontHandlerObjectProvider) {
        this.frontHandlerObjectProvider = frontHandlerObjectProvider;
    }

    @Override
    public Object postProcessAfterInitialization(@NonNull Object bean, @NonNull String beanName) {
        if (bean instanceof MMMQListener) {
            interfaceExecutions.add(new InterfaceExecution((MMMQListener<?>) bean, objectMapper));
        }
        return bean;
    }

    @Override
    public void afterSingletonsInstantiated() {
        try {
            frontHandlerObjectProvider.ifAvailable(
                    frontHandler -> interfaceExecutions.forEach(frontHandler::addHandlerExecution)
            );
            interfaceExecutions.clear();
        } catch (BeansException e) {
            throw new HandlerExecutionRegistrationException("Failed to register InterfaceExecution.", e);
        } catch (Exception e) {
            throw new HandlerExecutionRegistrationException(
                    "Unexpected error during InterfaceExecution registration.",
                    e
            );
        }
    }
}
