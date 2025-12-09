package org.mmmq.consumer.handler.execution.method;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.mmmq.consumer.exception.HandlerExecutionRegistrationException;
import org.mmmq.consumer.handler.FrontHandler;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
class MethodExecutionRegistration implements BeanPostProcessor, SmartInitializingSingleton {

    final ObjectMapper objectMapper = new ObjectMapper();
    final List<MethodExecution> methodExecutions = new ArrayList<>();
    final ObjectProvider<FrontHandler> frontHandlerObjectProvider;

    MethodExecutionRegistration(ObjectProvider<FrontHandler> frontHandlerObjectProvider) {
        this.frontHandlerObjectProvider = frontHandlerObjectProvider;
    }

    @Override
    public Object postProcessAfterInitialization(@NonNull Object bean, @NonNull String beanName) {
        ReflectionUtils.doWithMethods(
                ClassUtils.getUserClass(bean),
                method -> Optional.ofNullable(AnnotatedElementUtils.findMergedAnnotation(method, MMMQListener.class))
                        .ifPresent(annotation -> registerMessageListener(bean, method, annotation))
        );
        return bean;
    }

    private void registerMessageListener(Object bean, Method method, MMMQListener annotation) {
        MethodExecution methodExecution = new MethodExecution(
                annotation.topic(),
                bean,
                method,
                objectMapper
        );
        methodExecutions.add(methodExecution);
    }

    @Override
    public void afterSingletonsInstantiated() {
        try {
            frontHandlerObjectProvider.ifAvailable(
                frontHandler -> methodExecutions.forEach(frontHandler::addHandlerExecutions)
            );
            methodExecutions.clear();
        } catch (BeansException e) {
            throw new HandlerExecutionRegistrationException("Failed to register HandlerExecution.", e);
        } catch (Exception e) {
            throw new HandlerExecutionRegistrationException("Unexpected error during HandlerExecution registration.",
                e);
        }
    }
}
