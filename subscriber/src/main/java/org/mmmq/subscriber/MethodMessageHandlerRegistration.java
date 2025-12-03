package org.mmmq.subscriber;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

@Component
class MethodMessageHandlerRegistration implements BeanPostProcessor, SmartInitializingSingleton {

    final ObjectMapper objectMapper = new ObjectMapper();
    final List<MethodMessageHandler> messageHandlers = new ArrayList<>();
    final ObjectProvider<MessageReceivedEventHandler> messageReceivedEventHandlerProvider;

    MethodMessageHandlerRegistration(ObjectProvider<MessageReceivedEventHandler> messageReceivedEventHandlerProvider) {
        this.messageReceivedEventHandlerProvider = messageReceivedEventHandlerProvider;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, @NonNull String beanName) {
        Method[] methods = ReflectionUtils.getAllDeclaredMethods(bean.getClass());

        for (Method method : methods) {
            MMMQListener annotation = AnnotationUtils.findAnnotation(method, MMMQListener.class);
            if (annotation != null) {
                registerMessageListener(bean, method, annotation);
            }
        }
        return bean;
    }

    private void registerMessageListener(Object bean, Method method, MMMQListener annotation) {
        MethodMessageHandler messageHandler = new MethodMessageHandler(
                annotation.topic(),
                bean,
                method,
                objectMapper
        );
        messageHandlers.add(messageHandler);
    }

    @Override
    public void afterSingletonsInstantiated() {
        messageReceivedEventHandlerProvider.ifAvailable(
                messageReceivedEventHandler -> messageHandlers.forEach(messageReceivedEventHandler::addMessageHandler));
        messageHandlers.clear();
    }
}
