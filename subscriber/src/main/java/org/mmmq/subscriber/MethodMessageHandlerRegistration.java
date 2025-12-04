package org.mmmq.subscriber;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
class MethodMessageHandlerRegistration implements BeanPostProcessor, SmartInitializingSingleton {

    final ObjectMapper objectMapper = new ObjectMapper();
    final List<MethodMessageHandler> messageHandlers = new ArrayList<>();
    final ObjectProvider<MessageReceivedEventHandler> messageReceivedEventHandlerProvider;

    MethodMessageHandlerRegistration(ObjectProvider<MessageReceivedEventHandler> messageReceivedEventHandlerProvider) {
        this.messageReceivedEventHandlerProvider = messageReceivedEventHandlerProvider;
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
        try {
            messageReceivedEventHandlerProvider.ifAvailable(
                    messageReceivedEventHandler ->
                            messageHandlers.forEach(messageReceivedEventHandler::addMessageHandler)
            );
            messageHandlers.clear();
        } catch (BeansException e) {
            throw new MessageHandlerRegistrationException("Failed to register message handlers.", e);
        } catch (Exception e) {
            throw new MessageHandlerRegistrationException("Unexpected error during message handler registration.", e);
        }
    }
}
