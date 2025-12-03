package org.mmmq.subscriber;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

import org.mmmq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

@Component
class MessageListenerRegistration implements BeanPostProcessor {

    private static final Logger log = LoggerFactory.getLogger(MessageListenerRegistration.class);
    final ObjectProvider<MessageListener> messageListenerProvider;
    MessageListener messageListener = null;

    MessageListenerRegistration(ObjectProvider<MessageListener> messageListenerProvider) {
        this.messageListenerProvider = messageListenerProvider;
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
        if(this.messageListener == null) {
            this.messageListener = messageListenerProvider.getObject();
        }
        // ApplicationEventMulticaster multicaster = applicationContext.getBean(
        //         AbstractApplicationContext.APPLICATION_EVENT_MULTICASTER_BEAN_NAME,
        //         ApplicationEventMulticaster.class
        // );
        validateFormat(bean, method);
        method.setAccessible(true);
        Type genericParameterType = method.getGenericParameterTypes()[0];
        // JavaType cachedJavaType = objectMapper.constructType(genericParameterType);

        // messageListener.addTopic(annotation.topic(), ...);
        // multicaster.addApplicationListener(listener);
    }

    private static void validateFormat(Object bean, Method method) {
        if (method.getParameterCount() != 1) {
            throw new InvalidMessageListenerException(
                "@MMMQListener method must have exactly one parameter: "
                    + bean.getClass().getName() + "#"
                    + method.getName()
            );
        }
        if (!method.getParameterTypes()[0].equals(Message.class)) {
            throw new InvalidMessageListenerException(
                "@MMMQListener method parameter must be of type Message: "
                    + bean.getClass().getName() + "#"
                    + method.getName()
            );
        }
    }
}
