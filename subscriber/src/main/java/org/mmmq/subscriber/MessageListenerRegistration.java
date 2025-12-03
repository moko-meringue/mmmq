package org.mmmq.subscriber;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
class MessageListenerRegistration implements BeanPostProcessor {

    static final ThreadPoolExecutor DEFAULT_THREAD_POOL_EXECUTOR = new ThreadPoolExecutor(
        2,
        5,
        40L,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(5)
    );

    final ApplicationContext applicationContext;
    final ObjectMapper objectMapper = new ObjectMapper();

    MessageListenerRegistration(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    private static void validateFormat(Object bean, Method method) {
        if (method.getParameterCount() != 1) {
            throw new InvalidMessageListenerException(
                    "@MMMQListener method must have exactly one parameter: "
                            + bean.getClass().getName() + "#"
                            + method.getName()
            );
        }
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
        ApplicationEventMulticaster multicaster = applicationContext.getBean(
                AbstractApplicationContext.APPLICATION_EVENT_MULTICASTER_BEAN_NAME,
                ApplicationEventMulticaster.class
        );
        validateFormat(bean, method);
        method.setAccessible(true);
        Type genericParameterType = method.getGenericParameterTypes()[0];
        JavaType cachedJavaType = objectMapper.constructType(genericParameterType);

        ApplicationListener<MMMQEvent> listener = new MessageListener(
                annotation.topic(),
                bean,
                method,
                cachedJavaType,
                objectMapper,
                DEFAULT_THREAD_POOL_EXECUTOR
        );

        multicaster.addApplicationListener(listener);
    }
}
