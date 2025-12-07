package org.mmmq.subscriber;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.core.message.Message;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

class MethodMessageHandler extends MessageHandler {

    final Object bean;
    final Method method;
    final JavaType parameterType;
    final ObjectMapper objectMapper;

    MethodMessageHandler(String topic, Object bean, Method method, ObjectMapper objectMapper) {
        super(bean.getClass().getCanonicalName() + "#" + method.getName(), topic);
        method.setAccessible(true);
        this.bean = bean;
        this.method = method;
        this.objectMapper = objectMapper;
        this.parameterType = getParameterType(method, objectMapper);
    }

    private JavaType getParameterType(Method method, ObjectMapper objectMapper) {
        if (method.getParameterCount() != 1) {
            throw new InvalidMessageHandlerException("HandlerMethod must have exactly one parameter: " + name);
        }
        return objectMapper.constructType(method.getGenericParameterTypes()[0]);
    }

    @Override
    public void handle(Message message) {
        Object parameter = getParameter(message);

        try {
            method.invoke(bean, parameter);
        } catch (InvocationTargetException e) {
            throw new MessageHandlerExecutionException(
                    "Handler " + name + "threw an exception while processing.",
                    e.getCause()
            );
        } catch (Exception e) {
            throw new MessageHandlerExecutionException(
                    String.format("Unexpected error occurred during execute handler %s: %s", name, e),
                    e
            );
        }
    }

    private Object getParameter(Message message) {
        try {
            return objectMapper.convertValue(message.content(), parameterType);
        } catch (IllegalArgumentException e) {
            throw new MessageHandlerExecutionException(
                    String.format("Failed to convert parameter for handler '%s': %s", name, e.getMessage()),
                    e
            );
        }
    }
}
