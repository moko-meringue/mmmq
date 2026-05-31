package org.mmmq.consumer.handler.execution.method;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.consumer.exception.HandlerExecutionException;
import org.mmmq.consumer.exception.InvalidHandlerException;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.core.message.Message;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

class MethodExecution extends HandlerExecution {

    final Object bean;
    final Method method;
    final JavaType parameterType;
    final ObjectMapper objectMapper;

    MethodExecution(String id, Object bean, Method method, ObjectMapper objectMapper) {
        super(id);
        method.setAccessible(true);
        this.bean = bean;
        this.method = method;
        this.objectMapper = objectMapper;
        this.parameterType = getParameterType(method, objectMapper);
    }

    private JavaType getParameterType(Method method, ObjectMapper objectMapper) {
        if (method.getParameterCount() != 1) {
            throw new InvalidHandlerException("MethodExecution must have exactly one parameter: " + id);
        }
        return objectMapper.constructType(method.getGenericParameterTypes()[0]);
    }

    @Override
    public void execute(Message message) {
        Object parameter = getParameter(message);

        try {
            method.invoke(bean, parameter);
        } catch (InvocationTargetException e) {
            throw new HandlerExecutionException(
                    "MethodExecution " + id + " threw an exception while processing.",
                    e.getCause()
            );
        } catch (Exception e) {
            throw new HandlerExecutionException(
                    String.format("Unexpected error occurred during execute handler execution %s: %s", id, e),
                    e
            );
        }
    }

    private Object getParameter(Message message) {
        if (message.content() == null) {
            return null;
        }
        try {
            return objectMapper.convertValue(message.content(), parameterType);
        } catch (IllegalArgumentException e) {
            throw new HandlerExecutionException(
                    String.format("Failed to convert parameter for handler execution '%s': %s", id, e.getMessage()),
                    e
            );
        }
    }
}
