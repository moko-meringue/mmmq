package org.mmmq.consumer.handler.execution.type;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.consumer.exception.HandlerExecutionException;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.core.message.Message;
import org.springframework.core.GenericTypeResolver;
import org.springframework.util.ClassUtils;

class InterfaceExecution implements HandlerExecution {

    private final String id;
    private final MMMQListener<Object> mmmqListener;
    private final JavaType parameterType;
    private final ObjectMapper objectMapper;

    @SuppressWarnings("unchecked")
    InterfaceExecution(MMMQListener<?> mmmqListener, ObjectMapper objectMapper) {
        this.id = mmmqListener.id();
        this.mmmqListener = (MMMQListener<Object>) mmmqListener;
        this.objectMapper = objectMapper;
        this.parameterType = resolveParameterType(mmmqListener, objectMapper);
    }

    private JavaType resolveParameterType(MMMQListener<?> mmmqListener, ObjectMapper objectMapper) {
        Class<?> userClass = ClassUtils.getUserClass(mmmqListener);
        Class<?> genericType = GenericTypeResolver.resolveTypeArgument(userClass, MMMQListener.class);

        return objectMapper.constructType(genericType != null ? genericType : Object.class);
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public void execute(Message message) {
        Object content = getParameter(message);
        try {
            mmmqListener.handle(content);
        } catch (Exception e) {
            throw new HandlerExecutionException(
                    String.format("Unexpected error during interface execution %s: %s", id, e),
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
                    String.format("Failed to convert parameter for interface execution '%s': %s", id, e.getMessage()),
                    e
            );
        }
    }
}
