package org.mmmq.consumer.handler.execution.type;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.consumer.exception.HandlerExecutionException;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.core.GenericTypeResolver;
import org.springframework.util.ClassUtils;

class InterfaceExecution extends HandlerExecution {

    final MMMQListener<Object> mmmqListener;
    final JavaType parameterType;
    final ObjectMapper objectMapper;

    @SuppressWarnings("unchecked")
    InterfaceExecution(MMMQListener<?> mmmqListener, ObjectMapper objectMapper) {
        super(
                ClassUtils.getUserClass(mmmqListener).getCanonicalName(),
                new Topic(mmmqListener.listens())
        );
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
    public void execute(Message message) {
        Object content = getParameter(message);
        try {
            mmmqListener.handle(content);
        } catch (Exception e) {
            throw new HandlerExecutionException(
                    String.format("Unexpected error during interface execution %s: %s", name, e),
                    e
            );
        }
    }

    private Object getParameter(Message message) {
        try {
            return objectMapper.convertValue(message.content(), parameterType);
        } catch (IllegalArgumentException e) {
            throw new HandlerExecutionException(
                    String.format("Failed to convert parameter for interface execution '%s': %s", name, e.getMessage()),
                    e
            );
        }
    }
}
