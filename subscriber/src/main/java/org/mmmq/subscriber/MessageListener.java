package org.mmmq.subscriber;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
class MessageListener implements ApplicationListener<MMMQEvent> {

    final ObjectMapper objectMapper = new ObjectMapper();
    final Map<String, Execution> executionMap = new HashMap<>();
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(
        2,
        5,
        40L,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>()
    );

    void addTopic(String topic, Execution execution) {
        if (executionMap.containsKey(topic)) {
            throw new DuplicateTopicException("Duplicate topic: " + topic);
        }
        executionMap.put(topic, execution);
    }

    @Override
    public void onApplicationEvent(MMMQEvent event) {
        Optional.ofNullable(executionMap.get(event.message.topic()))
            .ifPresent(execution -> executor.execute(() -> invokeListenerMethodAsync(execution, event)));
    }

    private void invokeListenerMethodAsync(Execution execution, MMMQEvent mmmqEvent) {
        try {
            Object parameter = objectMapper.convertValue(mmmqEvent.message.content(), execution.javaType());
            execution.method().invoke(execution.bean(), parameter);
        } catch (IllegalArgumentException e) {
            throw new MessageConversionException(
                String.format(
                    "Failed to convert Message topic: %s, expected type: %s",
                    mmmqEvent.message.topic(),
                    execution.javaType().getRawClass().getName()
                ), e
            );
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            throw new ListenerExecutionException(
                "Failed to invoke listener method" + ": " + cause.getMessage(), cause);
        } catch (Exception e) {
            throw new ListenerExecutionException(
                "Unexpected error during listener method invocation: " + e.getMessage(), e
            );
        }
    }

    private record Execution(
        Object bean,
        Method method,
        JavaType javaType
    ) {}
}
