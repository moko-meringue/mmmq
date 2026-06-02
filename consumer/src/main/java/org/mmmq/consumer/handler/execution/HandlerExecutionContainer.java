package org.mmmq.consumer.handler.execution;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.annotation.Nullable;
import org.springframework.stereotype.Component;

@Component
public class HandlerExecutionContainer {

    private final Map<ConsumerId, HandlerExecution> handlerExecutions = new ConcurrentHashMap<>();

    public void add(HandlerExecution handlerExecution) {
        HandlerExecution previous = handlerExecutions.putIfAbsent(handlerExecution.id(), handlerExecution);
        if (previous != null) {
            throw new IllegalStateException(
                    "Duplicate HandlerExecution id '" + handlerExecution.id() + "'"
            );
        }
    }

    @Nullable
    public HandlerExecution find(ConsumerId id) {
        return handlerExecutions.get(id);
    }
}
