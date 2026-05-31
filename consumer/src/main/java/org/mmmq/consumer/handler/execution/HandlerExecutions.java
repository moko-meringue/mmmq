package org.mmmq.consumer.handler.execution;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Component
public class HandlerExecutions {

    private final Map<String, HandlerExecution> handlerIdToExecution = new ConcurrentHashMap<>();

    public void add(HandlerExecution handlerExecution) {
        HandlerExecution previous = handlerIdToExecution.putIfAbsent(handlerExecution.id(), handlerExecution);
        if (previous != null) {
            throw new IllegalStateException(
                    "Duplicate HandlerExecution id '" + handlerExecution.id() + "'"
            );
        }
    }

    @Nullable
    public HandlerExecution find(String id) {
        return handlerIdToExecution.get(id);
    }

    public int size() {
        return handlerIdToExecution.size();
    }
}
