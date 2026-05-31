package org.mmmq.consumer.handler.execution;

import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class HandlerExecutions {

    private final Map<String, HandlerExecution> byId = new ConcurrentHashMap<>();

    public void add(HandlerExecution handlerExecution) {
        HandlerExecution previous = byId.putIfAbsent(handlerExecution.id(), handlerExecution);
        if (previous != null) {
            throw new IllegalStateException(
                    "Duplicate HandlerExecution id '" + handlerExecution.id() + "'"
            );
        }
    }

    @Nullable
    public HandlerExecution find(String id) {
        return byId.get(id);
    }

    public int size() {
        return byId.size();
    }
}
