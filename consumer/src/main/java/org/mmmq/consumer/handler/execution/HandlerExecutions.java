package org.mmmq.consumer.handler.execution;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Component
public class HandlerExecutions {

    private final Map<String, HandlerExecution> byHandlerId = new ConcurrentHashMap<>();

    public void add(HandlerExecution handlerExecution) {
        HandlerExecution previous = byHandlerId.putIfAbsent(handlerExecution.id(), handlerExecution);
        if (previous != null) {
            throw new IllegalStateException(
                    "Duplicate HandlerExecution id '" + handlerExecution.id() + "'"
            );
        }
    }

    @Nullable
    public HandlerExecution find(String id) {
        return byHandlerId.get(id);
    }

    public int size() {
        return byHandlerId.size();
    }
}
