package org.mmmq.consumer.handler.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HandlerExecutions {

    private final Map<String, List<HandlerExecution>> executions = new HashMap<>();

    public void add(HandlerExecution execution) {
        executions.computeIfAbsent(execution.getTopic(), topic -> new ArrayList<>()).add(execution);
    }

    public List<HandlerExecution> getExecutions(String topic) {
        return executions.getOrDefault(topic, new ArrayList<>());
    }
}
