package org.mmmq.consumer.handler.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mmmq.core.message.Topic;

public class HandlerExecutions {

    final Map<Topic, List<HandlerExecution>> executions = new HashMap<>();

    public void add(HandlerExecution execution) {
        executions.computeIfAbsent(execution.getTopic(), topic -> new ArrayList<>()).add(execution);
    }

    public List<HandlerExecution> getExecutions(Topic topic) {
        return executions.getOrDefault(topic, new ArrayList<>());
    }

    public int size() {
        return executions.size();
    }
}
