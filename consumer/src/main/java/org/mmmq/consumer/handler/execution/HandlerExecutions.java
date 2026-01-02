package org.mmmq.consumer.handler.execution;

import org.mmmq.core.message.Topic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HandlerExecutions {

    final List<HandlerExecution> executions = new ArrayList<>();
    final Map<Topic, List<HandlerExecution>> patternCache = new ConcurrentHashMap<>();

    public void add(HandlerExecution execution) {
        executions.add(execution);
    }

    public List<HandlerExecution> getExecutions(Topic topic) {
        if (patternCache.containsKey(topic)) {
            return patternCache.get(topic);
        }
        List<HandlerExecution> matchedExecutions = executions.stream()
                .filter(execution -> execution.supports(topic))
                .toList();
        patternCache.put(topic, matchedExecutions);
        return matchedExecutions;
    }

    public int size() {
        return executions.size();
    }
}
