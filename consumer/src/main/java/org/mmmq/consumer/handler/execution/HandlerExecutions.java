package org.mmmq.consumer.handler.execution;

import org.mmmq.core.message.Message;
import org.mmmq.core.message.Pattern;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HandlerExecutions {

    final List<HandlerExecution> executions = new ArrayList<>();
    final Map<Pattern, List<HandlerExecution>> patternCache = new ConcurrentHashMap<>();

    public void add(HandlerExecution execution) {
        executions.add(execution);
    }

    public List<HandlerExecution> getExecutions(Message message) {
        Pattern pattern = message.pattern();
        if (pattern == null) {
            return List.of();
        }
        if (patternCache.containsKey(pattern)) {
            return patternCache.get(pattern);
        }
        List<HandlerExecution> matchedExecutions = executions.stream()
                .filter(execution -> execution.supports(message))
                .toList();
        patternCache.put(pattern, matchedExecutions);
        return matchedExecutions;
    }

    public int size() {
        return executions.size();
    }
}
