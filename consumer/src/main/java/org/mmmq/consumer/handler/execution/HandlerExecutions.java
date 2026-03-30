package org.mmmq.consumer.handler.execution;

import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HandlerExecutions {

    final List<HandlerExecution> executions = new ArrayList<>();
    final Map<Topic, List<HandlerExecution>> topicCache = new ConcurrentHashMap<>();

    public void add(HandlerExecution execution) {
        executions.add(execution);
    }

    public List<HandlerExecution> getExecutions(Message message) {
        Topic topic = message.topic();
        if (topicCache.containsKey(topic)) {
            return topicCache.get(topic);
        }
        List<HandlerExecution> matchedExecutions = executions.stream()
                .filter(execution -> execution.supports(message))
                .toList();
        topicCache.put(topic, matchedExecutions);
        return matchedExecutions;
    }

    public int size() {
        return executions.size();
    }
}
