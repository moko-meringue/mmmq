package org.mmmq.subscriber;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MessageHandlerContainer {

    private final Map<String, List<MessageHandler>> handlers = new HashMap<>();

    void add(MessageHandler handler) {
        handlers.computeIfAbsent(handler.topic, topic -> new ArrayList<>()).add(handler);
    }

    List<MessageHandler> getHandlers(String topic) {
        return handlers.getOrDefault(topic, new ArrayList<>());
    }
}
