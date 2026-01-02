package org.mmmq.broker.dispatcher;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.mmmq.core.message.Topic;

public class BindingCache {

    final Set<Topic> cache = ConcurrentHashMap.newKeySet();

    public void put(Topic topic) {
        cache.add(topic);
    }

    public boolean matches(Topic topic) {
        return cache.contains(topic);
    }
}
