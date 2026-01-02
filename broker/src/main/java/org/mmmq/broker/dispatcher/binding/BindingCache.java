package org.mmmq.broker.dispatcher.binding;

import org.mmmq.core.message.Topic;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BindingCache {

    final Set<Topic> cache = ConcurrentHashMap.newKeySet();

    public void put(Topic topic) {
        cache.add(topic);
    }

    public boolean matches(Topic topic) {
        return cache.contains(topic);
    }
}
