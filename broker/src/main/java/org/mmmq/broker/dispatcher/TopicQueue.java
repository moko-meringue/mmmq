package org.mmmq.broker.dispatcher;

import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TopicQueue {

    private final Topic topic;
    private final List<Message> messages = new ArrayList<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public TopicQueue(Topic topic) {
        this.topic = topic;
    }

    public void add(Message message) {
        lock.writeLock().lock();
        try {
            messages.add(message);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Optional<Message> get(long offset) {
        lock.readLock().lock();
        try {
            if (offset < messages.size()) {
                return Optional.of(messages.get((int) offset));
            }
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    public Topic topic() {
        return topic;
    }
}
