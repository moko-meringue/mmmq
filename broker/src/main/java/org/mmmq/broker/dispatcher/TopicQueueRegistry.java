package org.mmmq.broker.dispatcher;

import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class TopicQueueRegistry {

    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition newMessage = lock.newCondition();

    public void add(Topic topic, Message message) {
        queues.computeIfAbsent(topic, TopicQueue::new).add(message);
        lock.lock();
        try {
            newMessage.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public Collection<TopicQueue> getAll() {
        return queues.values();
    }

    public void awaitNewMessage() throws InterruptedException {
        lock.lock();
        try {
            newMessage.await(100, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }
}
