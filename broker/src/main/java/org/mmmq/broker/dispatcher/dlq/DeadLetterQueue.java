package org.mmmq.broker.dispatcher.dlq;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueue.class);

    protected final String name;
    protected final BlockingQueue<DeadLetter> deadLetterQueue = new LinkedBlockingQueue<>();

    public DeadLetterQueue(String name) {
        this.name = name;
    }

    public void add(DeadLetter deadLetter) {
        deadLetterQueue.add(deadLetter);
    }

    void start() {
    }

    void stop() {
    }
}
