package org.mmmq.broker.dispatcher.dlq;

import org.mmmq.broker.dispatcher.dlq.handler.DeadLetterHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueue.class);

    protected final String name;
    protected final DeadLetterHandler handler;

    public DeadLetterQueue(String name, DeadLetterHandler handler) {
        this.name = name;
        this.handler = handler;
    }

    public abstract void add(DeadLetter deadLetter);

    public void start() {
    }

    public void stop() {
    }
}
