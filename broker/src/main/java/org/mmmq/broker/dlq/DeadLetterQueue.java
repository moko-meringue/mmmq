package org.mmmq.broker.dlq;

import org.mmmq.broker.dlq.handler.DeadLetterHandler;

public abstract class DeadLetterQueue {

    public static final DeadLetterQueue NO_OP = new DeadLetterQueue(null, null) {
        @Override
        public void add(DeadLetter deadLetter) {
        }
    };

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
