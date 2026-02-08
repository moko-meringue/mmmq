package org.mmmq.broker.dlq;

import org.mmmq.broker.dlq.handler.DeadLetterHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueue.class);

    final String name;
    final BlockingQueue<DeadLetter> queue;
    final DeadLetterHandler handler;

    DeadLetterQueue(String name, BlockingQueue<DeadLetter> queue, DeadLetterHandler handler) {
        this.name = name;
        this.queue = queue;
        this.handler = handler;
    }

    protected DeadLetterQueue(String name, DeadLetterHandler handler) {
        this.name = name;
        this.queue = new LinkedBlockingQueue<>();
        this.handler = handler;
    }

    public void add(DeadLetter deadLetter) {
        try {
            queue.add(deadLetter);
        } catch (Exception e) {
            log.warn("Failed to add dead letter", e);
        }
    }

    protected final List<DeadLetter> drainAll() {
        int currentSize = queue.size();
        List<DeadLetter> drained = new ArrayList<>(currentSize);
        queue.drainTo(drained);
        return drained;
    }
}
