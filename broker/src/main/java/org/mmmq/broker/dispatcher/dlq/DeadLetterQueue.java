package org.mmmq.broker.dispatcher.dlq;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.stereotype.Component;

@Component
public class DeadLetterQueue {

    final Queue<DeadLetter> queue = new LinkedBlockingQueue<>();

    public void add(DeadLetter deadLetter) {
        queue.add(deadLetter);
    }
}
