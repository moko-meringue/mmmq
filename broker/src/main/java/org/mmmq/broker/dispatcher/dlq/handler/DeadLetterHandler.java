package org.mmmq.broker.dispatcher.dlq.handler;

import org.mmmq.broker.dispatcher.dlq.DeadLetter;

import java.util.Collection;

public interface DeadLetterHandler {

    void handle(DeadLetter deadLetter);

    void handle(Collection<DeadLetter> deadLetters);
}
