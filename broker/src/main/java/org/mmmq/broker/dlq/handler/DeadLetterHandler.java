package org.mmmq.broker.dlq.handler;

import org.mmmq.broker.dlq.DeadLetter;

import java.util.Collection;

public interface DeadLetterHandler {

    void handle(Collection<DeadLetter> deadLetters);
}
