package org.mmmq.consumer.handler.execution.type;

import org.mmmq.core.message.TopicPattern;

public interface MMMQListener<T> {

    TopicPattern listens();

    void handle(T content);
}
