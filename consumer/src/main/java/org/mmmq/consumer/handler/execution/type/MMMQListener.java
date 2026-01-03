package org.mmmq.consumer.handler.execution.type;

import org.mmmq.core.message.Pattern;

public interface MMMQListener<T> {

    Pattern listens();

    void handle(T content);
}
