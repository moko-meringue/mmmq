package org.mmmq.consumer.handler.execution.type;

public interface MMMQListener<T> {

    String listens();

    void handle(T content);
}
