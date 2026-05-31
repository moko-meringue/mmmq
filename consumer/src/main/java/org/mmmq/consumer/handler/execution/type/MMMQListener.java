package org.mmmq.consumer.handler.execution.type;

public interface MMMQListener<T> {

    String id();

    void handle(T content);
}
