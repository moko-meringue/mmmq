package org.mmmq.broker.topicqueue;

import jakarta.annotation.Nullable;
import org.mmmq.core.message.Message;

class Segment {

    private static final int CAPACITY = 1000;

    private final Message[] data;
    private int size = 0;

    Segment() {
        this.data = new Message[CAPACITY];
    }

    void put(Message message) {
        this.data[size++] = message;
    }

    boolean isFull() {
        return size >= CAPACITY;
    }

    int getSize() {
        return size;
    }

    @Nullable
    Message get(int index) {
        return data[index];
    }
}
