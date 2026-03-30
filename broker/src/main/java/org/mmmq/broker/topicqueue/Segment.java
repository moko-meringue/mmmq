package org.mmmq.broker.topicqueue;

import org.mmmq.core.message.Message;

class Segment {

    private final int capacity;
    private final Message[] data;
    private int size = 0;

    Segment(int capacity) {
        this.capacity = capacity;
        this.data = new Message[capacity];
    }

    void put(Message message) {
        this.data[size++] = message;
    }

    boolean isFull() {
        return size >= capacity;
    }

    Message get(int index) {
        return data[index];
    }

    boolean existsAt(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("Index cannot be negative");
        }
        return index < size;
    }
}
