package org.mmmq.broker.topicqueue;

public class Offset implements Comparable<Offset> {

    private long value;

    Offset(long value) {
        this.value = value;
    }

    long getValue() {
        return value;
    }

    void increment() {
        this.value++;
    }

    @Override
    public int compareTo(Offset o) {
        return Long.compare(this.value, o.value);
    }
}
