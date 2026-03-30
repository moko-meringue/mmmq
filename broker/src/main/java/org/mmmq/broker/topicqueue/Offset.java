package org.mmmq.broker.topicqueue;

public class Offset implements Comparable<Offset> {

    private long value;

    private Offset(long value) {
        this.value = value;
    }

    Offset() {
        this(0);
    }

    int getRelativeIndex(long unitSize) {
        return (int) (value % unitSize);
    }

    int getUnitIndex(long unitSize) {
        return (int) (value / unitSize);
    }

    void increment() {
        this.value++;
    }

    @Override
    public int compareTo(Offset o) {
        return Long.compare(this.value, o.value);
    }
}
