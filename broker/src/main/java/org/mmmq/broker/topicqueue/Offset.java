package org.mmmq.broker.topicqueue;

public record Offset(
        long value
) implements Comparable<Offset> {

    public Offset next() {
        return new Offset(value + 1);
    }

    @Override
    public int compareTo(Offset other) {
        return Long.compare(value, other.value);
    }
}
