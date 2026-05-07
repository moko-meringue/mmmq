package org.mmmq.broker.topicqueue;

public record Offset(
        long value
) {

    public Offset next() {
        return new Offset(value + 1);
    }
}
