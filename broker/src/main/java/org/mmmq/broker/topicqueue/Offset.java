package org.mmmq.broker.topicqueue;

public record Offset(
        long value // 다음에 읽을 메시지의 절대 offset
) implements Comparable<Offset> { // 메시지 읽기 위치를 나타내는 immutable value object

    public Offset() { // 신규 dispatcher를 위한 기본 생성자: offset 0부터 시작
        this(0L);
    }

    public Offset next() { // 다음 offset을 가리키는 새 Offset 반환. peek 후 commit 단계에서 사용 (at-least-once 보장)
        return new Offset(value + 1);
    }

    @Override
    public int compareTo(Offset other) { // Long.compare 위임: 두 offset의 순서를 비교
        return Long.compare(value, other.value);
    }
}
