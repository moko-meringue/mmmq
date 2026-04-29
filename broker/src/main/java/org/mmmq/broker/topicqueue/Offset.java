package org.mmmq.broker.topicqueue;

public final class Offset implements Comparable<Offset> { // 메시지 읽기 위치를 나타내는 단순 long 래퍼. 디스크 저장 구조로 전환하며 세그먼트 산술(getUnitIndex 등)을 제거하고 절대 offset만 보유

    private long value; // 다음에 읽을 메시지의 절대 offset. peek 시 사용하고, commit 시에만 증가

    public Offset() { // 신규 dispatcher를 위한 기본 생성자: offset 0부터 시작
        this(0L);
    }

    public Offset(long value) { // OffsetCheckpoint에서 읽은 마지막 커밋 값으로 복원할 때 사용
        this.value = value;
    }

    public long value() { // 현재 offset 값 반환. SegmentChain.readAt() 호출 시 인자로 전달됨
        return value;
    }

    public void increment() { // peek한 메시지가 정상 처리된 후 commit()에서만 호출. peek 시에는 절대 호출하지 않음 (at-least-once 보장)
        value++;
    }

    @Override
    public int compareTo(Offset other) { // Long.compare 위임: 두 offset의 순서를 비교
        return Long.compare(value, other.value);
    }
}
