package org.mmmq.broker.dispatcher;

import org.mmmq.core.message.Message;

/**
 * Segment는 메시지를 담는 고정 크기 배열 기반의 순수한 데이터 컨테이너입니다.
 * 이 클래스는 동시성을 전혀 보장하지 않으며, 용량(capacity)을 외부에서 주입받습니다.
 */
public class Segment {

    // 메시지를 실제로 저장하는 배열입니다.
    private final Message[] data;
    // 이 세그먼트가 저장할 수 있는 최대 메시지 수입니다.
    private final int capacity;

    // 이 세그먼트에 저장된 메시지의 현재 개수입니다.
    private int size = 0;

    /**
     * 지정된 용량으로 Segment를 생성합니다.
     * 동일 패키지 내에서만 생성할 수 있도록 package-private으로 선언합니다.
     * @param capacity 세그먼트의 용량
     */
    Segment(int capacity) {
        this.capacity = capacity;
        this.data = new Message[capacity];
    }

    /**
     * 메시지를 배열에 추가하고 size를 증가시킵니다.
     * 이 메서드는 스레드에 안전하지 않으므로, 반드시 외부에서 Lock을 건 상태로 호출되어야 합니다.
     * @param message 추가할 메시지
     */
    void put(Message message) {
        this.data[size++] = message;
    }

    /**
     * 세그먼트가 가득 찼는지 확인합니다.
     * @return 가득 찼으면 true
     */
    boolean isFull() {
        return size >= capacity;
    }

    Message getMessageAt(int index) {
        return data[index];
    }

    int getSize() {
        return size;
    }

    int getCapacity() {
        return capacity;
    }
}
