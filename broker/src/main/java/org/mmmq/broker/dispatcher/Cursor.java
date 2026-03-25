package org.mmmq.broker.dispatcher;

/**
 * Subscription은 각 소비자가 토픽의 어느 지점까지 메시지를 읽었는지를 나타내는 '절대 오프셋' 커서 객체입니다. 이 클래스는 어떠한 로직도 수행하지 않으며, 순수한 데이터 컨테이너(Data
 * Container) 역할을 합니다.
 */
public class Cursor {

    /**
     * 구독자가 읽어야 할 다음 메시지의 절대 오프셋입니다. 큐에 첫 번째로 들어온 메시지의 오프셋은 0입니다.
     */
    private long offset;

    /**
     * 오프셋이 0인 새로운 Cursor를 생성합니다.
     */
    Cursor() {
        this.offset = 0;
    }

    /**
     * 현재 오프셋을 반환합니다.
     *
     * @return 현재 오프셋
     */
    long getOffset() {
        return offset;
    }

    /**
     * 오프셋을 1 증가시킵니다.
     */
    void incrementOffset() {
        this.offset++;
    }
}
