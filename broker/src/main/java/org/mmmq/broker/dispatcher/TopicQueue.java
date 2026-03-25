package org.mmmq.broker.dispatcher;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

/**
 * TopicQueue는 '절대 오프셋' 기반으로 메시지를 제공하는 API 게이트웨이 역할을 합니다.
 * <p>
 * 이 클래스는 소비자의 대기 및 신호(await/signal) 동기화만을 책임지며, 데이터 및 커서에 대한 모든 관리와 구체적인 로직은 SegmentChain에 위임합니다.
 */
public class TopicQueue {

    private final Topic topic;

    // 소비자의 대기/신호를 위한 Lock 및 Condition 입니다.
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition newMessage = lock.newCondition();

    // 모든 핵심 로직(세그먼트, 커서, 메모리 관리)을 책임지는 객체입니다.
    private final SegmentChain segmentChain;

    /**
     * 지정된 토픽에 대한 TopicQueue를 생성합니다.
     */
    public TopicQueue(Topic topic) {
        this.topic = topic;
        this.segmentChain = new SegmentChain();
    }

    /**
     * 큐에 메시지를 추가하고 소비자들에게 신호를 보냅니다.
     */
    public void add(Message message) {
        // SegmentChain의 add는 자체적으로 lock을 가지므로, TopicQueue의 lock 밖에서 호출합니다.
        segmentChain.add(message);

        // 새 메시지가 추가되었음을 대기중인 스레드에 알리기 위해 lock을 사용합니다.
        this.lock.lock();
        try {
            newMessage.signalAll();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 이 큐에 대한 새로운 구독 '커서' 객체를 생성하고 등록합니다.
     */
    public Cursor subscribe() {
        return segmentChain.createAndRegisterCursor();
    }

    public Topic getTopic() {
        return this.topic;
    }

    /**
     * (핵심 API) 특정 구독 커서가 요청하는 offset의 메시지를 기다리고, 준비되면 가져와 반환합니다.
     */
    public Message take(Cursor cursor) throws InterruptedException {
        // SegmentChain의 hasNext는 자체 lock을 사용하므로, TopicQueue의 lock 밖에서 호출할 수 있습니다.
        // 하지만 await과 함께 사용될 때는 반드시 TopicQueue의 lock 안에서 호출되어야 원자성이 보장됩니다.
        this.lock.lock();
        try {
            // 읽어야 할 오프셋의 메시지가 아직 큐에 도착하지 않았다면, 신호가 올 때까지 대기합니다.
            while (!segmentChain.hasNext(cursor)) {
                this.newMessage.await();
            }
        } finally {
            this.lock.unlock();
        }

        // 대기가 끝난 후, SegmentChain에 메시지 조회, 커서 업데이트, trim까지 모든 것을 위임합니다.
        // getMessageAt 메서드는 이제 자체적으로 동기화를 처리합니다.
        return this.segmentChain.getMessageAt(cursor);
    }
}
