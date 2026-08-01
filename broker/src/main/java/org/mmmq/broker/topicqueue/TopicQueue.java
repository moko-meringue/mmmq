package org.mmmq.broker.topicqueue;

import java.io.Closeable;
import java.util.concurrent.locks.ReentrantLock;
import org.mmmq.broker.topicqueue.storage.CorruptionException;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.broker.topicqueue.storage.StorageException;
import org.mmmq.core.annotation.Nullable;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 토픽 하나의 append-only 메시지 로그.
 *
 * <p>생산자가 보낸 메시지를 세그먼트 파일에 이어 쓰고({@link #offer}), 절대 offset으로 읽는다
 * ({@link #peek}). 그 이상은 모른다 — 누가 구독 중인지, 어디까지 읽었는지는 여기 상태가 아니다.
 * 그 상태는 {@code subscription.Subscription}이 (TopicQueue, Dispatcher) 짝 단위로 갖는다.
 * 읽기 위치를 여기서 없앤 이유는 그것이 로그 자체의 성질이 아니라 구독자마다 다른, 구독의 성질이기
 * 때문이다.
 */
public class TopicQueue implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TopicQueue.class);

    private final Topic topic;
    private final SegmentFileChain segmentFileChain;
    private final ReentrantLock writeLock = new ReentrantLock();

    public TopicQueue(Topic topic, SegmentFileChain segmentFileChain) {
        this.topic = topic;
        this.segmentFileChain = segmentFileChain;
    }

    public boolean offer(Message message) {
        writeLock.lock();
        try {
            segmentFileChain.append(message);
            return true;
        } catch (StorageException exception) {
            log.error("Failed to persist message for topic {}", topic, exception);
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    @Nullable
    public Message peek(Offset offset) throws CorruptionException {
        return segmentFileChain.readAt(offset.value());
    }

    public long tailOffset() {
        return segmentFileChain.tailOffset();
    }

    public Topic getTopic() {
        return topic;
    }

    @Override
    public void close() {
        segmentFileChain.close();
    }
}
