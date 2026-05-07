package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueInitializedEvent;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.core.message.TopicPattern;

class DispatcherTest {

    private static final long SEGMENT_MAX_BYTES = 64L * 1024 * 1024;

    Host host = new Host(WebProtocol.HTTP, "localhost", 8080);
    Dispatcher dispatcher;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        dispatcher = new Dispatcher("test-dispatcher", host, List.of(new TopicPattern("**")));
    }

    @Test
    @DisplayName("Dispatcher 이름이 regex에 부합하지 않으면 예외를 던진다")
    void rejectInvalidDispatcherName() {
        assertThatThrownBy(() -> new Dispatcher("invalid name!", host, List.of(new TopicPattern("**"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("토픽 큐에서 메시지를 읽어 전송한다")
    void consumeFromTopicQueueTest() {
        final CountDownLatch latch = new CountDownLatch(1);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                latch.countDown();
                return true;
            }
        };

        final TopicQueue topicQueue = createTopicQueue(new Topic("test"));
        dispatcher.onTopicQueueInitialized(new TopicQueueInitializedEvent(topicQueue));
        topicQueue.offer(new Message(new Topic("test"), Map.of("key", "value")));
        dispatcher.onMessageArrived(new MessageArrivedEvent(topicQueue));

        assertThatCode(latch::await).doesNotThrowAnyException();
        dispatcher.destroy();
    }

    @Test
    @DisplayName("여러 토픽을 동시에 구독한다")
    void consumeMultipleTopicsTest() {
        final CountDownLatch latch = new CountDownLatch(2);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                latch.countDown();
                return true;
            }
        };

        final TopicQueue orderQueue = createTopicQueue(new Topic("order.new"));
        final TopicQueue paymentQueue = createTopicQueue(new Topic("payment.kakao"));
        dispatcher.onTopicQueueInitialized(new TopicQueueInitializedEvent(orderQueue));
        dispatcher.onTopicQueueInitialized(new TopicQueueInitializedEvent(paymentQueue));

        orderQueue.offer(new Message(new Topic("order.new"), Map.of("id", 1)));
        paymentQueue.offer(new Message(new Topic("payment.kakao"), Map.of("id", 2)));
        dispatcher.onMessageArrived(new MessageArrivedEvent(orderQueue));
        dispatcher.onMessageArrived(new MessageArrivedEvent(paymentQueue));

        assertThatCode(latch::await).doesNotThrowAnyException();
        dispatcher.destroy();
    }

    @Test
    @DisplayName("토픽별 메시지를 독립적으로 소비한다")
    void consumePerTopicIndependentlyTest() {
        final CountDownLatch latch = new CountDownLatch(3);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                latch.countDown();
                return true;
            }
        };

        final TopicQueue topicA = createTopicQueue(new Topic("topicA"));
        final TopicQueue topicB = createTopicQueue(new Topic("topicB"));
        dispatcher.onTopicQueueInitialized(new TopicQueueInitializedEvent(topicA));
        dispatcher.onTopicQueueInitialized(new TopicQueueInitializedEvent(topicB));

        topicA.offer(new Message(new Topic("topicA"), Map.of("seq", 1)));
        topicA.offer(new Message(new Topic("topicA"), Map.of("seq", 2)));
        topicB.offer(new Message(new Topic("topicB"), Map.of("seq", 1)));
        dispatcher.onMessageArrived(new MessageArrivedEvent(topicA));
        dispatcher.onMessageArrived(new MessageArrivedEvent(topicB));

        assertThatCode(latch::await).doesNotThrowAnyException();
        dispatcher.destroy();
    }

    @Test
    @DisplayName("패턴 미매칭 토픽은 구독하지 않는다")
    void doesNotSubscribeUnmatchedTopicTest() {
        dispatcher = new Dispatcher("test-dispatcher", host, List.of(new TopicPattern("order.*")));
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                return true;
            }
        };

        final TopicQueue paymentQueue = createTopicQueue(new Topic("payment.kakao"));
        dispatcher.onTopicQueueInitialized(new TopicQueueInitializedEvent(paymentQueue));
        paymentQueue.offer(new Message(new Topic("payment.kakao"), Map.of("id", 1)));
        dispatcher.onMessageArrived(new MessageArrivedEvent(paymentQueue));

        assertThat(dispatcher.subscriptions).doesNotContainKey(paymentQueue);
    }

    private TopicQueue createTopicQueue(Topic topic) {
        final Path topicDir = tempDir.resolve(topic.name());
        try {
            Files.createDirectories(topicDir);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        final SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, SEGMENT_MAX_BYTES);
        final CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(topicDir);

        return new TopicQueue(topic, segmentFileChain, checkpointDirectory);
    }
}
