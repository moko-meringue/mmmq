package org.mmmq.broker.dispatcher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.core.message.TopicPattern;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DispatcherTest {

    private static final long SEGMENT_MAX_BYTES = 64L * 1024 * 1024;

    Host host = new Host(WebProtocol.HTTP, "localhost", 8080);
    Dispatcher dispatcher;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        dispatcher = new Dispatcher(host, "test-dispatcher", new TopicPattern("**"));
    }

    @Test
    @DisplayName("handler id가 regex에 부합하지 않으면 예외를 던진다")
    void rejectInvalidHandlerId() {
        assertThatThrownBy(() -> new Dispatcher(host, "invalid handler!", new TopicPattern("**")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("handler id getter가 생성자 인자를 그대로 반환한다")
    void handlerIdGetter() {
        Dispatcher dispatcher = new Dispatcher(host, "order-dispatcher", new TopicPattern("order.*"));

        assertThat(dispatcher.handlerId()).isEqualTo("order-dispatcher");
    }

    @Test
    @DisplayName("패턴 매칭이 true이면 matches()가 true를 반환한다")
    void matchesReturnsTrueWhenPatternMatches() {
        Dispatcher dispatcher = new Dispatcher(host, "order-dispatcher", new TopicPattern("order.*"));

        assertThat(dispatcher.matches(new Topic("order.new"))).isTrue();
        assertThat(dispatcher.matches(new Topic("payment.kakao"))).isFalse();
    }

    @Test
    @DisplayName("subscribe 후 drain하면 TopicQueue의 메시지를 읽어 전송한다")
    void drainConsumesFromSubscribedTopicQueue() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, String handlerId, int retryCount) {
                latch.countDown();
                return true;
            }
        };

        TopicQueue topicQueue = createTopicQueue(new Topic("test"));
        dispatcher.subscribe(topicQueue);
        topicQueue.offer(new Message(new Topic("test"), Map.of("key", "value")));
        dispatcher.drain(topicQueue);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        dispatcher.destroy();
    }

    @Test
    @DisplayName("여러 토픽 큐를 동시에 구독한다")
    void subscribesMultipleTopicQueues() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, String handlerId, int retryCount) {
                latch.countDown();
                return true;
            }
        };

        TopicQueue orderQueue = createTopicQueue(new Topic("order.new"));
        TopicQueue paymentQueue = createTopicQueue(new Topic("payment.kakao"));
        dispatcher.subscribe(orderQueue);
        dispatcher.subscribe(paymentQueue);

        orderQueue.offer(new Message(new Topic("order.new"), Map.of("id", 1)));
        paymentQueue.offer(new Message(new Topic("payment.kakao"), Map.of("id", 2)));
        dispatcher.drain(orderQueue);
        dispatcher.drain(paymentQueue);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        dispatcher.destroy();
    }

    @Test
    @DisplayName("토픽별 메시지를 독립적으로 소비한다")
    void consumesPerTopicIndependently() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, String handlerId, int retryCount) {
                latch.countDown();
                return true;
            }
        };

        TopicQueue topicA = createTopicQueue(new Topic("topicA"));
        TopicQueue topicB = createTopicQueue(new Topic("topicB"));
        dispatcher.subscribe(topicA);
        dispatcher.subscribe(topicB);

        topicA.offer(new Message(new Topic("topicA"), Map.of("seq", 1)));
        topicA.offer(new Message(new Topic("topicA"), Map.of("seq", 2)));
        topicB.offer(new Message(new Topic("topicB"), Map.of("seq", 1)));
        dispatcher.drain(topicA);
        dispatcher.drain(topicB);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        dispatcher.destroy();
    }

    @Test
    @DisplayName("subscribe되지 않은 TopicQueue에 대한 drain은 무시된다")
    void drainIgnoresUnsubscribedQueue() {
        Dispatcher dispatcher = new Dispatcher(host, "test-dispatcher", new TopicPattern("order.*"));
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, String handlerId, int retryCount) {
                return true;
            }
        };

        TopicQueue paymentQueue = createTopicQueue(new Topic("payment.kakao"));
        paymentQueue.offer(new Message(new Topic("payment.kakao"), Map.of("id", 1)));
        assertThatCode(() -> dispatcher.drain(paymentQueue)).doesNotThrowAnyException();

        assertThat(dispatcher.subscriptions).doesNotContainKey(paymentQueue);
    }

    @Test
    @DisplayName("send에는 dispatcher의 handlerId가 전달된다")
    void deliversHandlerIdToSender() throws InterruptedException {
        Dispatcher dispatcher = new Dispatcher(host, "my-handler", new TopicPattern("**"));
        CountDownLatch latch = new CountDownLatch(1);
        String[] receivedHandlerId = new String[1];
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, String handlerId, int retryCount) {
                receivedHandlerId[0] = handlerId;
                latch.countDown();
                return true;
            }
        };

        TopicQueue queue = createTopicQueue(new Topic("test"));
        dispatcher.subscribe(queue);
        queue.offer(new Message(new Topic("test"), Map.of("k", "v")));
        dispatcher.drain(queue);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(receivedHandlerId[0]).isEqualTo("my-handler");
        dispatcher.destroy();
    }

    private TopicQueue createTopicQueue(Topic topic) {
        Path topicDir = tempDir.resolve(topic.name());
        try {
            Files.createDirectories(topicDir);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, SEGMENT_MAX_BYTES);
        CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(topicDir);

        return new TopicQueue(topic, segmentFileChain, checkpointDirectory);
    }
}
