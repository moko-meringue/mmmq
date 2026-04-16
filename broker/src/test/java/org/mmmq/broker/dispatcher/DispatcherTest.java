package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.fixture.NoOpWalWriter;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Pattern;
import org.mmmq.core.message.Topic;

class DispatcherTest {

    Host host = new Host(WebProtocol.HTTP, "localhost", 8080);
    Dispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new Dispatcher("test-dispatcher", host, List.of(new Pattern("**")));
    }

    @Test
    @DisplayName("토픽 큐에서 메시지를 읽어 전송한다")
    void consumeFromTopicQueueTest() {
        CountDownLatch latch = new CountDownLatch(1);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                latch.countDown();
                return true;
            }
        };

        TopicQueue topicQueue = new TopicQueue(new Topic("test"), new NoOpWalWriter());
        dispatcher.subscribe(topicQueue);
        topicQueue.offer(new Message(new Topic("test"), Map.of("key", "value")), true);
        dispatcher.onMessageArrived(new MessageArrivedEvent(topicQueue));

        assertThatCode(latch::await).doesNotThrowAnyException();
        dispatcher.stop();
    }

    @Test
    @DisplayName("여러 토픽을 동시에 구독한다")
    void consumeMultipleTopicsTest() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                latch.countDown();
                return true;
            }
        };

        TopicQueue orderQueue = new TopicQueue(new Topic("order.new"), new NoOpWalWriter());
        TopicQueue paymentQueue = new TopicQueue(new Topic("payment.kakao"), new NoOpWalWriter());
        dispatcher.subscribe(orderQueue);
        dispatcher.subscribe(paymentQueue);

        orderQueue.offer(new Message(new Topic("order.new"), Map.of("id", 1)), true);
        paymentQueue.offer(new Message(new Topic("payment.kakao"), Map.of("id", 2)), true);
        dispatcher.onMessageArrived(new MessageArrivedEvent(orderQueue));
        dispatcher.onMessageArrived(new MessageArrivedEvent(paymentQueue));

        assertThatCode(latch::await).doesNotThrowAnyException();
        dispatcher.stop();
    }

    @Test
    @DisplayName("토픽별 메시지를 독립적으로 소비한다")
    void consumePerTopicIndependentlyTest() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                latch.countDown();
                return true;
            }
        };

        TopicQueue topicA = new TopicQueue(new Topic("topicA"), new NoOpWalWriter());
        TopicQueue topicB = new TopicQueue(new Topic("topicB"), new NoOpWalWriter());
        dispatcher.subscribe(topicA);
        dispatcher.subscribe(topicB);

        topicA.offer(new Message(new Topic("topicA"), Map.of("seq", 1)), true);
        topicA.offer(new Message(new Topic("topicA"), Map.of("seq", 2)), true);
        topicB.offer(new Message(new Topic("topicB"), Map.of("seq", 1)), true);
        dispatcher.onMessageArrived(new MessageArrivedEvent(topicA));
        dispatcher.onMessageArrived(new MessageArrivedEvent(topicB));

        assertThatCode(latch::await).doesNotThrowAnyException();
        dispatcher.stop();
    }

    @Test
    @DisplayName("패턴 미매칭 토픽은 구독하지 않는다")
    void doesNotSubscribeUnmatchedTopicTest() throws InterruptedException {
        dispatcher = new Dispatcher("test-dispatcher", host, List.of(new Pattern("order.*")));
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                return true;
            }
        };

        TopicQueue paymentQueue = new TopicQueue(new Topic("payment.kakao"), new NoOpWalWriter());
        dispatcher.subscribe(paymentQueue);
        paymentQueue.offer(new Message(new Topic("payment.kakao"), Map.of("id", 1)), true);
        dispatcher.onMessageArrived(new MessageArrivedEvent(paymentQueue));

        assertThat(dispatcher.subscriptions).doesNotContainKey(paymentQueue);
    }
}
