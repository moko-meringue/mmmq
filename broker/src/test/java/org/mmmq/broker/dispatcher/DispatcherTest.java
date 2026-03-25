package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;

class DispatcherTest {

    Host host = new Host("http", "localhost", 8080);
    TopicQueueRegistry registry;
    Dispatcher dispatcher;

    @BeforeEach
    void setUp() {
        registry = new TopicQueueRegistry();
        dispatcher = new Dispatcher("test-dispatcher", host);
        dispatcher.initialize(registry, mock(ObjectProvider.class));
    }

    @Test
    @DisplayName("토픽 큐에서 메시지를 읽어 전송한다")
    void consumeFromTopicQueueTest() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                latch.countDown();
                return true;
            }
        };

        registry.add(new Topic("test"), new Message(new Topic("test"), Map.of("key", "value")));

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

        registry.add(new Topic("order.new"), new Message(new Topic("order.new"), Map.of("id", 1)));
        registry.add(new Topic("payment.kakao"), new Message(new Topic("payment.kakao"), Map.of("id", 2)));

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

        Topic topicA = new Topic("topicA");
        Topic topicB = new Topic("topicB");
        registry.add(topicA, new Message(topicA, Map.of("seq", 1)));
        registry.add(topicA, new Message(topicA, Map.of("seq", 2)));
        registry.add(topicB, new Message(topicB, Map.of("seq", 1)));

        assertThatCode(latch::await).doesNotThrowAnyException();
        dispatcher.stop();
    }

//    @Test
//    @DisplayName("Segment 경계를 넘어 메시지를 모두 소비한다")
//    void segmentBoundaryTest() throws InterruptedException {
//        int totalMessages = TopicQueue.SEGMENT_CAPACITY + 5;
//        CountDownLatch latch = new CountDownLatch(totalMessages);
//        dispatcher.sender = new Sender(null) {
//            @Override
//            public boolean send(Message message, int retryCount) {
//                latch.countDown();
//                return true;
//            }
//        };
//
//        Topic topic = new Topic("boundary-topic");
//        for (int i = 0; i < totalMessages; i++) {
//            registry.add(topic, new Message(topic, Map.of("seq", i)));
//        }
//
//        assertThatCode(latch::await).doesNotThrowAnyException();
//        dispatcher.stop();
//    }
}
