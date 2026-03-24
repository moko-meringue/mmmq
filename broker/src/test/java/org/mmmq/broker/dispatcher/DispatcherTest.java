package org.mmmq.broker.dispatcher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

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
        dispatcher.start();

        Message message = new Message(new Topic("test"), Map.of("key", "value"));
        registry.add(new Topic("test"), message);

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
        dispatcher.start();

        registry.add(new Topic("order.new"), new Message(new Topic("order.new"), Map.of("id", 1)));
        registry.add(new Topic("payment.kakao"), new Message(new Topic("payment.kakao"), Map.of("id", 2)));

        assertThatCode(latch::await).doesNotThrowAnyException();
        dispatcher.stop();
    }

    @Test
    @DisplayName("토픽별 오프셋을 독립적으로 관리한다")
    void offsetPerTopicTest() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                latch.countDown();
                return true;
            }
        };

        Topic topicA = new Topic("topic.a");
        Topic topicB = new Topic("topic.b");
        registry.add(topicA, new Message(topicA, Map.of("seq", 1)));
        registry.add(topicA, new Message(topicA, Map.of("seq", 2)));
        registry.add(topicB, new Message(topicB, Map.of("seq", 1)));

        dispatcher.start();

        assertThatCode(latch::await).doesNotThrowAnyException();
        assertThat(dispatcher.offsets.get(topicA).get()).isEqualTo(2);
        assertThat(dispatcher.offsets.get(topicB).get()).isEqualTo(1);
        dispatcher.stop();
    }
}
