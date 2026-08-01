package org.mmmq.broker.subscription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.persistence.PersistenceProperties;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueFactory;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.core.message.TopicPattern;

class SubscriptionContainerTest {

    Host host = new Host(WebProtocol.HTTP, "localhost", 8080);

    @TempDir
    Path tempDir;

    TopicQueueFactory topicQueueFactory;
    SubscriptionContainer container;

    @BeforeEach
    void setUp() {
        PersistenceProperties properties = new PersistenceProperties(tempDir.toString(), null);
        topicQueueFactory = new TopicQueueFactory(properties);
        container = new SubscriptionContainer(topicQueueFactory);
    }

    @Test
    @DisplayName("rematchAll로 미리 알려진 Dispatcher는 이후 register되는 새 토픽 큐와 매칭된다")
    void seededDispatcherMatchesLaterRegisteredQueue() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Dispatcher dispatcher = countingDispatcher("consumer", latch);
        container.rematchAll(List.of(dispatcher));

        TopicQueue queue = topicQueueFactory.create(new Topic("order.created"));
        container.register(queue);
        queue.offer(new Message(new Topic("order.created"), Map.of("id", 1)));
        container.trigger(queue);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("패턴이 매칭되지 않는 토픽은 트리거해도 전송되지 않는다")
    void unmatchedTopicNeverTriggersDelivery() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Dispatcher dispatcher = new Dispatcher(
                host, new ConsumerId("consumer"), new TopicPattern("order.*"),
                countingSender(latch)
        );
        container.rematchAll(List.of(dispatcher));

        TopicQueue queue = topicQueueFactory.create(new Topic("payment.done"));
        container.register(queue);
        queue.offer(new Message(new Topic("payment.done"), Map.of("id", 1)));
        container.trigger(queue);

        assertThat(latch.await(300, TimeUnit.MILLISECONDS)).isFalse();
    }

    @Test
    @DisplayName("rematchAll로 패턴을 넓히면 이미 쌓인 백로그는 건너뛰고 tail부터 구독한다")
    void wideningPatternSubscribesNewTopicAtTail() throws InterruptedException {
        TopicQueue orderQueue = topicQueueFactory.create(new Topic("order.created"));
        TopicQueue paymentQueue = topicQueueFactory.create(new Topic("payment.done"));
        Dispatcher narrow = new Dispatcher(host, new ConsumerId("consumer"), new TopicPattern("order.*"));
        container.rematchAll(List.of(narrow));
        container.register(orderQueue);
        container.register(paymentQueue);
        paymentQueue.offer(new Message(new Topic("payment.done"), Map.of("seq", 1)));

        List<Message> received = new CopyOnWriteArrayList<>();
        Dispatcher widened = countingDispatcher("consumer", received);
        container.rematchAll(List.of(widened));
        container.trigger(paymentQueue);
        awaitSize(received, 0);
        assertThat(received).isEmpty();
        assertThat(checkpointOf("payment.done", "consumer")).exists();

        paymentQueue.offer(new Message(new Topic("payment.done"), Map.of("seq", 2)));
        container.trigger(paymentQueue);
        awaitSize(received, 1);

        assertThat(received).hasSize(1);
    }

    @Test
    @DisplayName("rematchAll로 패턴을 좁히면 빠진 토픽의 구독과 체크포인트가 사라진다")
    void narrowingPatternDropsSubscriptionAndCheckpoint() {
        TopicQueue orderQueue = topicQueueFactory.create(new Topic("order.created"));
        TopicQueue paymentQueue = topicQueueFactory.create(new Topic("payment.done"));
        Dispatcher wide = new Dispatcher(host, new ConsumerId("consumer"), new TopicPattern("**"));
        container.rematchAll(List.of(wide));
        container.register(orderQueue);
        container.register(paymentQueue);
        assertThat(checkpointOf("payment.done", "consumer")).exists();

        Dispatcher narrow = new Dispatcher(host, new ConsumerId("consumer"), new TopicPattern("order.*"));
        container.rematchAll(List.of(narrow));

        assertThat(checkpointOf("payment.done", "consumer")).doesNotExist();
        assertThat(checkpointOf("order.created", "consumer")).exists();
    }

    @Test
    @DisplayName("제거된 Dispatcher는 트리거로도 다시는 메시지를 받지 못한다")
    void removedDispatcherNeverReceivesAgain() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Dispatcher dispatcher = countingDispatcher("consumer", latch);
        TopicQueue orderQueue = topicQueueFactory.create(new Topic("order.created"));
        TopicQueue shippedQueue = topicQueueFactory.create(new Topic("order.shipped"));
        container.rematchAll(List.of(dispatcher));
        container.register(orderQueue);
        // shippedQueue는 register만 되고 아직 한 번도 트리거된 적 없다 — 지연 워커 생성 버그가 있었다면
        // 이 큐가 바로 그 재현 지점이다.
        container.register(shippedQueue);

        container.rematchAll(List.of());

        shippedQueue.offer(new Message(new Topic("order.shipped"), Map.of("id", 1)));
        container.trigger(shippedQueue);
        orderQueue.offer(new Message(new Topic("order.created"), Map.of("id", 1)));
        container.trigger(orderQueue);

        assertThat(latch.await(300, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(checkpointOf("order.created", "consumer")).doesNotExist();
        assertThat(checkpointOf("order.shipped", "consumer")).doesNotExist();
    }

    @Test
    @DisplayName("같은 Dispatcher가 여러 토픽 큐를 독립적으로 구독한다")
    void singleDispatcherServesMultipleQueuesIndependently() throws InterruptedException {
        List<Message> received = new CopyOnWriteArrayList<>();
        Dispatcher dispatcher = countingDispatcher("consumer", received);
        container.rematchAll(List.of(dispatcher));
        TopicQueue orderQueue = topicQueueFactory.create(new Topic("order.created"));
        TopicQueue paymentQueue = topicQueueFactory.create(new Topic("payment.done"));
        container.register(orderQueue);
        container.register(paymentQueue);

        orderQueue.offer(new Message(new Topic("order.created"), Map.of("id", 1)));
        paymentQueue.offer(new Message(new Topic("payment.done"), Map.of("id", 2)));
        container.trigger(orderQueue);
        container.trigger(paymentQueue);
        awaitSize(received, 2);

        assertThat(received).hasSize(2);
    }

    @Test
    @DisplayName("register되지 않은 TopicQueue로 trigger를 호출해도 예외가 나지 않는다")
    void triggerOnUnregisteredTopicQueueDoesNotThrow() {
        TopicQueue neverRegistered = topicQueueFactory.create(new Topic("never.registered"));

        assertThatCode(() -> container.trigger(neverRegistered)).doesNotThrowAnyException();
    }

    private void awaitSize(List<?> list, int expectedSize) throws InterruptedException {
        if (expectedSize == 0) {
            Thread.sleep(300);
            return;
        }
        long deadline = System.currentTimeMillis() + 5000;
        while (list.size() < expectedSize && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
    }

    private Path checkpointOf(String topicName, String consumerId) {
        return tempDir.resolve("topics")
                .resolve(topicName)
                .resolve("checkpoints")
                .resolve(consumerId + ".checkpoint");
    }

    private Sender countingSender(CountDownLatch latch) {
        return new Sender(null) {
            @Override
            public boolean send(Message message, ConsumerId consumerId, int retryCount) {
                latch.countDown();
                return true;
            }
        };
    }

    private Dispatcher countingDispatcher(String consumerId, CountDownLatch latch) {
        return new Dispatcher(host, new ConsumerId(consumerId), new TopicPattern("**"), countingSender(latch));
    }

    private Dispatcher countingDispatcher(String consumerId, List<Message> received) {
        return new Dispatcher(
                host,
                new ConsumerId(consumerId),
                new TopicPattern("**"),
                new Sender(null) {
                    @Override
                    public boolean send(Message message, ConsumerId id, int retryCount) {
                        received.add(message);
                        return true;
                    }
                }
        );
    }
}
