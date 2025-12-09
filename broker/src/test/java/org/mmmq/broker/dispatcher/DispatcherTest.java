package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.ConsumerAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class DispatcherTest {

    Dispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new Dispatcher.Builder("name", "http", "localhost", 8080).build();
    }

    @Test
    @DisplayName("push 테스트")
    void pushTest() {
        Message message = new Message(new Topic("test"), Map.of("key", "value"));
        dispatcher.push(message);

        assertThat(dispatcher.messageQueue).contains(Map.entry(message, 0));
    }

    @Test
    @DisplayName("push 동시성 보장 테스트")
    void pushConcurrencyTest() throws InterruptedException {
        int threadCount = 100;
        int messagesPerThread = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < messagesPerThread; j++) {
                        Message message = new Message(new Topic("topic"), Map.of("id", threadId, "msg", j));
                        dispatcher.push(message);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await();
        executor.shutdown();

        int expectedCount = threadCount * messagesPerThread;
        int actualCount = dispatcher.messageQueue.size();

        assertThat(actualCount).isEqualTo(expectedCount);
    }

    @Test
    @DisplayName("ACK가 오면 메시지를 재전송하지 않는다.")
    void ackTest() throws Exception {
        dispatcher.startWorker();
        Sender sender = mock(Sender.class);
        Message message = new Message(new Topic("test"), Map.of("key", "value"));
        when(sender.send(message)).thenReturn(new ConsumerAcknowledgement(Acknowledgement.ACK));
        Field filed = Dispatcher.class.getDeclaredField("sender");
        filed.setAccessible(true);
        filed.set(dispatcher, sender);

        dispatcher.push(message);

        Thread.sleep(500L);
        verify(sender, times(1)).send(message);
    }

    @Test
    @DisplayName("NAK가 오면 메시지를 3회 재전송한다.")
    void nakTest() throws Exception {
        dispatcher.startWorker();
        Sender sender = mock(Sender.class);
        Message message = new Message(new Topic("test"), Map.of("key", "value"));
        when(sender.send(message)).thenReturn(new ConsumerAcknowledgement(Acknowledgement.NACK));
        Field filed = Dispatcher.class.getDeclaredField("sender");
        filed.setAccessible(true);
        filed.set(dispatcher, sender);

        dispatcher.push(message);

        Thread.sleep(1000L);
        verify(sender, times(1 + Dispatcher.MAX_RETRY_COUNT)).send(message);
    }

    @Test
    @DisplayName("isSubscribing 테스트")
    void isSubscribingTest() {
        dispatcher.topics.addAll(
                Set.of(
                        new Topic("topic1"),
                        new Topic("topic2")
                )
        );

        assertThat(dispatcher.isSubscribing(new Topic("topic1"))).isTrue();
        assertThat(dispatcher.isSubscribing(new Topic("topic2"))).isTrue();
        assertThat(dispatcher.isSubscribing(new Topic("topic3"))).isFalse();
    }
}
