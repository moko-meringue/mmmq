package org.mmmq.broker.dispatcher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.SubscriberAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class MessageDispatcherTest {

    MessageDispatcher messageDispatcher;

    @BeforeEach
    void setUp() {
        messageDispatcher = new MessageDispatcher.Builder("name", "http", "localhost", 8080).build();
    }

    @Test
    @DisplayName("push 테스트")
    void pushTest() {
        Message message = new Message("test", Map.of("key", "value"));
        messageDispatcher.push(message);

        assertThat(messageDispatcher.messageQueue).contains(Map.entry(message, 0));
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
                        Message message = new Message("topic", Map.of("id", threadId, "msg", j));
                        messageDispatcher.push(message);
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
        int actualCount = messageDispatcher.messageQueue.size();

        assertThat(actualCount).isEqualTo(expectedCount);
    }

    @Test
    @DisplayName("ACK가 오면 메시지를 재전송하지 않는다.")
    void ackTest() throws Exception {
        messageDispatcher.startWorker();
        MessageSender messageSender = mock(MessageSender.class);
        Message message = new Message("test", Map.of("key", "value"));
        when(messageSender.send(message)).thenReturn(new SubscriberAcknowledgement(Acknowledgement.ACK));
        Field filed = MessageDispatcher.class.getDeclaredField("messageSender");
        filed.setAccessible(true);
        filed.set(messageDispatcher, messageSender);

        messageDispatcher.push(message);

        Thread.sleep(500L);
        verify(messageSender, times(1)).send(message);
    }

    @Test
    @DisplayName("NAK가 오면 메시지를 3회 재전송한다.")
    void nakTest() throws Exception {
        messageDispatcher.startWorker();
        MessageSender messageSender = mock(MessageSender.class);
        Message message = new Message("test", Map.of("key", "value"));
        when(messageSender.send(message)).thenReturn(new SubscriberAcknowledgement(Acknowledgement.NACK));
        Field filed = MessageDispatcher.class.getDeclaredField("messageSender");
        filed.setAccessible(true);
        filed.set(messageDispatcher, messageSender);

        messageDispatcher.push(message);

        Thread.sleep(1000L);
        verify(messageSender, times(1 + MessageDispatcher.MAX_RETRY_COUNT)).send(message);
    }

    @Test
    @DisplayName("isSubscribing 테스트")
    void isSubscribingTest() {
        messageDispatcher.topics.addAll(
                Set.of(
                        new Topic("topic1"),
                        new Topic("topic2")
                )
        );

        assertThat(messageDispatcher.isSubscribing("topic1")).isTrue();
        assertThat(messageDispatcher.isSubscribing("topic2")).isTrue();
        assertThat(messageDispatcher.isSubscribing("topic3")).isFalse();
    }
}
