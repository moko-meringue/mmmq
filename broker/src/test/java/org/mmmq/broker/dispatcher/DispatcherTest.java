package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class DispatcherTest {

    static final DispatcherDefinition DEFINITION = DispatcherDefinition
        .builder("name", "http", "localhost", 8080)
        .build();

    Dispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = DEFINITION.toDispatcher();
    }

    @Test
    @DisplayName("push 테스트")
    void dispatchTest() {
        Message message = new Message(new Topic("test"), Map.of("key", "value"));
        dispatcher.dispatch(message);

        assertThat(dispatcher.messageQueue).contains(message);
    }

    @Test
    @DisplayName("push 동시성 보장 테스트")
    void dispatchConcurrencyTest() throws InterruptedException {
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
                        dispatcher.dispatch(message);
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
    @DisplayName("메시지 전송 테스트")
    void executeTest() {
        CountDownLatch latch = new CountDownLatch(1);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message) {
                latch.countDown();
                return true;
            }
        };
        dispatcher.start();
        Message message = new Message(new Topic("test"), Map.of("key", "value"));
        dispatcher.dispatch(message);
        assertThatCode(latch::await).doesNotThrowAnyException();
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

    // static class FakeSender extends Sender {
    //
    //     int sendCount = 0;
    //     final boolean willAck = false;
    //
    //     public FakeSender(int nakUntil) {
    //         this.nakUntil = nakUntil;
    //     }
    //
    //     @Override
    //     public boolean send(Message message) {
    //         sendCount++;
    //         return sendCount > nakUntil;
    //     }
    // }
}
