package org.mmmq.consumer.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Pattern;
import org.mmmq.core.message.Topic;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class FrontHandlerTest {

    FrontHandler frontHandler;

    @BeforeEach
    void setUp() {
        frontHandler = new FrontHandler();
    }

    @Test
    @DisplayName("큐 동시성 보장 테스트")
    void queueConcurrencyTest() throws InterruptedException {
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
                        frontHandler.handle(message);
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
        int actualCount = frontHandler.queue.size();

        assertThat(actualCount).isEqualTo(expectedCount);
    }

    @Test
    @DisplayName("메시지 실행 테스트")
    void executeTest() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);

        FakeHandlerExecution fakeHandlerExecutionA = new FakeHandlerExecution(new Pattern("topic a")) {
            @Override
            public void execute(Message message) {
                super.execute(message);
                latch.countDown();
            }
        };
        FakeHandlerExecution fakeHandlerExecutionB = new FakeHandlerExecution(new Pattern("topic b")) {
            @Override
            public void execute(Message message) {
                super.execute(message);
                latch.countDown();
            }
        };

        frontHandler.addHandlerExecution(fakeHandlerExecutionA);
        frontHandler.addHandlerExecution(fakeHandlerExecutionB);

        Message messageA = new Message(new Topic("topic a"), Map.of("key", "value A"), new Pattern("topic a"));
        Message messageB = new Message(new Topic("topic b"), Map.of("key", "value B"), new Pattern("topic b"));

        frontHandler.start();

        frontHandler.handle(messageA);
        frontHandler.handle(messageB);
        frontHandler.handle(messageB);

        latch.await();

        assertThat(fakeHandlerExecutionA.executionCount).isEqualTo(1);
        assertThat(fakeHandlerExecutionB.executionCount).isEqualTo(2);
    }

    @Test
    @DisplayName("핸들 테스트")
    void handleTest() {
        frontHandler.start();
        CountDownLatch latch = new CountDownLatch(1);
        frontHandler.addHandlerExecution(new HandlerExecution("name", new Pattern("topic")) {
            @Override
            public void execute(Message message) {
                latch.countDown();
            }
        });
        Message message = new Message(new Topic("topic"), Map.of("key", "value"), new Pattern("topic"));
        frontHandler.handle(message);
        assertThatCode(latch::await).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("소멸될 때 스레드풀을 종료한다.")
    void destructTest() {
        frontHandler.stop();
        assertThat(frontHandler.threadPool.isShutdown()).isTrue();
    }

    class FakeHandlerExecution extends HandlerExecution {
        int executionCount = 0;

        protected FakeHandlerExecution(Pattern pattern) {
            super("FakeHandlerExecution", pattern);
        }

        @Override
        public void execute(Message message) {
            synchronized (this) {
                executionCount++;
            }
        }
    }
}
