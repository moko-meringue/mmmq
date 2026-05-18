package org.mmmq.consumer.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.TopicPattern;
import org.mmmq.core.message.Topic;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        FakeHandlerExecution fakeHandlerExecutionA = new FakeHandlerExecution(new TopicPattern("topicA")) {
            @Override
            public void execute(Message message) {
                super.execute(message);
                latch.countDown();
            }
        };
        FakeHandlerExecution fakeHandlerExecutionB = new FakeHandlerExecution(new TopicPattern("topicB")) {
            @Override
            public void execute(Message message) {
                super.execute(message);
                latch.countDown();
            }
        };

        frontHandler.addHandlerExecution(fakeHandlerExecutionA);
        frontHandler.addHandlerExecution(fakeHandlerExecutionB);

        Message messageA = new Message(new Topic("topicA"), Map.of("key", "value A"));
        Message messageB = new Message(new Topic("topicB"), Map.of("key", "value B"));

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
        frontHandler.addHandlerExecution(new HandlerExecution("name", new TopicPattern("topic")) {
            @Override
            public void execute(Message message) {
                latch.countDown();
            }
        });
        Message message = new Message(new Topic("topic"), Map.of("key", "value"));
        frontHandler.handle(message);
        assertThatCode(latch::await).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("동기 핸들은 현재 스레드에서 핸들러를 실행한다")
    void handleSyncTest() {
        Message message = new Message(new Topic("topic"), Map.of("key", "value"));
        FakeHandlerExecution fakeHandlerExecution = new FakeHandlerExecution(new TopicPattern("topic"));

        frontHandler.addHandlerExecution(fakeHandlerExecution);
        frontHandler.handleSync(message);

        assertThat(fakeHandlerExecution.executionCount).isEqualTo(1);
        assertThat(frontHandler.queue).isEmpty();
    }

    @Test
    @DisplayName("동기 핸들 중 예외가 발생하면 호출자에게 전파한다")
    void handleSyncThrowsException() {
        Message message = new Message(new Topic("topic"), Map.of("key", "value"));
        RuntimeException exception = new RuntimeException("handler failed");
        frontHandler.addHandlerExecution(new HandlerExecution("name", new TopicPattern("topic")) {
            @Override
            public void execute(Message message) {
                throw exception;
            }
        });

        assertThatThrownBy(() -> frontHandler.handleSync(message))
                .isSameAs(exception);
    }

    @Test
    @DisplayName("소멸될 때 스레드풀을 종료한다.")
    void destructTest() {
        frontHandler.destroy();
        assertThat(frontHandler.threadPool.isShutdown()).isTrue();
    }

    class FakeHandlerExecution extends HandlerExecution {
        int executionCount = 0;

        protected FakeHandlerExecution(TopicPattern pattern) {
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
