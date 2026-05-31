package org.mmmq.consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.consumer.handler.execution.HandlerExecutionContainer;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.ConsumerAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * id 기반 라우팅 모델로 전환되면서 FrontHandler가 제거되었다.
 * FrontHandlerTest가 검증하던 "메시지가 적절한 핸들러로 라우팅된다"는 의도를
 * Consumer Controller + HandlerExecutionContainer 조합으로 재작성한다.
 */
class ConsumerRoutingTest {

    HandlerExecutionContainer handlerExecutionContainer;
    Consumer consumer;

    @BeforeEach
    void setUp() {
        handlerExecutionContainer = new HandlerExecutionContainer();
        consumer = new Consumer(handlerExecutionContainer);
    }

    @Test
    @DisplayName("handler id로 등록된 핸들러가 메시지를 처리하고 ACK를 반환한다")
    void routesMessageToHandlerById() {
        CountDownLatch latch = new CountDownLatch(1);
        FakeHandlerExecution handler = new FakeHandlerExecution("handler-A") {
            @Override
            public void execute(Message message) {
                super.execute(message);
                latch.countDown();
            }
        };
        handlerExecutionContainer.add(handler);

        Message message = new Message(new Topic("topic"), Map.of("key", "value"));
        ResponseEntity<ConsumerAcknowledgement> response = consumer.receiveMessage(
                handlerIdHeader("handler-A"),
                message
        );

        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().acknowledgement()).isEqualTo(Acknowledgement.ACK);
        assertThat(handler.executionCount.get()).isEqualTo(1);
        assertThat(latch.getCount()).isZero();
    }

    @Test
    @DisplayName("핸들러는 자신의 id로 들어온 메시지만 받는다")
    void deliversOnlyToTargetedHandler() {
        FakeHandlerExecution handlerA = new FakeHandlerExecution("handler-A");
        FakeHandlerExecution handlerB = new FakeHandlerExecution("handler-B");
        handlerExecutionContainer.add(handlerA);
        handlerExecutionContainer.add(handlerB);

        Message messageA = new Message(new Topic("topicA"), Map.of("key", "value A"));
        Message messageB1 = new Message(new Topic("topicB"), Map.of("key", "value B1"));
        Message messageB2 = new Message(new Topic("topicB"), Map.of("key", "value B2"));

        consumer.receiveMessage(handlerIdHeader("handler-A"), messageA);
        consumer.receiveMessage(handlerIdHeader("handler-B"), messageB1);
        consumer.receiveMessage(handlerIdHeader("handler-B"), messageB2);

        assertThat(handlerA.executionCount.get()).isEqualTo(1);
        assertThat(handlerB.executionCount.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("handler id 헤더가 없으면 NACK를 반환한다")
    void returnsNackWhenHandlerIdMissing() {
        FakeHandlerExecution handler = new FakeHandlerExecution("handler-A");
        handlerExecutionContainer.add(handler);

        ResponseEntity<ConsumerAcknowledgement> response = consumer.receiveMessage(
                new HashMap<>(),
                new Message(new Topic("topic"), Map.of("key", "value"))
        );

        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().acknowledgement()).isEqualTo(Acknowledgement.NACK);
        assertThat(handler.executionCount.get()).isZero();
    }

    @Test
    @DisplayName("등록되지 않은 handler id이면 NACK를 반환한다")
    void returnsNackWhenHandlerNotFound() {
        ResponseEntity<ConsumerAcknowledgement> response = consumer.receiveMessage(
                handlerIdHeader("unknown-handler"),
                new Message(new Topic("topic"), Map.of("key", "value"))
        );

        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().acknowledgement()).isEqualTo(Acknowledgement.NACK);
    }

    @Test
    @DisplayName("핸들러가 예외를 던지면 NACK를 반환한다")
    void returnsNackWhenHandlerThrows() {
        handlerExecutionContainer.add(new HandlerExecution() {
            @Override
            public String id() {
                return "failing-handler";
            }

            @Override
            public void execute(Message message) {
                throw new IllegalStateException("boom");
            }
        });

        ResponseEntity<ConsumerAcknowledgement> response = consumer.receiveMessage(
                handlerIdHeader("failing-handler"),
                new Message(new Topic("topic"), Map.of("key", "value"))
        );

        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().acknowledgement()).isEqualTo(Acknowledgement.NACK);
    }

    @Test
    @DisplayName("동시에 여러 스레드가 라우팅을 요청해도 정확히 호출된다")
    void concurrentRoutingDeliversAllMessages() throws InterruptedException {
        int threadCount = 50;
        int messagesPerThread = 20;
        FakeHandlerExecution handler = new FakeHandlerExecution("handler-A");
        handlerExecutionContainer.add(handler);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < messagesPerThread; j++) {
                        Message message = new Message(
                                new Topic("topic"),
                                Map.of("threadId", threadId, "seq", j)
                        );
                        consumer.receiveMessage(handlerIdHeader("handler-A"), message);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean finished = endLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(finished).isTrue();
        assertThat(handler.executionCount.get()).isEqualTo(threadCount * messagesPerThread);
    }

    private Map<String, String> handlerIdHeader(String handlerId) {
        Map<String, String> headers = new HashMap<>();
        headers.put("mmmq-handler-id", handlerId);
        return headers;
    }

    static class FakeHandlerExecution implements HandlerExecution {

        private final String id;
        final AtomicInteger executionCount = new AtomicInteger();

        FakeHandlerExecution(String id) {
            this.id = id;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public void execute(Message message) {
            executionCount.incrementAndGet();
        }
    }
}
