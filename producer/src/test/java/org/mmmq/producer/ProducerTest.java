package org.mmmq.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.Host;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.BrokerAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.producer.exception.ProduceException;
import org.mockito.MockedConstruction;

class ProducerTest {

    @Test
    @DisplayName("기본 생성자는 기본 재시도 횟수를 설정한다")
    void constructorWithHostOnly() {
        Host host = mock(Host.class);

        Producer producer = new Producer(host);

        assertThat(producer.maxRetryCount).isEqualTo(Producer.DEFAULT_MAX_RETRY_COUNT);
    }

    @Test
    @DisplayName("재시도 횟수를 지정하는 생성자는 지정된 값을 설정한다")
    void constructorWithHostAndMaxRetryCount() {
        Host host = mock(Host.class);
        int customRetryCount = 5;

        Producer producer = new Producer(host, customRetryCount);

        assertThat(producer.maxRetryCount).isEqualTo(customRetryCount);
    }

    @Test
    @DisplayName("메시지 발행이 성공하면 재시도 없이 완료된다")
    void produceSuccess_NoRetry() {
        Host host = mock(Host.class);
        Message message = new Message(new Topic("test-topic"), Map.of("key", "value"));
        BrokerAcknowledgement ackResponse = new BrokerAcknowledgement(Acknowledgement.ACK);

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> when(mock.send(message)).thenReturn(ackResponse)
                )
        ) {

            Producer producer = new Producer(host);
            producer.produce(message);

            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(1)).send(message);
        }
    }

    @Test
    @DisplayName("메시지 발행이 NAK이면 재시도하고 결국 성공한다")
    void produceRetryUntilSuccess() {
        Host host = mock(Host.class);
        Message message = new Message(new Topic("test-topic"), Map.of("key", "value"));
        BrokerAcknowledgement nakResponse = new BrokerAcknowledgement(Acknowledgement.NACK);
        BrokerAcknowledgement ackResponse = new BrokerAcknowledgement(Acknowledgement.ACK);

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> when(mock.send(message))
                                .thenReturn(nakResponse)
                                .thenReturn(nakResponse)
                                .thenReturn(ackResponse)
                )
        ) {

            Producer producer = new Producer(host);
            producer.produce(message);

            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(3)).send(message);
        }
    }

    @Test
    @DisplayName("최대 재시도 횟수까지 NAK이면 ProduceException을 던진다")
    void produceExceedMaxRetry() {
        Host host = mock(Host.class);
        Message message = new Message(new Topic("test-topic"), Map.of("key", "value"));
        BrokerAcknowledgement nakResponse = new BrokerAcknowledgement(Acknowledgement.NACK);
        int maxRetryCount = 2;

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> when(mock.send(message)).thenReturn(nakResponse)
                )
        ) {

            Producer producer = new Producer(host, maxRetryCount);

            assertThatThrownBy(() -> producer.produce(message))
                    .isInstanceOf(ProduceException.class)
                    .hasMessageContaining(String.valueOf(maxRetryCount + 1));

            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(maxRetryCount + 1)).send(message);
        }
    }

    @Test
    @DisplayName("Gateway에서 예외가 발생하면 ProduceException을 던진다")
    void produceThrowsException_WhenGatewayFails() {
        Host host = mock(Host.class);
        Message message = new Message(new Topic("test-topic"), Map.of("key", "value"));
        RuntimeException gatewayException = new RuntimeException("Gateway error");

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> when(mock.send(message)).thenThrow(gatewayException)
                )
        ) {
            Producer producer = new Producer(host);

            assertThatThrownBy(() -> producer.produce(message))
                    .isInstanceOf(ProduceException.class)
                    .hasMessage("Failed to produce message")
                    .hasCause(gatewayException);
        }
    }

    @Test
    @DisplayName("비동기 발행은 배치 크기에 도달하면 백그라운드에서 전송한다")
    void produceAsyncFlushesWhenBatchSizeIsReached() throws InterruptedException {
        Host host = mock(Host.class);
        Message firstMessage = new Message(new Topic("test-topic"), Map.of("key", "first"));
        Message secondMessage = new Message(new Topic("test-topic"), Map.of("key", "second"));
        BrokerAcknowledgement ackResponse = new BrokerAcknowledgement(Acknowledgement.ACK);
        CountDownLatch latch = new CountDownLatch(2);

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> doAnswer(invocation -> {
                            latch.countDown();
                            return ackResponse;
                        }).when(mock).send(any(Message.class))
                );
                Producer producer = Producer.builder(host)
                        .batchSize(2)
                        .flushInterval(Duration.ofSeconds(30))
                        .build()
        ) {

            producer.produceAsync(firstMessage);
            producer.produceAsync(secondMessage);

            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(1)).send(firstMessage);
            verify(gateway, times(1)).send(secondMessage);
        }
    }

    @Test
    @DisplayName("비동기 발행은 플러시 간격이 지나면 미달 배치도 전송한다")
    void produceAsyncFlushesWhenIntervalPasses() throws InterruptedException {
        Host host = mock(Host.class);
        Message message = new Message(new Topic("test-topic"), Map.of("key", "value"));
        BrokerAcknowledgement ackResponse = new BrokerAcknowledgement(Acknowledgement.ACK);
        CountDownLatch latch = new CountDownLatch(1);

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> doAnswer(invocation -> {
                            latch.countDown();
                            return ackResponse;
                        }).when(mock).send(message)
                );
                Producer producer = Producer.builder(host)
                        .batchSize(10)
                        .flushInterval(Duration.ofMillis(50))
                        .build()
        ) {

            producer.produceAsync(message);

            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(1)).send(message);
        }
    }

    @Test
    @DisplayName("비동기 발행 큐가 가득 차면 ProduceException을 던진다")
    void produceAsyncThrowsExceptionWhenQueueIsFull() {
        Host host = mock(Host.class);
        Message firstMessage = new Message(new Topic("test-topic"), Map.of("key", "first"));
        Message secondMessage = new Message(new Topic("test-topic"), Map.of("key", "second"));

        try (
                MockedConstruction<Gateway> ignored = mockConstruction(Gateway.class);
                Producer producer = Producer.builder(host)
                        .asyncQueueCapacity(1)
                        .batchSize(10)
                        .flushInterval(Duration.ofSeconds(30))
                        .build()
        ) {

            producer.produceAsync(firstMessage);

            assertThatThrownBy(() -> producer.produceAsync(secondMessage))
                    .isInstanceOf(ProduceException.class)
                    .hasMessage("Async producer queue is full");
        }
    }

    @Test
    @DisplayName("비동기 발행도 NACK이면 기존 재시도 횟수만큼 재시도한다")
    void produceAsyncRetriesWhenBrokerReturnsNack() throws InterruptedException {
        Host host = mock(Host.class);
        Message message = new Message(new Topic("test-topic"), Map.of("key", "value"));
        BrokerAcknowledgement nakResponse = new BrokerAcknowledgement(Acknowledgement.NACK);
        BrokerAcknowledgement ackResponse = new BrokerAcknowledgement(Acknowledgement.ACK);
        CountDownLatch latch = new CountDownLatch(3);

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> doAnswer(invocation -> {
                            latch.countDown();
                            long remainingCount = latch.getCount();
                            if (remainingCount == 0) {
                                return ackResponse;
                            }
                            return nakResponse;
                        }).when(mock).send(message)
                );
                Producer producer = Producer.builder(host)
                        .batchSize(1)
                        .flushInterval(Duration.ofSeconds(30))
                        .build()
        ) {

            producer.produceAsync(message);

            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(3)).send(message);
        }
    }

    @Test
    @DisplayName("Producer를 종료하면 남은 비동기 메시지를 전송한다")
    void closeFlushesRemainingAsyncMessages() {
        Host host = mock(Host.class);
        Message message = new Message(new Topic("test-topic"), Map.of("key", "value"));
        BrokerAcknowledgement ackResponse = new BrokerAcknowledgement(Acknowledgement.ACK);

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> when(mock.send(message)).thenReturn(ackResponse)
                );
                Producer producer = Producer.builder(host)
                        .batchSize(10)
                        .flushInterval(Duration.ofSeconds(30))
                        .build()
        ) {

            producer.produceAsync(message);
            producer.close();

            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(1)).send(message);
        }
    }
}
