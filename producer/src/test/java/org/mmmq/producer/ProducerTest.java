package org.mmmq.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.Host;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.BrokerAcknowledgement;
import org.mmmq.core.backoff.ExponentialBackoff;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.producer.exception.ProduceException;
import org.mockito.MockedConstruction;

class ProducerTest {

    private static final ExponentialBackoff FAST_BACKOFF = new ExponentialBackoff(
            Duration.ofMillis(1),
            Duration.ofMillis(1),
            2.0
    );

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
    @DisplayName("백오프를 지정하는 생성자는 지정된 백오프를 설정한다")
    void constructorWithBackoff() {
        Host host = mock(Host.class);
        ExponentialBackoff customBackoff = new ExponentialBackoff(
                Duration.ofMillis(10),
                Duration.ofMillis(100),
                3.0
        );

        Producer producer = new Producer(host, customBackoff);

        assertThat(producer.backoff).isEqualTo(customBackoff);
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

            Producer producer = new Producer(host, FAST_BACKOFF);
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

            Producer producer = new Producer(host, FAST_BACKOFF);
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

            Producer producer = new Producer(host, maxRetryCount, FAST_BACKOFF);

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
            int maxRetryCount = 2;
            Producer producer = new Producer(host, maxRetryCount, FAST_BACKOFF);

            assertThatThrownBy(() -> producer.produce(message))
                    .isInstanceOf(ProduceException.class)
                    .hasMessage("Failed to produce message")
                    .hasCause(gatewayException);

            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(maxRetryCount + 1)).send(message);
        }
    }

    @Test
    @DisplayName("Gateway 예외가 일시적으로 발생하면 백오프 후 재시도하고 성공한다")
    void produceRetryUntilSuccess_WhenGatewayTemporarilyFails() {
        Host host = mock(Host.class);
        Message message = new Message(new Topic("test-topic"), Map.of("key", "value"));
        RuntimeException gatewayException = new RuntimeException("Gateway error");
        BrokerAcknowledgement ackResponse = new BrokerAcknowledgement(Acknowledgement.ACK);

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> when(mock.send(message))
                                .thenThrow(gatewayException)
                                .thenReturn(ackResponse)
                )
        ) {
            Producer producer = new Producer(host, FAST_BACKOFF);

            producer.produce(message);

            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(2)).send(message);
        }
    }

    @Test
    @DisplayName("NACK 재시도 시에는 백오프가 적용되지 않는다")
    void nackDoesNotTriggerBackoff() {
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
            ExponentialBackoff slowBackoff = new ExponentialBackoff(
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    2.0
            );
            Producer producer = new Producer(host, slowBackoff);

            assertTimeout(Duration.ofMillis(200), () -> producer.produce(message));

            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(3)).send(message);
        }
    }
}
