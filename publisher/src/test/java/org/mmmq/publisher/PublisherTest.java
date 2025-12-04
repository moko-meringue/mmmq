package org.mmmq.publisher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.Host;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.GatewayAcknowledgement;
import org.mmmq.core.message.Message;
import org.mockito.MockedConstruction;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class PublisherTest {

    @Test
    @DisplayName("기본 생성자는 기본 재시도 횟수를 설정한다")
    void constructorWithHostOnly() {
        Host host = mock(Host.class);

        Publisher publisher = new Publisher(host);

        assertThat(publisher.maxRetryCount).isEqualTo(Publisher.DEFAULT_MAX_RETRY_COUNT);
    }

    @Test
    @DisplayName("재시도 횟수를 지정하는 생성자는 지정된 값을 설정한다")
    void constructorWithHostAndMaxRetryCount() {
        Host host = mock(Host.class);
        int customRetryCount = 5;

        Publisher publisher = new Publisher(host, customRetryCount);

        assertThat(publisher.maxRetryCount).isEqualTo(customRetryCount);
    }

    @Test
    @DisplayName("메시지 발행이 성공하면 재시도 없이 완료된다")
    void publishSuccess_NoRetry() {
        Host host = mock(Host.class);
        Message message = new Message("test-topic", Map.of("key", "value"));
        GatewayAcknowledgement ackResponse = new GatewayAcknowledgement(Acknowledgement.ACK);

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> when(mock.send(message)).thenReturn(ackResponse)
                )
        ) {

            Publisher publisher = new Publisher(host);
            publisher.publish(message);

            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(1)).send(message);
        }
    }

    @Test
    @DisplayName("메시지 발행이 NAK이면 재시도하고 결국 성공한다")
    void publishRetryUntilSuccess() {
        Host host = mock(Host.class);
        Message message = new Message("test-topic", Map.of("key", "value"));
        GatewayAcknowledgement nakResponse = new GatewayAcknowledgement(Acknowledgement.NACK);
        GatewayAcknowledgement ackResponse = new GatewayAcknowledgement(Acknowledgement.ACK);

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> when(mock.send(message))
                                .thenReturn(nakResponse)
                                .thenReturn(nakResponse)
                                .thenReturn(ackResponse)
                )
        ) {

            Publisher publisher = new Publisher(host);
            publisher.publish(message);

            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(3)).send(message);
        }
    }

    @Test
    @DisplayName("최대 재시도 횟수까지 NAK이면 재시도를 중단한다")
    void publishExceedMaxRetry() {
        Host host = mock(Host.class);
        Message message = new Message("test-topic", Map.of("key", "value"));
        GatewayAcknowledgement nakResponse = new GatewayAcknowledgement(Acknowledgement.NACK);
        int maxRetryCount = 2;

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> when(mock.send(message)).thenReturn(nakResponse)
                )
        ) {

            Publisher publisher = new Publisher(host, maxRetryCount);
            publisher.publish(message);

            Gateway gateway = mockedGateway.constructed().get(0);
            verify(gateway, times(maxRetryCount + 1)).send(message);
        }
    }

    @Test
    @DisplayName("Gateway에서 예외가 발생하면 MessagePublishException을 던진다")
    void publishThrowsException_WhenGatewayFails() {
        Host host = mock(Host.class);
        Message message = new Message("test-topic", Map.of("key", "value"));
        RuntimeException gatewayException = new RuntimeException("Gateway error");

        try (
                MockedConstruction<Gateway> mockedGateway = mockConstruction(
                        Gateway.class,
                        (mock, context) -> when(mock.send(message)).thenThrow(gatewayException)
                )
        ) {

            Publisher publisher = new Publisher(host);

            assertThatThrownBy(() -> publisher.publish(message))
                    .isInstanceOf(MessagePublishException.class)
                    .hasMessage("Failed to publish message")
                    .hasCause(gatewayException);
        }
    }
}
