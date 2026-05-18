package org.mmmq.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.consumer.config.ConsumerMode;
import org.mmmq.consumer.config.ConsumerProperties;
import org.mmmq.consumer.handler.FrontHandler;
import org.mmmq.core.acknowledgement.ConsumerAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.http.ResponseEntity;

class ConsumerTest {

    @Test
    @DisplayName("기본 소비 모드는 ASYNC이다")
    void defaultModeIsAsync() {
        ConsumerProperties consumerProperties = new ConsumerProperties();

        assertThat(consumerProperties.getMode()).isEqualTo(ConsumerMode.ASYNC);
    }

    @Test
    @DisplayName("ASYNC 모드는 메시지를 큐에 적재하고 ACK를 반환한다")
    void receiveMessageWithAsyncMode() {
        FrontHandler frontHandler = mock(FrontHandler.class);
        ConsumerProperties consumerProperties = new ConsumerProperties();
        Consumer consumer = new Consumer(frontHandler, consumerProperties);
        Message message = new Message(new Topic("topic"), Map.of("key", "value"));

        ResponseEntity<ConsumerAcknowledgement> response = consumer.receiveMessage(message);

        assertThat(response.getBody().isAck()).isTrue();
        verify(frontHandler).handleAsync(message);
        verify(frontHandler, never()).handleSync(message);
    }

    @Test
    @DisplayName("SYNC 모드는 핸들러를 직접 실행하고 ACK를 반환한다")
    void receiveMessageWithSyncMode() {
        FrontHandler frontHandler = mock(FrontHandler.class);
        ConsumerProperties consumerProperties = new ConsumerProperties();
        consumerProperties.setMode(ConsumerMode.SYNC);
        Consumer consumer = new Consumer(frontHandler, consumerProperties);
        Message message = new Message(new Topic("topic"), Map.of("key", "value"));

        ResponseEntity<ConsumerAcknowledgement> response = consumer.receiveMessage(message);

        assertThat(response.getBody().isAck()).isTrue();
        verify(frontHandler).handleSync(message);
        verify(frontHandler, never()).handleAsync(message);
    }

    @Test
    @DisplayName("SYNC 모드에서 핸들러 실행이 실패하면 NACK를 반환한다")
    void receiveMessageWithSyncModeReturnsNackWhenHandlerFails() {
        FrontHandler frontHandler = mock(FrontHandler.class);
        ConsumerProperties consumerProperties = new ConsumerProperties();
        consumerProperties.setMode(ConsumerMode.SYNC);
        Consumer consumer = new Consumer(frontHandler, consumerProperties);
        Message message = new Message(new Topic("topic"), Map.of("key", "value"));

        doThrow(new IllegalStateException("handler failed")).when(frontHandler).handleSync(message);

        ResponseEntity<ConsumerAcknowledgement> response = consumer.receiveMessage(message);

        assertThat(response.getBody().isAck()).isFalse();
    }
}
