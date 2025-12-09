package org.mmmq.gateway.broker;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.message.Message;
import org.mmmq.gateway.dispatcher.MessageDispatcher;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BrokerTest {

    @Test
    @DisplayName("Broker는 메시지를 받으면 적절한 MessageDispatcher에 전달할 수 있다.")
    void forwardMessageTest() {
        MessageDispatcher messageDispatcher = Mockito.mock(MessageDispatcher.class);
        Broker broker = new Broker(List.of(messageDispatcher));
        when(messageDispatcher.isSubscribing("topic1")).thenReturn(true);

        Message message = new Message("topic1", Map.of("key1", "value"));
        broker.push(message);

        verify(messageDispatcher).push(message);
    }
}
