package org.mmmq.broker;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.dispatcher.FrontDispatcher;
import org.mmmq.core.message.Message;
import org.mockito.Mockito;

class FrontDispatcherTest {

    @Test
    @DisplayName("Broker는 메시지를 받으면 적절한 MessageDispatcher에 전달할 수 있다.")
    void forwardMessageTest() {
        Dispatcher dispatcher = Mockito.mock(Dispatcher.class);
        FrontDispatcher frontDispatcher = new FrontDispatcher(List.of(dispatcher));
        when(dispatcher.isSubscribing("topic1")).thenReturn(true);

        Message message = new Message("topic1", Map.of("key1", "value"));
        frontDispatcher.push(message);

        verify(dispatcher).push(message);
    }
}
