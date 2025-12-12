package org.mmmq.broker.dispatcher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FrontDispatcherTest {

    @Test
    @DisplayName("Broker는 메시지를 받으면 적절한 MessageDispatcher에 전달할 수 있다.")
    void forwardMessageTest() {
        Dispatcher dispatcher = Mockito.mock(Dispatcher.class);
        FrontDispatcher frontDispatcher = new FrontDispatcher(List.of(dispatcher));
        when(dispatcher.isSubscribing(new Topic("topic1"))).thenReturn(true);

        Message message = new Message(new Topic("topic1"), Map.of("key1", "value"));
        frontDispatcher.push(message);

        verify(dispatcher).dispatch(message);
    }
}
