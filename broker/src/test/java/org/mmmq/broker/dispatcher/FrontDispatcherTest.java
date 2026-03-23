package org.mmmq.broker.dispatcher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mmmq.broker.dlq.DeadLetterQueue;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//@ExtendWith(MockitoExtension.class)
//class FrontDispatcherTest {
//
//    @Mock
//    ObjectProvider<DeadLetterQueue> deadLetterQueueProvider;
//
//    @Test
//    @DisplayName("Broker는 메시지를 받으면 적절한 MessageDispatcher에 전달할 수 있다.")
//    void forwardMessageTest() {
//        Dispatcher dispatcher = Mockito.mock(Dispatcher.class);
//        FrontDispatcher frontDispatcher = new FrontDispatcher(List.of(dispatcher), deadLetterQueueProvider);
//        when(dispatcher.isSubscribing(new Topic("topic1"))).thenReturn(true);
//
//        Message message = new Message(new Topic("topic1"), Map.of("key1", "value"));
//        frontDispatcher.dispatch(message);
//
//        verify(dispatcher).dispatch(eq(message), any());
//    }
//}
@ExtendWith(MockitoExtension.class)
class FrontDispatcherTest {

    @Mock
    ObjectProvider<DeadLetterQueue> deadLetterQueueProvider;

    @Test
    @DisplayName("Broker는 메시지를 받으면 적절한 MessageDispatcher에 전달한다.")
    void forwardMessageTest() {
        Dispatcher dispatcher = Mockito.mock(Dispatcher.class);
        FrontDispatcher frontDispatcher = new FrontDispatcher(List.of(dispatcher), deadLetterQueueProvider);
        when(dispatcher.isSubscribing(any())).thenReturn(true);
        Message message = new Message(new Topic("topic1"), Map.of("key1", "value"));

        frontDispatcher.dispatch(message);

        verify(dispatcher).dispatch(eq(message), any());
    }
}
