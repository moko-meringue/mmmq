package org.mmmq.broker.dispatcher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueContainer;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FrontDispatcherTest {

    @Mock
    TopicQueueContainer container;

    @Mock
    DispatcherContainer dispatcherContainer;

    @Test
    @DisplayName("offer 성공 시 ACK 반환 및 DispatcherContainer로 dispatch 위임")
    void dispatchPersistsAndDelegatesToDispatcherContainer() {
        TopicQueue mockQueue = mock(TopicQueue.class);
        when(container.get(new Topic("order.new"))).thenReturn(mockQueue);
        when(mockQueue.offer(any())).thenReturn(true);
        when(dispatcherContainer.subscribers(mockQueue)).thenReturn(List.of());

        FrontDispatcher frontDispatcher = new FrontDispatcher(container, dispatcherContainer);
        Message message = new Message(new Topic("order.new"), Map.of("id", 1));

        Acknowledgement acknowledgement = frontDispatcher.dispatch(message);

        assertThat(acknowledgement).isEqualTo(Acknowledgement.ACK);
        verify(container).get(new Topic("order.new"));
        verify(mockQueue).offer(message);
        verify(dispatcherContainer).subscribers(mockQueue);
    }

    @Test
    @DisplayName("매칭되는 Dispatcher가 없어도 메시지를 영속화하고 dispatch는 위임된다")
    void dispatchPersistsEvenWithoutMatchingDispatcher() {
        TopicQueue mockQueue = mock(TopicQueue.class);
        when(container.get(new Topic("payment.kakao"))).thenReturn(mockQueue);
        when(mockQueue.offer(any())).thenReturn(true);
        when(dispatcherContainer.subscribers(mockQueue)).thenReturn(List.of());

        FrontDispatcher frontDispatcher = new FrontDispatcher(container, dispatcherContainer);
        Message message = new Message(new Topic("payment.kakao"), Map.of("id", 1));

        Acknowledgement acknowledgement = frontDispatcher.dispatch(message);

        assertThat(acknowledgement).isEqualTo(Acknowledgement.ACK);
        verify(mockQueue).offer(message);
        verify(dispatcherContainer).subscribers(mockQueue);
    }

    @Test
    @DisplayName("offer 실패 시 NACK 반환하고 dispatch는 호출하지 않는다")
    void dispatchReturnsNackWhenOfferFails() {
        TopicQueue mockQueue = mock(TopicQueue.class);
        when(container.get(new Topic("order.new"))).thenReturn(mockQueue);
        when(mockQueue.offer(any())).thenReturn(false);

        FrontDispatcher frontDispatcher = new FrontDispatcher(container, dispatcherContainer);
        Message message = new Message(new Topic("order.new"), Map.of("id", 1));

        Acknowledgement acknowledgement = frontDispatcher.dispatch(message);

        assertThat(acknowledgement).isEqualTo(Acknowledgement.NACK);
        verifyNoInteractions(dispatcherContainer);
    }
}
