package org.mmmq.broker.subscription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
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

@ExtendWith(MockitoExtension.class)
class FrontDispatcherTest {

    @Mock
    TopicQueueContainer container;

    @Mock
    SubscriptionContainer subscriptionContainer;

    @Test
    @DisplayName("offer 성공 시 ACK 반환 및 SubscriptionContainer로 트리거 위임")
    void dispatchPersistsAndDelegatesToSubscriptionContainer() {
        TopicQueue mockQueue = mock(TopicQueue.class);
        when(container.getOrCreate(new Topic("order.new"))).thenReturn(mockQueue);
        when(mockQueue.offer(any())).thenReturn(true);

        FrontDispatcher frontDispatcher = new FrontDispatcher(container, subscriptionContainer);
        Message message = new Message(new Topic("order.new"), Map.of("id", 1));

        Acknowledgement acknowledgement = frontDispatcher.dispatch(message);

        assertThat(acknowledgement).isEqualTo(Acknowledgement.ACK);
        verify(container).getOrCreate(new Topic("order.new"));
        verify(mockQueue).offer(message);
        verify(subscriptionContainer).trigger(mockQueue);
    }

    @Test
    @DisplayName("매칭되는 구독이 없어도 메시지를 영속화하고 트리거는 위임된다")
    void dispatchPersistsEvenWithoutMatchingSubscription() {
        TopicQueue mockQueue = mock(TopicQueue.class);
        when(container.getOrCreate(new Topic("payment.kakao"))).thenReturn(mockQueue);
        when(mockQueue.offer(any())).thenReturn(true);

        FrontDispatcher frontDispatcher = new FrontDispatcher(container, subscriptionContainer);
        Message message = new Message(new Topic("payment.kakao"), Map.of("id", 1));

        Acknowledgement acknowledgement = frontDispatcher.dispatch(message);

        assertThat(acknowledgement).isEqualTo(Acknowledgement.ACK);
        verify(mockQueue).offer(message);
        verify(subscriptionContainer).trigger(mockQueue);
    }

    @Test
    @DisplayName("offer 실패 시 NACK 반환하고 트리거는 호출하지 않는다")
    void dispatchReturnsNackWhenOfferFails() {
        TopicQueue mockQueue = mock(TopicQueue.class);
        when(container.getOrCreate(new Topic("order.new"))).thenReturn(mockQueue);
        when(mockQueue.offer(any())).thenReturn(false);

        FrontDispatcher frontDispatcher = new FrontDispatcher(container, subscriptionContainer);
        Message message = new Message(new Topic("order.new"), Map.of("id", 1));

        Acknowledgement acknowledgement = frontDispatcher.dispatch(message);

        assertThat(acknowledgement).isEqualTo(Acknowledgement.NACK);
        verifyNoInteractions(subscriptionContainer);
    }
}
