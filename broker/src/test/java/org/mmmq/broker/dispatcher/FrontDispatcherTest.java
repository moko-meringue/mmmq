package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueContainer;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.core.message.TopicPattern;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

@ExtendWith(MockitoExtension.class)
class FrontDispatcherTest {

    @Mock
    TopicQueueContainer container;
    ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

    Host host = new Host(WebProtocol.HTTP, "localhost", 8080);

    @Test
    @DisplayName("offer 성공 시 ACK 반환 및 MessageArrivedEvent 발행")
    void dispatchPersistsAndPublishesEvent() {
        final TopicQueue mockQueue = mock(TopicQueue.class);
        when(container.get(new Topic("order.new"))).thenReturn(mockQueue);
        when(mockQueue.offer(org.mockito.ArgumentMatchers.any())).thenReturn(true);

        final Dispatcher dispatcher = new Dispatcher("test", host, List.of(new TopicPattern("order.*")));
        final FrontDispatcher frontDispatcher = new FrontDispatcher(container, publisher);

        final Message message = new Message(new Topic("order.new"), Map.of("id", 1));
        final Acknowledgement acknowledgement = frontDispatcher.dispatch(message);

        assertThat(acknowledgement).isEqualTo(Acknowledgement.ACK);
        verify(container).get(new Topic("order.new"));
        verify(mockQueue).offer(message);
        verify(publisher).publishEvent(new MessageArrivedEvent(mockQueue));
    }

    @Test
    @DisplayName("매칭되는 Dispatcher가 없어도 메시지를 영속화한다")
    void dispatchPersistsEvenWithoutMatchingDispatcher() {
        final TopicQueue mockQueue = mock(TopicQueue.class);
        when(container.get(new Topic("payment.kakao"))).thenReturn(mockQueue);
        when(mockQueue.offer(org.mockito.ArgumentMatchers.any())).thenReturn(true);

        final Dispatcher dispatcher = new Dispatcher("test", host, List.of(new TopicPattern("order.*")));
        final FrontDispatcher frontDispatcher = new FrontDispatcher(container, publisher);

        final Message message = new Message(new Topic("payment.kakao"), Map.of("id", 1));
        final Acknowledgement acknowledgement = frontDispatcher.dispatch(message);

        assertThat(acknowledgement).isEqualTo(Acknowledgement.ACK);
        verify(mockQueue).offer(message);
    }

    @Test
    @DisplayName("offer 실패 시 NACK 반환하고 이벤트는 발행하지 않는다")
    void dispatchReturnsNackWhenOfferFails() {
        final TopicQueue mockQueue = mock(TopicQueue.class);
        when(container.get(new Topic("order.new"))).thenReturn(mockQueue);
        when(mockQueue.offer(org.mockito.ArgumentMatchers.any())).thenReturn(false);

        final Dispatcher dispatcher = new Dispatcher("test", host, List.of(new TopicPattern("order.*")));
        final FrontDispatcher frontDispatcher = new FrontDispatcher(container, publisher);

        final Message message = new Message(new Topic("order.new"), Map.of("id", 1));
        final Acknowledgement acknowledgement = frontDispatcher.dispatch(message);

        assertThat(acknowledgement).isEqualTo(Acknowledgement.NACK);
        org.mockito.Mockito.verifyNoInteractions(publisher);
    }
}
