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
import org.mmmq.broker.topicqueue.TopicQueueRegistry;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.TopicPattern;
import org.mmmq.core.message.Topic;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

@ExtendWith(MockitoExtension.class)
class FrontDispatcherTest {

    @Mock
    TopicQueueRegistry registry; // TopicQueueRegistry를 mock으로 대체해 디스크 I/O 없이 동작 검증
    ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class); // 이벤트 발행 여부를 verify로 검사

    Host host = new Host(WebProtocol.HTTP, "localhost", 8080);

    @Test
    @DisplayName("offer 성공 시 true 반환 및 MessageArrivedEvent 발행")
    void dispatchPersistsAndPublishesEvent() {
        final TopicQueue mockQueue = mock(TopicQueue.class);
        when(registry.get(new Topic("order.new"))).thenReturn(mockQueue); // 특정 토픽 요청에 mock 큐 반환
        when(mockQueue.offer(org.mockito.ArgumentMatchers.any())).thenReturn(true); // 디스크 쓰기 성공 시뮬레이션

        final Dispatcher dispatcher = new Dispatcher("test", host, List.of(new TopicPattern("order.*")));
        final FrontDispatcher frontDispatcher = new FrontDispatcher(registry, publisher);

        final Message message = new Message(new Topic("order.new"), Map.of("id", 1));
        final boolean persisted = frontDispatcher.dispatch(message);

        assertThat(persisted).isTrue(); // offer 성공이면 true 반환
        verify(registry).get(new Topic("order.new")); // 정확한 토픽으로 registry.get이 호출됐는지 확인
        verify(mockQueue).offer(message); // 해당 메시지가 큐에 저장 시도됐는지 확인
        verify(publisher).publishEvent(new MessageArrivedEvent(mockQueue)); // 저장 성공 후 이벤트 발행됐는지 확인
    }

    @Test
    @DisplayName("매칭되는 Dispatcher가 없어도 메시지를 영속화한다")
    void dispatchPersistsEvenWithoutMatchingDispatcher() {
        final TopicQueue mockQueue = mock(TopicQueue.class);
        when(registry.get(new Topic("payment.kakao"))).thenReturn(mockQueue); // "order.*" 패턴과 불일치하는 토픽
        when(mockQueue.offer(org.mockito.ArgumentMatchers.any())).thenReturn(true);

        final Dispatcher dispatcher = new Dispatcher("test", host, List.of(new TopicPattern("order.*"))); // payment 토픽과 불일치
        final FrontDispatcher frontDispatcher = new FrontDispatcher(registry, publisher);

        final Message message = new Message(new Topic("payment.kakao"), Map.of("id", 1));
        final boolean persisted = frontDispatcher.dispatch(message);

        assertThat(persisted).isTrue(); // 수신자 없어도 디스크에 저장하고 ACK 반환
        verify(mockQueue).offer(message); // offer는 dispatcher 매칭 여부와 무관하게 항상 호출됨
    }

    @Test
    @DisplayName("offer 실패 시 false 반환하고 이벤트는 발행하지 않는다")
    void dispatchReturnsFalseWhenOfferFails() {
        final TopicQueue mockQueue = mock(TopicQueue.class);
        when(registry.get(new Topic("order.new"))).thenReturn(mockQueue);
        when(mockQueue.offer(org.mockito.ArgumentMatchers.any())).thenReturn(false); // 디스크 쓰기 실패 시뮬레이션

        final Dispatcher dispatcher = new Dispatcher("test", host, List.of(new TopicPattern("order.*")));
        final FrontDispatcher frontDispatcher = new FrontDispatcher(registry, publisher);

        final Message message = new Message(new Topic("order.new"), Map.of("id", 1));
        final boolean persisted = frontDispatcher.dispatch(message);

        assertThat(persisted).isFalse(); // offer 실패이면 false 반환 → NACK
        org.mockito.Mockito.verifyNoInteractions(publisher); // 저장 실패 시 이벤트를 발행하면 안됨. Dispatcher에게 존재하지 않는 메시지를 읽으라고 알릴 수 있음
    }
}
