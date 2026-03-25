package org.mmmq.broker.dispatcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Pattern;
import org.mmmq.core.message.Topic;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FrontDispatcherTest {

    @Mock
    TopicQueueRegistry registry;

    Host host = new Host("http", "localhost", 8080);

    @Test
    @DisplayName("매칭되는 Dispatcher가 있으면 해당 TopicQueue에 메시지를 추가한다")
    void dispatchToMatchingTopicQueue() {
        TopicQueue mockQueue = mock(TopicQueue.class);
        when(registry.getOrCreateQueue(new Topic("order.new"))).thenReturn(mockQueue);

        Dispatcher dispatcher = new Dispatcher("test", host, List.of(new Pattern("order.*")));
        FrontDispatcher frontDispatcher = new FrontDispatcher(List.of(dispatcher), registry);

        Message message = new Message(new Topic("order.new"), Map.of("id", 1));
        frontDispatcher.dispatch(message);

        verify(registry).getOrCreateQueue(new Topic("order.new"));
        verify(mockQueue).add(message);
    }

    @Test
    @DisplayName("매칭되는 Dispatcher가 없으면 TopicQueue를 생성하지 않는다")
    void dispatchIgnoresUnmatchedTopic() {
        Dispatcher dispatcher = new Dispatcher("test", host, List.of(new Pattern("order.*")));
        FrontDispatcher frontDispatcher = new FrontDispatcher(List.of(dispatcher), registry);

        frontDispatcher.dispatch(new Message(new Topic("payment.kakao"), Map.of("id", 1)));

        verify(registry, never()).getOrCreateQueue(any());
    }
}
