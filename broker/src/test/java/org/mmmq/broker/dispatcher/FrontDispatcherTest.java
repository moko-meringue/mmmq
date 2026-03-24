package org.mmmq.broker.dispatcher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mmmq.broker.dlq.DeadLetterQueue;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class FrontDispatcherTest {

    @Mock
    ObjectProvider<DeadLetterQueue> dlqProvider;

    @Test
    @DisplayName("메시지를 받으면 해당 토픽의 TopicQueue에 추가한다")
    void dispatchAddsMessageToTopicQueue() {
        TopicQueueRegistry registry = new TopicQueueRegistry();
        FrontDispatcher frontDispatcher = new FrontDispatcher(List.of(), registry, dlqProvider);

        Message message = new Message(new Topic("order.new"), Map.of("id", 1));
        frontDispatcher.dispatch(message);

        assertThat(registry.getAll()).hasSize(1);
        assertThat(registry.getAll().iterator().next().get(0)).contains(message);
    }

    @Test
    @DisplayName("서로 다른 토픽의 메시지는 각각의 TopicQueue에 저장된다")
    void dispatchCreatesTopicQueuePerTopic() {
        TopicQueueRegistry registry = new TopicQueueRegistry();
        FrontDispatcher frontDispatcher = new FrontDispatcher(List.of(), registry, dlqProvider);

        frontDispatcher.dispatch(new Message(new Topic("order.new"), Map.of("id", 1)));
        frontDispatcher.dispatch(new Message(new Topic("payment.kakao"), Map.of("id", 2)));

        assertThat(registry.getAll()).hasSize(2);
    }
}
