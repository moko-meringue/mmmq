package org.mmmq.broker.subscription;

import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueContainer;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.message.Message;
import org.springframework.stereotype.Component;

/**
 * 브로커로 들어온 메시지를 받는 첫 진입점. 영속화하고 구독자에게 도착을 알린다.
 *
 * <p>{@code dispatcher} 패키지가 아니라 여기 있는 이유는 {@link SubscriptionContainer}를 참조해야 해서다 —
 * {@code dispatcher}에 남으면 {@code dispatcher → subscription} 의존이 생겨 {@code subscription → dispatcher}와
 * 함께 순환이 된다. 여기로 옮기면 {@link org.mmmq.broker.dispatcher.Dispatcher}를 아예 알 필요가 없다 —
 * 어떤 Dispatcher가 매칭되는지는 {@link SubscriptionContainer}만 안다.
 */
@Component
public class FrontDispatcher {

    private final TopicQueueContainer container;
    private final SubscriptionContainer subscriptionContainer;

    public FrontDispatcher(TopicQueueContainer container, SubscriptionContainer subscriptionContainer) {
        this.container = container;
        this.subscriptionContainer = subscriptionContainer;
    }

    public Acknowledgement dispatch(Message message) {
        TopicQueue queue = container.getOrCreate(message.topic());
        if (!queue.offer(message)) {
            return Acknowledgement.NACK;
        }
        subscriptionContainer.trigger(queue);
        return Acknowledgement.ACK;
    }
}
