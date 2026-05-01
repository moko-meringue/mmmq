package org.mmmq.broker.dispatcher;

import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueRegistry;
import org.mmmq.core.message.Message;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class FrontDispatcher { // 수신된 메시지를 TopicQueue에 저장하고 Dispatcher들에게 도착 이벤트를 전파하는 진입점

    final TopicQueueRegistry registry; // 토픽 → TopicQueue 조회. 토픽이 없으면 새로 생성
    private final ApplicationEventPublisher publisher; // MessageArrivedEvent를 Spring 이벤트 버스로 발행

    public FrontDispatcher(TopicQueueRegistry registry, ApplicationEventPublisher publisher) {
        this.registry = registry;
        this.publisher = publisher;
    }

    public boolean dispatch(Message message) { // 메시지를 디스크에 저장하고 도착 이벤트 발행. 저장 성공 시 true(→ ACK), 실패 시 false(→ NACK)
        final TopicQueue queue = registry.get(message.topic()); // 토픽 큐 조회 또는 생성. dispatcher 없어도 큐는 생성해 디스크에 저장
        final boolean persisted = queue.offer(message); // .mmm + .idx fsync 완료 후 true 반환
        if (persisted) { // 디스크 저장이 완료된 경우에만 이벤트 발행. NACK 시에는 Dispatcher에 알리지 않음
            publisher.publishEvent(new MessageArrivedEvent(queue)); // 해당 토픽을 구독하는 Dispatcher의 drain을 트리거
        }

        return persisted; // Broker가 이 값으로 ACK/NACK를 결정
    }
}
