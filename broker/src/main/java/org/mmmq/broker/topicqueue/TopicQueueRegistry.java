package org.mmmq.broker.topicqueue;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueRegistry { // 토픽→큐 맵 보유. 새 큐 등록 시 TopicQueueInitializedEvent 발행해 listener가 자체 구독

    private static final Logger log = LoggerFactory.getLogger(TopicQueueRegistry.class);

    private final TopicQueueFactory factory; // 큐 조립을 위임. Registry는 디스크 레이아웃을 모름
    private final ApplicationEventPublisher publisher; // 새 큐 등록 시 이벤트 발행. Dispatcher 등이 listener로 자체 구독
    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>(); // 토픽 → TopicQueue. 동시 접근 안전

    public TopicQueueRegistry(TopicQueueFactory factory, ApplicationEventPublisher publisher) {
        this.factory = factory;
        this.publisher = publisher;
    }

    public TopicQueue get(Topic topic) { // 토픽의 큐를 조회하거나 없으면 생성. FrontDispatcher가 메시지 수신 시마다 호출
        return queues.computeIfAbsent(topic, t -> {
            TopicQueue queue = factory.create(t);
            log.info("Topic queue created: {}", queue.getTopic().name()); // lazy 생성: prior state 없음
            publisher.publishEvent(new TopicQueueInitializedEvent(queue));

            return queue;
        });
    }

    void register(TopicQueue queue) { // Bootstrap이 디스크에서 복원한 큐를 등록할 때 사용
        queues.put(queue.getTopic(), queue);
        log.info("Topic queue registered: {}", queue.getTopic().name()); // 부팅 복원: 미커밋 메시지 가능 (catch-up은 ApplicationReadyEvent에서)
        publisher.publishEvent(new TopicQueueInitializedEvent(queue));
    }

    @PreDestroy
    public void destroy() { // Spring context 종료 시 호출. 모든 TopicQueue를 닫아 fd 누수 방지
        queues.values().forEach(queue -> {
            try {
                queue.close();
            } catch (Exception exception) {
                log.error("Failed to close topic queue: {}", queue.getTopic(), exception);
            }
        });
    }
}
