package org.mmmq.broker.topicqueue;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 토픽별 {@link TopicQueue}를 만들고 보관한다. 처음 보는 토픽이면 그 자리에서 만든다.
 *
 * <p>큐를 새로 만들 때마다 {@link TopicQueueRegistrar}에게 알려 구독자가 붙게 한다.
 * 구체 타입이 아니라 인터페이스로 주입받는 이유는 그 구현이 {@code subscription} 패키지에 있어서다 —
 * {@code subscription}은 이미 {@link TopicQueue}를 참조하므로, 여기서 구체 타입을 직접 참조하면
 * 반대 방향 참조가 더해져 두 패키지가 서로를 알게 된다.
 */
@Component
public class TopicQueueContainer {

    private static final Logger log = LoggerFactory.getLogger(TopicQueueContainer.class);

    private final TopicQueueFactory factory;
    private final TopicQueueRegistrar registrar;
    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();

    public TopicQueueContainer(TopicQueueFactory factory, TopicQueueRegistrar registrar) {
        this.factory = factory;
        this.registrar = registrar;
    }

    public TopicQueue getOrCreate(Topic topic) {
        return queues.computeIfAbsent(topic, key -> {
            TopicQueue queue = factory.create(key);
            log.info("Topic queue created: {}", key.name());
            registrar.register(queue);
            return queue;
        });
    }

    @PreDestroy
    public void destroy() {
        queues.values().forEach(queue -> {
            try {
                queue.close();
            } catch (Exception exception) {
                log.error("Failed to close topic queue: {}", queue.getTopic(), exception);
            }
        });
        log.info("All topic queues closed");
    }
}
