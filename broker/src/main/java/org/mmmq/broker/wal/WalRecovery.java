package org.mmmq.broker.wal;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueRegistry;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class WalRecovery implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(WalRecovery.class);

    private final TopicQueueRegistry registry;
    private final ApplicationEventPublisher publisher;
    private final WalDirectory walDirectory;

    public WalRecovery(
            TopicQueueRegistry registry,
            ApplicationEventPublisher publisher,
            WalDirectory walDirectory
    ) {
        this.registry = registry;
        this.publisher = publisher;
        this.walDirectory = walDirectory;
    }

    @Override
    public void afterSingletonsInstantiated() {
        Map<String, List<WalFile>> segmentsByTopic = walDirectory.segmentFiles().stream()
                .collect(Collectors.groupingBy(WalFile::topicName));
        for (Map.Entry<String, List<WalFile>> entry : segmentsByTopic.entrySet()) {
            recoverTopic(entry.getKey(), entry.getValue());
        }
    }

    private void recoverTopic(String topicName, List<WalFile> segmentFiles) {
        TopicQueue topicQueue = registry.get(new Topic(topicName));
        long replayed = 0;
        for (WalFile segmentFile : segmentFiles) {
            try (Stream<WalEntry> entries = walDirectory.read(segmentFile)) {
                for (WalEntry entry : (Iterable<WalEntry>) entries::iterator) {
                    topicQueue.restore(entry.message());
                    replayed++;
                }
            }
        }
        if (replayed > 0) {
            log.info("Recovered {} messages for topic '{}' from WAL", replayed, topicName);
            publisher.publishEvent(new TopicQueueRecoveredEvent(topicQueue));
        }
    }
}
