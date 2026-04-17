package org.mmmq.broker.wal;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueRegistry;
import org.mmmq.broker.wal.codec.WalCodec;
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

    private final TopicWalReader topicWalReader;

    public WalRecovery(
            TopicQueueRegistry registry,
            ApplicationEventPublisher publisher,
            WalCodec walCodec
    ) {
        this.registry = registry;
        this.publisher = publisher;
        this.topicWalReader = new TopicWalReader(new WalReader(walCodec));
    }

    @Override
    public void afterSingletonsInstantiated() {
        Path walDir = registry.walDir();
        if (!Files.isDirectory(walDir)) {
            return;
        }
        try (Stream<String> topicNames = topicWalReader.topicNames(walDir)) {
            topicNames.forEach(topicName -> recoverTopic(walDir, topicName));
        }
    }

    private void recoverTopic(Path walDir, String topicName) {
        TopicQueue topicQueue = registry.get(new Topic(topicName));
        AtomicLong replayed = new AtomicLong();
        try (Stream<WalEntry> entries = topicWalReader.stream(walDir, topicName)) {
            entries.forEach(entry -> {
                topicQueue.restore(entry.message());
                replayed.incrementAndGet();
            });
        }
        if (replayed.get() > 0) {
            log.info("Recovered {} messages for topic '{}' from WAL", replayed, topicName);
            publisher.publishEvent(new TopicQueueRecoveredEvent(topicQueue));
        }
    }
}
