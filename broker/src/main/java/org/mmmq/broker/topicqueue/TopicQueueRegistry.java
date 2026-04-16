package org.mmmq.broker.topicqueue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.wal.WalWriter;
import org.mmmq.broker.wal.flush.WalFlushPolicy;
import org.mmmq.broker.wal.flush.WalFlushPolicyFactory;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueRegistry {

    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();
    private final ObjectProvider<Dispatcher> dispatcherProvider;
    private final Path walDir;
    private final WalFlushPolicy flushPolicy;

    public TopicQueueRegistry(
            ObjectProvider<Dispatcher> dispatcherProvider,
            @Value("${mmmq.broker.wal.dir:./wal}") String walDir,
            @Value("${mmmq.broker.wal.flush-policy:page_cache}") String flushPolicy
    ) {
        this.dispatcherProvider = dispatcherProvider;
        this.walDir = Paths.get(walDir);
        this.flushPolicy = WalFlushPolicyFactory.create(flushPolicy);
        ensureWalDir();
    }

    public TopicQueue get(Topic topic) {
        return queues.computeIfAbsent(topic, this::create);
    }

    public Path walDir() {
        return walDir;
    }

    private TopicQueue create(Topic topic) {
        final WalWriter walWriter = new WalWriter(walDir, topic.name(), flushPolicy);
        final TopicQueue topicQueue = new TopicQueue(topic, walWriter);
        dispatcherProvider.stream()
                .forEach(dispatcher -> dispatcher.subscribe(topicQueue));

        return topicQueue;
    }

    private void ensureWalDir() {
        try {
            Files.createDirectories(walDir);
        } catch (IOException exception) {
            throw new RuntimeException("Failed to create WAL directory: " + walDir, exception);
        }
    }
}
