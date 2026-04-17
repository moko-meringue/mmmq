package org.mmmq.broker.topicqueue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.wal.WalAppender;
import org.mmmq.broker.wal.WalAppenderFactory;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueRegistry {

    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();
    private final ObjectProvider<Dispatcher> dispatcherProvider;
    private final Path walDir;

    private final WalAppenderFactory walAppenderFactory;

    public TopicQueueRegistry(
            ObjectProvider<Dispatcher> dispatcherProvider,
            @Value("${mmmq.broker.wal.dir:./wal}") String walDir,
            WalAppenderFactory walAppenderFactory
    ) {
        this.dispatcherProvider = dispatcherProvider;
        this.walDir = Paths.get(walDir);
        this.walAppenderFactory = walAppenderFactory;
        ensureWalDir();
    }

    public TopicQueue get(Topic topic) {
        return queues.computeIfAbsent(topic, this::create);
    }

    public Path walDir() {
        return walDir;
    }

    private TopicQueue create(Topic topic) {
        WalAppender walAppender = walAppenderFactory.create(topic.name());
        TopicQueue topicQueue = new TopicQueue(topic, walAppender);
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
