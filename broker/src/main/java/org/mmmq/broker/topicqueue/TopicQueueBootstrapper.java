package org.mmmq.broker.topicqueue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.mmmq.broker.config.StorageProperties;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueBootstrapper implements SmartInitializingSingleton {

    private final Path root;
    private final TopicQueueFactory factory;
    private final TopicQueueRegistry registry;

    public TopicQueueBootstrapper(StorageProperties storage, TopicQueueFactory factory, TopicQueueRegistry registry) {
        this.root = Path.of(storage.rootDir());
        this.factory = factory;
        this.registry = registry;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (!Files.isDirectory(root)) {
            return;
        }
        try (Stream<Path> entries = Files.list(root)) {
            entries.filter(Files::isDirectory)
                    .map(path -> new Topic(path.getFileName().toString()))
                    .map(factory::create)
                    .forEach(registry::register);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to scan data directory: " + root, exception);
        }
    }
}
