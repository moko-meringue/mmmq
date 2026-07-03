package org.mmmq.broker.topicqueue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.mmmq.broker.persistence.PersistenceProperties;
import org.mmmq.broker.topicqueue.storage.StorageException;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueBootstrapper implements SmartInitializingSingleton {

    private final Path root;
    private final TopicQueueContainer container;

    public TopicQueueBootstrapper(PersistenceProperties properties, TopicQueueContainer container) {
        root = properties.topicsDir();
        this.container = container;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (!Files.isDirectory(root)) {
            return;
        }
        try (Stream<Path> entries = Files.list(root)) {
            entries.filter(Files::isDirectory)
                    .map(path -> new Topic(path.getFileName().toString()))
                    .forEach(container::getOrCreate);
        } catch (IOException exception) {
            throw new StorageException("Failed to scan topics directory: " + root, exception);
        }
    }
}
