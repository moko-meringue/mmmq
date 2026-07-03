package org.mmmq.broker.topicqueue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.mmmq.broker.persistence.PersistenceProperties;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.broker.topicqueue.storage.StorageException;
import org.mmmq.core.message.Topic;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueFactory {

    private final Path root;
    private final long segmentMaxBytes;

    public TopicQueueFactory(PersistenceProperties properties) {
        root = properties.topicsDir();
        segmentMaxBytes = properties.segment().maxBytes();
    }

    public TopicQueue create(Topic topic) {
        Path topicDir = root.resolve(topic.name());
        try {
            Files.createDirectories(topicDir);
        } catch (IOException exception) {
            throw new StorageException("Failed to create topic directory: " + topicDir, exception);
        }
        SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, segmentMaxBytes);
        CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(topicDir);
        return new TopicQueue(topic, segmentFileChain, checkpointDirectory);
    }
}
