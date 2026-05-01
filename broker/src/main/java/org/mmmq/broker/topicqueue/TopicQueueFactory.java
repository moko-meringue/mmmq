package org.mmmq.broker.topicqueue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.mmmq.broker.config.SegmentProperties;
import org.mmmq.broker.config.StorageProperties;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.core.message.Topic;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueFactory {

    private final Path root;
    private final long segmentMaxBytes;

    public TopicQueueFactory(StorageProperties storage, SegmentProperties segment) {
        this.root = Path.of(storage.rootDir());
        this.segmentMaxBytes = segment.maxBytes();
    }

    public TopicQueue create(Topic topic) {
        Path topicDir = root.resolve(topic.name());
        try {
            Files.createDirectories(topicDir);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, segmentMaxBytes);
        CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(topicDir);
        return new TopicQueue(topic, segmentFileChain, checkpointDirectory);
    }
}
