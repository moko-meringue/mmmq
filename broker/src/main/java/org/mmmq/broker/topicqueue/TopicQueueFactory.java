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

/**
 * 토픽 하나가 디스크에서 차지하는 디렉터리 레이아웃을 아는 유일한 자리.
 *
 * <p>{@link TopicQueue}가 필요로 하는 {@link SegmentFileChain}을 열고, {@code subscription} 패키지가
 * 필요로 하는 {@link CheckpointDirectory}도 같은 토픽 디렉터리 아래에서 연다. 경로 조립
 * ({@code root.resolve(topic.name())})을 이 클래스 하나로 모으는 이유는, 그러지 않으면
 * {@code SubscriptionContainer}가 {@link PersistenceProperties}에서 같은 경로를 다시 조립해야 해서
 * 변경 이유가 같은 코드가 두 곳에 생기기 때문이다.
 */
@Component
public class TopicQueueFactory {

    private final Path root;
    private final long segmentMaxBytes;

    public TopicQueueFactory(PersistenceProperties properties) {
        root = properties.topicsDirPath();
        segmentMaxBytes = properties.segment().maxBytes();
    }

    public TopicQueue create(Topic topic) {
        Path topicDir = topicDir(topic);
        try {
            Files.createDirectories(topicDir);
        } catch (IOException exception) {
            throw new StorageException("Failed to create topic directory: " + topicDir, exception);
        }
        SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, segmentMaxBytes);
        return new TopicQueue(topic, segmentFileChain);
    }

    public CheckpointDirectory openCheckpointDirectory(Topic topic) {
        return CheckpointDirectory.open(topicDir(topic));
    }

    private Path topicDir(Topic topic) {
        return root.resolve(topic.name());
    }
}
