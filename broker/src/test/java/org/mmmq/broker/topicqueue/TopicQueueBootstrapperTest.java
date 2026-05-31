package org.mmmq.broker.topicqueue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.config.SegmentProperties;
import org.mmmq.broker.config.StorageProperties;
import org.mmmq.broker.dispatcher.DispatcherContainer;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class TopicQueueBootstrapperTest {

    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;

    @Test
    @DisplayName("data 디렉토리에 존재하는 토픽들이 부팅 시 모두 복원된다")
    void restoresAllTopicsOnBoot(@TempDir Path tempDir) {
        seedTopic(tempDir, "topic-a", new Message(new Topic("topic-a"), Map.of("k", "v")));
        seedTopic(tempDir, "topic-b", new Message(new Topic("topic-b"), Map.of("k", "v")));

        StorageProperties storage = new StorageProperties(tempDir.toAbsolutePath().toString());
        SegmentProperties segment = new SegmentProperties(DEFAULT_MAX_BYTES);
        TopicQueueFactory factory = new TopicQueueFactory(storage, segment);
        DispatcherContainer dispatcherContainer = mock(DispatcherContainer.class);
        TopicQueueContainer container = new TopicQueueContainer(factory, dispatcherContainer);
        TopicQueueBootstrapper bootstrapper = new TopicQueueBootstrapper(storage, container);

        bootstrapper.afterSingletonsInstantiated();

        TopicQueue queueA = container.get(new Topic("topic-a"));
        TopicQueue queueB = container.get(new Topic("topic-b"));
        Offset offsetA = queueA.subscribe("dispatcher-1");
        Offset offsetB = queueB.subscribe("dispatcher-1");

        assertThat(queueA.peek(offsetA)).isNotNull();
        assertThat(queueB.peek(offsetB)).isNotNull();
    }

    @Test
    @DisplayName("dispatcher가 마지막 commit 위치에서 재개한다")
    void resumesFromLastCommittedOffset(@TempDir Path tempDir) throws IOException {
        Path topicDir = tempDir.resolve("topic-a");
        Files.createDirectories(topicDir);
        SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, DEFAULT_MAX_BYTES);
        CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(topicDir);
        TopicQueue queue = new TopicQueue(new Topic("topic-a"), segmentFileChain, checkpointDirectory);
        queue.offer(new Message(new Topic("topic-a"), Map.of("seq", 1)));
        queue.offer(new Message(new Topic("topic-a"), Map.of("seq", 2)));
        Offset offset = queue.subscribe("dispatcher-1");
        queue.peek(offset);
        queue.commit("dispatcher-1", offset);

        StorageProperties storage = new StorageProperties(tempDir.toAbsolutePath().toString());
        SegmentProperties segment = new SegmentProperties(DEFAULT_MAX_BYTES);
        TopicQueueFactory factory = new TopicQueueFactory(storage, segment);
        DispatcherContainer dispatcherContainer = mock(DispatcherContainer.class);
        TopicQueueContainer container = new TopicQueueContainer(factory, dispatcherContainer);
        TopicQueueBootstrapper bootstrapper = new TopicQueueBootstrapper(storage, container);
        bootstrapper.afterSingletonsInstantiated();

        TopicQueue restored = container.get(new Topic("topic-a"));
        Offset restoredOffset = restored.subscribe("dispatcher-1");

        assertThat(restoredOffset.value()).isEqualTo(1L);
    }

    @Test
    @DisplayName("data 디렉토리가 없으면 정상 부팅된다")
    void noDataDirectoryDoesNotFail(@TempDir Path tempDir) {
        StorageProperties storage = new StorageProperties(
                tempDir.resolve("nonexistent").toAbsolutePath().toString()
        );
        SegmentProperties segment = new SegmentProperties(DEFAULT_MAX_BYTES);
        TopicQueueFactory factory = new TopicQueueFactory(storage, segment);
        DispatcherContainer dispatcherContainer = mock(DispatcherContainer.class);
        TopicQueueContainer container = new TopicQueueContainer(factory, dispatcherContainer);
        TopicQueueBootstrapper bootstrapper = new TopicQueueBootstrapper(storage, container);

        bootstrapper.afterSingletonsInstantiated();

        assertThat(container.get(new Topic("anything"))).isNotNull();
    }

    private void seedTopic(Path baseDir, String topicName, Message message) {
        Path topicDir = baseDir.resolve(topicName);
        try {
            Files.createDirectories(topicDir);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        try (SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, DEFAULT_MAX_BYTES)) {
            segmentFileChain.append(message);
        }
    }
}
