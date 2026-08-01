package org.mmmq.broker.topicqueue;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.persistence.PersistenceProperties;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.core.message.Topic;

class TopicQueueFactoryTest {

    @Test
    @DisplayName("openCheckpointDirectory는 create가 만든 토픽 디렉터리 아래 checkpoints를 연다")
    void opensCheckpointDirectoryUnderSameTopicDirectory(@TempDir Path tempDir) {
        PersistenceProperties properties = new PersistenceProperties(tempDir.toString(), null);
        TopicQueueFactory factory = new TopicQueueFactory(properties);
        Topic topic = new Topic("order.created");
        factory.create(topic);

        CheckpointDirectory checkpointDirectory = factory.openCheckpointDirectory(topic);

        assertThat(checkpointDirectory).isNotNull();
        assertThat(tempDir.resolve("topics").resolve("order.created").resolve("checkpoints")).exists();
    }
}
