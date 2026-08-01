package org.mmmq.broker.topicqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.persistence.PersistenceProperties;
import org.mmmq.core.message.Topic;
import org.mockito.ArgumentCaptor;

class TopicQueueBootstrapperTest {

    @Test
    @DisplayName("topics 디렉터리에 존재하는 토픽들이 부팅 시 모두 복원된다")
    void restoresAllTopicsOnBoot(@TempDir Path tempDir) throws IOException {
        Files.createDirectories(tempDir.resolve("topics").resolve("topic-a"));
        Files.createDirectories(tempDir.resolve("topics").resolve("topic-b"));
        PersistenceProperties properties = new PersistenceProperties(tempDir.toAbsolutePath().toString(), null);
        TopicQueueFactory factory = new TopicQueueFactory(properties);
        TopicQueueRegistrar registrar = mock(TopicQueueRegistrar.class);
        TopicQueueContainer container = new TopicQueueContainer(factory, registrar);
        TopicQueueBootstrapper bootstrapper = new TopicQueueBootstrapper(properties, container);

        bootstrapper.afterSingletonsInstantiated();

        ArgumentCaptor<TopicQueue> restored = ArgumentCaptor.forClass(TopicQueue.class);
        verify(registrar, times(2)).register(restored.capture());
        assertThat(restored.getAllValues())
                .extracting(TopicQueue::getTopic)
                .containsExactlyInAnyOrder(new Topic("topic-a"), new Topic("topic-b"));
    }

    @Test
    @DisplayName("topics 디렉터리가 없으면 정상 부팅된다")
    void noTopicsDirectoryDoesNotFail(@TempDir Path tempDir) {
        PersistenceProperties properties = new PersistenceProperties(
                tempDir.resolve("nonexistent").toAbsolutePath().toString(),
                null
        );
        TopicQueueFactory factory = new TopicQueueFactory(properties);
        TopicQueueRegistrar registrar = mock(TopicQueueRegistrar.class);
        TopicQueueContainer container = new TopicQueueContainer(factory, registrar);
        TopicQueueBootstrapper bootstrapper = new TopicQueueBootstrapper(properties, container);

        bootstrapper.afterSingletonsInstantiated();

        assertThat(container.getOrCreate(new Topic("anything"))).isNotNull();
    }
}
