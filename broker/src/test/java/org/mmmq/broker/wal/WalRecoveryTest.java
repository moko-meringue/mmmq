package org.mmmq.broker.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.fixture.NoOpTopicWal;
import org.mmmq.broker.topicqueue.MessageRestorer;
import org.mmmq.broker.topicqueue.Offset;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueReadyEvent;
import org.mmmq.broker.topicqueue.TopicQueueRestorer;
import org.mmmq.broker.wal.codec.JsonWalCodec;
import org.mmmq.broker.wal.file.WalFileStore;
import org.mmmq.core.message.Topic;
import org.springframework.context.ApplicationEventPublisher;

class WalRecoveryTest {

    private WalFileStore createWalStore(Path walDir) {
        return new WalFileStore(walDir.toString());
    }

    private WalRecovery createWalRecovery(
            MessageRestorer restorer,
            WalFileStore walDirectory,
            ApplicationEventPublisher publisher
    ) {
        return new WalRecovery(new JsonWalCodec(), walDirectory, restorer, publisher);
    }

    @Test
    @DisplayName("WAL 파일에서 토픽별로 세그먼트 순서대로 복구하고 이벤트를 발행한다")
    void recoversTopicsInSegmentOrder(@TempDir Path walDir) throws Exception {
        Files.writeString(
                walDir.resolve("order-0.wal"),
                "{\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":1}}}\n"
                        + "{\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":2}}}\n"
                        + "{\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":3}}}\n"
        );

        final WalFileStore walDirectory = createWalStore(walDir);
        final TopicQueueRestorer restorer = new TopicQueueRestorer(
                new TopicQueue(new Topic("order"), new NoOpTopicWal())
        );
        final ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

        createWalRecovery(restorer, walDirectory, publisher).afterSingletonsInstantiated();

        final TopicQueue topicQueue = restorer.getTopicQueue();
        final Offset offset = topicQueue.getOffsetAtHead();
        assertThat(((Map<?, ?>) topicQueue.poll(offset).content()).get("id")).isEqualTo(1);
        assertThat(((Map<?, ?>) topicQueue.poll(offset).content()).get("id")).isEqualTo(2);
        assertThat(((Map<?, ?>) topicQueue.poll(offset).content()).get("id")).isEqualTo(3);
        verify(publisher, times(1)).publishEvent(any(TopicQueueReadyEvent.class));
    }

    @Test
    @DisplayName("WAL 디렉터리가 비어있으면 이벤트를 발행한다")
    void emitsEventEvenWhenDirEmpty(@TempDir Path walDir) {
        final WalFileStore walDirectory = createWalStore(walDir);
        final MessageRestorer restorer = mock(MessageRestorer.class);
        final ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

        createWalRecovery(restorer, walDirectory, publisher).afterSingletonsInstantiated();

        verify(publisher, times(1)).publishEvent(any(TopicQueueReadyEvent.class));
    }

    @Test
    @DisplayName("WAL 파일 네이밍 규칙에 맞지 않는 파일은 무시한다")
    void ignoresNonWalFiles(@TempDir Path walDir) throws Exception {
        Files.writeString(walDir.resolve("README.md"), "hello");
        Files.writeString(walDir.resolve("order-abc.wal"), "garbage");

        final WalFileStore walDirectory = createWalStore(walDir);
        final MessageRestorer restorer = mock(MessageRestorer.class);
        final ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

        createWalRecovery(restorer, walDirectory, publisher).afterSingletonsInstantiated();

        verify(publisher, times(1)).publishEvent(any(TopicQueueReadyEvent.class));
    }
}
