package org.mmmq.broker.wal.file;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mmmq.broker.topicqueue.MessagePersistence;
import org.mmmq.broker.wal.codec.JsonWalCodec;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class MessagePersistenceTest {

    private MessagePersistence createMessagePersistence(Path tempDir, String topicName) {
        WalFileStore directory = new WalFileStore(tempDir.toString());
        WalMessagePersistenceFactory factory = new WalMessagePersistenceFactory(
                new JsonWalCodec(), directory, "page_cache"
        );
        return factory.create(topicName);
    }

    @Test
    @DisplayName("동일한 세그먼트 인덱스에 대해 여러 엔트리를 라인 단위로 append한다")
    void appendsMultipleEntriesToSameSegment(@TempDir Path tempDir) throws Exception {
        final MessagePersistence topicWal = createMessagePersistence(tempDir, "order");

        topicWal.persist(new Message(new Topic("order"), Map.of("id", 1)), 0);
        topicWal.persist(new Message(new Topic("order"), Map.of("id", 2)), 0);

        final List<String> lines = Files.readAllLines(tempDir.resolve("order-0.wal"));
        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).contains("\"id\":1");
        assertThat(lines.get(1)).contains("\"id\":2");
    }

    @Test
    @DisplayName("세그먼트 인덱스가 바뀌면 새 파일로 교체하여 기록한다")
    void rotatesToNewWalFile(@TempDir Path tempDir) throws Exception {
        final MessagePersistence topicWal = createMessagePersistence(tempDir, "order");

        topicWal.persist(new Message(new Topic("order"), Map.of("id", 1)), 0);
        topicWal.persist(new Message(new Topic("order"), Map.of("id", 2)), 1);

        assertThat(Files.readAllLines(tempDir.resolve("order-0.wal"))).hasSize(1);
        assertThat(Files.readAllLines(tempDir.resolve("order-1.wal"))).hasSize(1);
    }

    @Test
    @DisplayName("deleteSegment()는 현재 열린 채널을 닫고 파일을 삭제한다")
    void deletesCurrentSegment(@TempDir Path tempDir) throws Exception {
        final MessagePersistence topicWal = createMessagePersistence(tempDir, "order");
        topicWal.persist(new Message(new Topic("order"), Map.of("id", 1)), 0);

        topicWal.evict(0);

        assertThat(Files.exists(tempDir.resolve("order-0.wal"))).isFalse();
    }

    @Test
    @DisplayName("deleteSegment()는 현재 열린 채널이 아닌 세그먼트에 대해 현재 채널에 영향을 주지 않는다")
    void deleteSegmentDoesNotAffectCurrentChannel(@TempDir Path tempDir) throws Exception {
        final MessagePersistence topicWal = createMessagePersistence(tempDir, "order");
        topicWal.persist(new Message(new Topic("order"), Map.of("id", 1)), 0);

        topicWal.evict(1);

        topicWal.persist(new Message(new Topic("order"), Map.of("id", 2)), 0);
        final List<String> lines = Files.readAllLines(tempDir.resolve("order-0.wal"));
        assertThat(lines).hasSize(2);
    }

    @Test
    @DisplayName("채널 교체 시 close()가 실패해도 새 채널에 정상적으로 기록한다")
    void continuesWithNewChannelWhenCloseFails(@TempDir Path tempDir) {
        MessagePersistence persistence = createMessagePersistence(tempDir, "order");

        try (MockedConstruction<WalFileChannel> mocked = mockConstruction(WalFileChannel.class,
                (mock, context) -> {
                    WalFile walFile = (WalFile) context.arguments().get(0);
                    when(mock.getWalFileIndex()).thenReturn(walFile.index());
                    if (walFile.index() == 0) {
                        doThrow(new RuntimeException("simulated close failure")).when(mock).close();
                    }
                })) {
            persistence.persist(new Message(new Topic("order"), Map.of("id", 1)), 0);

            assertThatNoException().isThrownBy(() ->
                    persistence.persist(new Message(new Topic("order"), Map.of("id", 2)), 1)
            );
            verify(mocked.constructed().get(1)).write(any());
        }
    }
}
