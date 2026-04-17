package org.mmmq.broker.wal;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.wal.codec.JsonWalCodec;
import org.mmmq.broker.wal.flush.PageCacheFlushPolicy;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class WalWriterTest {

    @Test
    @DisplayName("동일한 세그먼트 인덱스에 대해 여러 엔트리를 라인 단위로 append한다")
    void appendsMultipleEntriesToSameSegment(@TempDir Path tempDir) throws Exception {
        final WalWriter writer = new WalWriter(tempDir, "order", new PageCacheFlushPolicy(), new JsonWalCodec());

        writer.write(new WalEntry(0, new Message(new Topic("order"), Map.of("id", 1))));
        writer.write(new WalEntry(0, new Message(new Topic("order"), Map.of("id", 2))));

        final List<String> lines = Files.readAllLines(tempDir.resolve("order-0.wal"));
        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).contains("\"id\":1");
        assertThat(lines.get(1)).contains("\"id\":2");
    }

    @Test
    @DisplayName("세그먼트 인덱스가 바뀌면 새 파일로 교체하여 기록한다")
    void rotatesToNewSegmentFile(@TempDir Path tempDir) throws Exception {
        final WalWriter writer = new WalWriter(tempDir, "order", new PageCacheFlushPolicy(), new JsonWalCodec());

        writer.write(new WalEntry(0, new Message(new Topic("order"), Map.of("id", 1))));
        writer.write(new WalEntry(1, new Message(new Topic("order"), Map.of("id", 2))));

        assertThat(Files.readAllLines(tempDir.resolve("order-0.wal"))).hasSize(1);
        assertThat(Files.readAllLines(tempDir.resolve("order-1.wal"))).hasSize(1);
    }

    @Test
    @DisplayName("현재 열린 세그먼트 파일도 deleteSegmentFile로 안전하게 삭제할 수 있다")
    void deletesCurrentSegmentFile(@TempDir Path tempDir) throws Exception {
        final WalWriter writer = new WalWriter(tempDir, "order", new PageCacheFlushPolicy(), new JsonWalCodec());
        writer.write(new WalEntry(0, new Message(new Topic("order"), Map.of("id", 1))));

        writer.deleteSegmentFile(0);

        assertThat(Files.exists(tempDir.resolve("order-0.wal"))).isFalse();
    }

    @Test
    @DisplayName("삭제 후에도 동일 세그먼트에 다시 write하면 파일이 재생성된다")
    void writeAfterDeleteRecreatesFile(@TempDir Path tempDir) throws Exception {
        final WalWriter writer = new WalWriter(tempDir, "order", new PageCacheFlushPolicy(), new JsonWalCodec());
        writer.write(new WalEntry(0, new Message(new Topic("order"), Map.of("id", 1))));
        writer.deleteSegmentFile(0);

        writer.write(new WalEntry(0, new Message(new Topic("order"), Map.of("id", 3))));

        assertThat(Files.readAllLines(tempDir.resolve("order-0.wal"))).hasSize(1);
    }
}
