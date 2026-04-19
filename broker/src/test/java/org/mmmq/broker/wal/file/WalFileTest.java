package org.mmmq.broker.wal.file;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.wal.WalEntry;
import org.mmmq.broker.wal.codec.JsonWalCodec;

class WalFileTest {

    @Test
    @DisplayName("파일이 없으면 read()는 빈 스트림을 반환한다")
    void returnsEmptyStreamWhenFileMissing(@TempDir Path tempDir) {
        final WalFile segmentFile = WalFile.of(tempDir, "order", 0);

        final List<WalEntry> entries = segmentFile.read(new JsonWalCodec()).toList();

        assertThat(entries).isEmpty();
    }

    @Test
    @DisplayName("파일의 각 줄을 WalEntry로 역직렬화한다")
    void readsEachLineAsEntry(@TempDir Path tempDir) throws Exception {
        final Path file = tempDir.resolve("order-0.wal");
        Files.writeString(
                file,
                "{\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":1}}}\n"
                        + "{\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":2}}}\n"
        );
        final WalFile segmentFile = WalFile.of(tempDir, "order", 0);

        final List<WalEntry> entries = segmentFile.read(new JsonWalCodec()).toList();

        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).message().topic().name()).isEqualTo("order");
        assertThat(entries.get(1).message().topic().name()).isEqualTo("order");
    }

    @Test
    @DisplayName("파싱할 수 없는 줄은 스킵한다")
    void skipsCorruptLines(@TempDir Path tempDir) throws Exception {
        final Path file = tempDir.resolve("order-0.wal");
        Files.writeString(
                file,
                "{\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":1}}}\n"
                        + "\n"
                        + "this-is-not-json\n"
                        + "{\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":3}}}\n"
        );
        final WalFile segmentFile = WalFile.of(tempDir, "order", 0);

        final List<WalEntry> entries = segmentFile.read(new JsonWalCodec()).toList();

        assertThat(entries).hasSize(2);
    }

}
