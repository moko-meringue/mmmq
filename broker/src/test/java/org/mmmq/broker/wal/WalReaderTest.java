package org.mmmq.broker.wal;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.wal.codec.JsonWalCodec;

class WalReaderTest {

    private final WalReader reader = new WalReader(new JsonWalCodec());

    @Test
    @DisplayName("파일이 존재하지 않으면 빈 리스트를 반환한다")
    void returnsEmptyWhenFileMissing(@TempDir Path tempDir) {
        final List<WalEntry> entries = reader.stream(tempDir.resolve("missing.wal")).toList();

        assertThat(entries).isEmpty();
    }

    @Test
    @DisplayName("파일의 각 라인을 WalEntry로 역직렬화한다")
    void readsEachLineAsEntry(@TempDir Path tempDir) throws Exception {
        final Path file = tempDir.resolve("order-0.wal");
        Files.writeString(
                file,
                "{\"segmentIndex\":0,\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":1}}}\n"
                        + "{\"segmentIndex\":0,\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":2}}}\n"
        );

        final List<WalEntry> entries = reader.stream(file).toList();

        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).message().topic().name()).isEqualTo("order");
        assertThat(entries.get(1).message().topic().name()).isEqualTo("order");
    }

    @Test
    @DisplayName("빈 줄은 건너뛰고 파싱할 수 없는 줄은 무시한다")
    void skipsBlankAndCorruptLines(@TempDir Path tempDir) throws Exception {
        final Path file = tempDir.resolve("order-0.wal");
        Files.writeString(
                file,
                "{\"segmentIndex\":0,\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":1}}}\n"
                        + "\n"
                        + "this-is-not-json\n"
                        + "{\"segmentIndex\":0,\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":3}}}\n"
        );

        final List<WalEntry> entries = reader.stream(file).toList();

        assertThat(entries).hasSize(2);
    }
}
