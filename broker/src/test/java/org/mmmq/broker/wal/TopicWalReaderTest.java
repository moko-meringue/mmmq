package org.mmmq.broker.wal;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.wal.codec.JsonWalCodec;

class TopicWalReaderTest {

    private final TopicWalReader topicWalReader = new TopicWalReader(new WalReader(new JsonWalCodec()));

    @Test
    @DisplayName("해당 토픽의 WAL 파일을 segmentIndex 오름차순으로 읽는다")
    void readsFilesInSegmentOrder(@TempDir Path walDir) throws Exception {
        Files.writeString(
                walDir.resolve("order-1.wal"),
                "{\"segmentIndex\":1,\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":2}}}\n"
        );
        Files.writeString(
                walDir.resolve("order-0.wal"),
                "{\"segmentIndex\":0,\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":1}}}\n"
        );

        final List<WalEntry> entries = topicWalReader.stream(walDir, "order").toList();

        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).segmentIndex()).isEqualTo(0);
        assertThat(entries.get(1).segmentIndex()).isEqualTo(1);
    }

    @Test
    @DisplayName("다른 토픽의 WAL 파일은 포함하지 않는다")
    void filtersOutOtherTopics(@TempDir Path walDir) throws Exception {
        Files.writeString(
                walDir.resolve("order-0.wal"),
                "{\"segmentIndex\":0,\"message\":{\"topic\":{\"name\":\"order\"},\"content\":{\"id\":1}}}\n"
        );
        Files.writeString(
                walDir.resolve("payment-0.wal"),
                "{\"segmentIndex\":0,\"message\":{\"topic\":{\"name\":\"payment\"},\"content\":{\"id\":99}}}\n"
        );

        final List<WalEntry> entries = topicWalReader.stream(walDir, "order").toList();

        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).message().topic().name()).isEqualTo("order");
    }

    @Test
    @DisplayName("walDir이 존재하지 않으면 빈 스트림을 반환한다")
    void returnsEmptyWhenDirMissing(@TempDir Path tempDir) {
        final Path nonExistent = tempDir.resolve("no-such-dir");

        final List<WalEntry> entries = topicWalReader.stream(nonExistent, "order").toList();

        assertThat(entries).isEmpty();
    }

    @Test
    @DisplayName("WAL 디렉터리가 비어있으면 빈 스트림을 반환한다")
    void returnsEmptyWhenDirEmpty(@TempDir Path walDir) {
        final List<WalEntry> entries = topicWalReader.stream(walDir, "order").toList();

        assertThat(entries).isEmpty();
    }

    @Test
    @DisplayName("topicNames는 WAL 파일에서 중복 없이 토픽 이름을 반환한다")
    void returnsDistinctTopicNames(@TempDir Path walDir) throws Exception {
        Files.writeString(walDir.resolve("order-0.wal"), "");
        Files.writeString(walDir.resolve("order-1.wal"), "");
        Files.writeString(walDir.resolve("payment-0.wal"), "");
        Files.writeString(walDir.resolve("README.md"), "");

        final List<String> topicNames;
        try (Stream<String> stream = topicWalReader.topicNames(walDir)) {
            topicNames = stream.toList();
        }

        assertThat(topicNames).containsExactlyInAnyOrder("order", "payment");
    }
}
