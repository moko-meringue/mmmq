package org.mmmq.broker.wal;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.wal.codec.JsonWalCodec;

class WalDirectoryTest {

    private WalDirectory createWalDirectory(Path walDir) {
        return new WalDirectory(new JsonWalCodec(), walDir.toString(), "page_cache");
    }

    @Test
    @DisplayName("topicNames()는 WAL 파일에서 중복 없이 토픽 이름을 반환한다")
    void returnsDistinctTopicNames(@TempDir Path tempDir) throws Exception {
        Files.writeString(tempDir.resolve("order-0.wal"), "");
        Files.writeString(tempDir.resolve("order-1.wal"), "");
        Files.writeString(tempDir.resolve("payment-0.wal"), "");
        Files.writeString(tempDir.resolve("README.md"), "");
        final WalDirectory walDirectory = createWalDirectory(tempDir);

        final List<String> topicNames = walDirectory.topicNames();

        assertThat(topicNames).containsExactlyInAnyOrder("order", "payment");
    }

    @Test
    @DisplayName("segmentFiles()는 segmentIndex 오름차순으로 정렬한다")
    void returnsWalFilesInAscendingOrder(@TempDir Path tempDir) throws Exception {
        Files.writeString(tempDir.resolve("order-2.wal"), "");
        Files.writeString(tempDir.resolve("order-0.wal"), "");
        Files.writeString(tempDir.resolve("order-1.wal"), "");
        final WalDirectory walDirectory = createWalDirectory(tempDir);

        final List<WalFile> segmentFiles = walDirectory.segmentFiles("order");

        assertThat(segmentFiles).hasSize(3);
        assertThat(segmentFiles.get(0).index()).isEqualTo(0);
        assertThat(segmentFiles.get(1).index()).isEqualTo(1);
        assertThat(segmentFiles.get(2).index()).isEqualTo(2);
    }

    @Test
    @DisplayName("segmentFiles()는 다른 토픽 파일을 제외한다")
    void filtersOutOtherTopics(@TempDir Path tempDir) throws Exception {
        Files.writeString(tempDir.resolve("order-0.wal"), "");
        Files.writeString(tempDir.resolve("payment-0.wal"), "");
        final WalDirectory walDirectory = createWalDirectory(tempDir);

        final List<WalFile> segmentFiles = walDirectory.segmentFiles("order");

        assertThat(segmentFiles).hasSize(1);
        assertThat(segmentFiles.get(0).topicName()).isEqualTo("order");
    }

    @Test
    @DisplayName("디렉터리가 비어있으면 topicNames()는 빈 목록을 반환한다")
    void returnsEmptyWhenDirectoryIsEmpty(@TempDir Path tempDir) {
        final WalDirectory walDirectory = createWalDirectory(tempDir);

        final List<String> topicNames = walDirectory.topicNames();

        assertThat(topicNames).isEmpty();
    }
}
