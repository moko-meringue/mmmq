package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OffsetIndexFileTest {

    @Test
    @DisplayName("entryCount는 파일 size를 8로 나눈 값과 같다")
    void countMatchesByteSize(@TempDir Path tempDir) {
        try (OffsetIndexFile index = OffsetIndexFile.open(tempDir, 0L)) {
            index.append(0L);
            index.append(100L);
            index.append(200L);

            assertThat(index.count()).isEqualTo(3L);
        }
    }

    @Test
    @DisplayName("readAddressAt 결과는 append 한 값과 동일하다")
    void readAddressRoundTrip(@TempDir Path tempDir) {
        try (OffsetIndexFile index = OffsetIndexFile.open(tempDir, 0L)) {
            index.append(0L);
            index.append(100L);
            index.append(200L);

            assertThat(index.readAt(0)).isEqualTo(0L);
            assertThat(index.readAt(1)).isEqualTo(100L);
            assertThat(index.readAt(2)).isEqualTo(200L);
        }
    }

    @Test
    @DisplayName("새로 만든 index는 entryCount가 0이다")
    void newIndexHasZeroEntries(@TempDir Path tempDir) {
        try (OffsetIndexFile index = OffsetIndexFile.open(tempDir, 0L)) {
            assertThat(index.count()).isZero();
        }
    }
}
