package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OffsetIndexTest {

    @Test
    @DisplayName("entryCount는 파일 size를 8로 나눈 값과 같다")
    void countMatchesByteSize(@TempDir Path tempDir) {
        try (OffsetIndex index = OffsetIndex.open(tempDir, "test")) {
            index.append(0L);   // 8 bytes
            index.append(100L); // 16 bytes
            index.append(200L); // 24 bytes

            assertThat(index.count()).isEqualTo(3L); // 24 bytes / 8 = 3 엔트리
        }
    }

    @Test
    @DisplayName("readAddressAt 결과는 append 한 값과 동일하다")
    void readAddressRoundTrip(@TempDir Path tempDir) {
        try (OffsetIndex index = OffsetIndex.open(tempDir, "test")) {
            index.append(0L);   // relativeOffset=0에 0 저장
            index.append(100L); // relativeOffset=1에 100 저장
            index.append(200L); // relativeOffset=2에 200 저장

            assertThat(index.readAt(0)).isEqualTo(0L);   // 첫 엔트리의 .mmm 주소
            assertThat(index.readAt(1)).isEqualTo(100L); // 두 번째 엔트리의 .mmm 주소
            assertThat(index.readAt(2)).isEqualTo(200L); // 세 번째 엔트리의 .mmm 주소
        }
    }

    @Test
    @DisplayName("새로 만든 index는 entryCount가 0이다")
    void newIndexHasZeroEntries(@TempDir Path tempDir) {
        try (OffsetIndex index = OffsetIndex.open(tempDir, "test")) {
            assertThat(index.count()).isZero(); // 빈 파일: size=0, size/8=0
        }
    }
}
