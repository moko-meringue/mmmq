package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentTest {

    @Test
    @DisplayName("append 후 반환 position은 prior size와 같다")
    void appendReturnsPriorSize(@TempDir Path tempDir) {
        try (Segment segment = Segment.openOrCreate(tempDir.resolve("test.mmm"))) {
            final long firstPosition = segment.appendAndForce("hello".getBytes()); // 첫 쓰기: 파일이 비어있으므로 position=0
            final long secondPosition = segment.appendAndForce("world".getBytes()); // 두 번째 쓰기: "hello"(5 bytes) 다음이므로 position=5

            assertThat(firstPosition).isEqualTo(0L); // 첫 엔트리는 항상 파일 시작에 위치
            assertThat(secondPosition).isEqualTo(5L); // "hello"의 길이(5)가 두 번째 엔트리 시작 위치
        }
    }

    @Test
    @DisplayName("append 후 readAt(position, length)으로 정확히 length bytes를 읽을 수 있다")
    void readAtReturnsAppendedBytes(@TempDir Path tempDir) {
        try (Segment segment = Segment.openOrCreate(tempDir.resolve("test.mmm"))) {
            final byte[] payload = "hello".getBytes();
            final long position = segment.appendAndForce(payload); // 쓰기 완료 후 반환된 position

            final byte[] read = segment.readAt(position, payload.length); // 저장된 position에서 정확한 길이만큼 읽기

            assertThat(read).isEqualTo(payload); // 쓴 내용과 읽은 내용이 byte 단위로 일치해야 함
        }
    }

    @Test
    @DisplayName("두 번째 entry는 별도 position에서 읽을 수 있다")
    void readSecondEntryFromCorrectPosition(@TempDir Path tempDir) {
        try (Segment segment = Segment.openOrCreate(tempDir.resolve("test.mmm"))) {
            segment.appendAndForce("line1".getBytes()); // 첫 번째 엔트리 (position=0, length=5)
            final long secondPos = segment.appendAndForce("line2".getBytes()); // 두 번째 엔트리 시작 위치

            final byte[] read = segment.readAt(secondPos, 5); // 두 번째 엔트리만 정확히 읽어야 함

            assertThat(new String(read)).isEqualTo("line2"); // 첫 번째 엔트리와 혼동 없이 두 번째만 반환
        }
    }

    @Test
    @DisplayName("범위를 벗어나는 readAt은 StorageException을 던진다")
    void readAtOutOfBoundsThrows(@TempDir Path tempDir) {
        try (Segment segment = Segment.openOrCreate(tempDir.resolve("test.mmm"))) {
            segment.appendAndForce("hello".getBytes()); // 5 bytes

            assertThatThrownBy(() -> segment.readAt(0L, 6)) // 파일 끝을 넘어가는 read
                    .isInstanceOf(StorageException.class);
            assertThatThrownBy(() -> segment.readAt(-1L, 1)) // 음수 position
                    .isInstanceOf(StorageException.class);
        }
    }

    @Test
    @DisplayName("truncate는 파일 size를 줄인다")
    void truncateReducesSize(@TempDir Path tempDir) {
        try (Segment segment = Segment.openOrCreate(tempDir.resolve("test.mmm"))) {
            segment.appendAndForce("hello".getBytes()); // 5 bytes 추가
            segment.appendAndForce("world".getBytes()); // 5 bytes 추가 → 총 10 bytes

            segment.truncate(5L); // 첫 번째 엔트리만 남기고 나머지 제거

            assertThat(segment.size()).isEqualTo(5L); // truncate 후 파일 크기가 지정한 값이 되어야 함
        }
    }
}
