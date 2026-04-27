package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentTest {

    @Test
    @DisplayName("append 후 반환 position은 prior size와 같다")
    void appendReturnsPriorSize(@TempDir Path tempDir) {
        try (Segment segmentFile = Segment.openOrCreate(tempDir.resolve("test.mmm"))) {
            final long firstPosition = segmentFile.appendAndForce("hello\n".getBytes()); // 첫 쓰기: 파일이 비어있으므로 position=0
            final long secondPosition = segmentFile.appendAndForce("world\n".getBytes()); // 두 번째 쓰기: "hello\n"(6 bytes) 다음이므로 position=6

            assertThat(firstPosition).isEqualTo(0L); // 첫 엔트리는 항상 파일 시작에 위치
            assertThat(secondPosition).isEqualTo(6L); // "hello\n"의 길이(6)가 두 번째 엔트리 시작 위치
        }
    }

    @Test
    @DisplayName("append 후 readAt으로 동일한 line을 읽을 수 있다")
    void readAtReturnsAppendedLine(@TempDir Path tempDir) {
        try (Segment segmentFile = Segment.openOrCreate(tempDir.resolve("test.mmm"))) {
            final byte[] payload = "hello\n".getBytes();
            final long position = segmentFile.appendAndForce(payload); // 쓰기 완료 후 반환된 position

            final byte[] read = segmentFile.readAt(position); // 저장된 position으로 읽기

            assertThat(read).isEqualTo(payload); // 쓴 내용과 읽은 내용이 byte 단위로 일치해야 함
        }
    }

    @Test
    @DisplayName("두 번째 line은 별도 position에서 읽을 수 있다")
    void readSecondLineFromCorrectPosition(@TempDir Path tempDir) {
        try (Segment segmentFile = Segment.openOrCreate(tempDir.resolve("test.mmm"))) {
            segmentFile.appendAndForce("line1\n".getBytes()); // 첫 번째 엔트리 (position=0, length=6)
            final long secondPos = segmentFile.appendAndForce("line2\n".getBytes()); // 두 번째 엔트리 시작 위치

            final byte[] read = segmentFile.readAt(secondPos); // 두 번째 엔트리만 정확히 읽어야 함

            assertThat(new String(read)).isEqualTo("line2\n"); // 첫 번째 엔트리와 혼동 없이 두 번째만 반환
        }
    }

    @Test
    @DisplayName("truncate는 파일 size를 줄인다")
    void truncateReducesSize(@TempDir Path tempDir) {
        try (Segment segmentFile = Segment.openOrCreate(tempDir.resolve("test.mmm"))) {
            segmentFile.appendAndForce("hello\n".getBytes()); // 6 bytes 추가
            segmentFile.appendAndForce("world\n".getBytes()); // 6 bytes 추가 → 총 12 bytes

            segmentFile.truncate(6L); // 첫 번째 엔트리만 남기고 나머지 제거

            assertThat(segmentFile.size()).isEqualTo(6L); // truncate 후 파일 크기가 지정한 값이 되어야 함
        }
    }
}
