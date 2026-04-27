package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class SegmentDirectoryTest {

    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024; // 일반 테스트에서 회전이 발생하지 않도록 크게 설정

    @Test
    @DisplayName("빈 디렉토리에서 첫 segment 가 startOffset=0 으로 생성된다")
    void createsFirstSegmentAtZero(@TempDir Path tempDir) {
        try (SegmentDirectory directory = SegmentDirectory.openOrCreate(tempDir, DEFAULT_MAX_BYTES)) {
            assertThat(directory.readAt(0L)).isNull(); // 비어있는 첫 세그먼트: 아직 아무 메시지도 없음
        }
    }

    @Test
    @DisplayName("rotate 이후 새 segment의 startOffset은 이전 segment의 nextAbsoluteOffset")
    void rotatesAtThreshold(@TempDir Path tempDir) {
        try (SegmentDirectory directory = SegmentDirectory.openOrCreate(tempDir, 1L)) { // 1 byte 한계: 기존 데이터가 있으면 다음 메시지 전에 rotate
            final Message message = new Message(new Topic("topic"), Map.of("k", "v"));
            directory.append(message); // 빈 세그먼트이므로 rotate 없이 쓰기. 이후 size > 1
            directory.append(message); // size > 0 && size >= 1 → rotate 후 새 세그먼트에 쓰기

            assertThat(directory.readAt(0L)).isEqualTo(message); // offset=0: 첫 세그먼트
            assertThat(directory.readAt(1L)).isEqualTo(message); // offset=1: rotate 후 새 세그먼트
            assertThat(directory.readAt(2L)).isNull();         // 그 이상은 없음
        }
    }

    @Test
    @DisplayName("cross-segment readAt이 동작한다")
    void crossSegmentRead(@TempDir Path tempDir) {
        try (SegmentDirectory directory = SegmentDirectory.openOrCreate(tempDir, 1L)) { // 각 메시지마다 세그먼트 회전
            final Message first = new Message(new Topic("topic"), Map.of("seq", 1));
            final Message second = new Message(new Topic("topic"), Map.of("seq", 2));
            final Message third = new Message(new Topic("topic"), Map.of("seq", 3));

            directory.append(first);  // segment-0 에 저장, offset=0
            directory.append(second); // segment-1 에 저장, offset=1
            directory.append(third);  // segment-2 에 저장, offset=2

            assertThat(directory.readAt(0L)).isEqualTo(first);  // floorEntry(0) → segment-0
            assertThat(directory.readAt(1L)).isEqualTo(second); // floorEntry(1) → segment-1
            assertThat(directory.readAt(2L)).isEqualTo(third);  // floorEntry(2) → segment-2
        }
    }

    @Test
    @DisplayName(".mmm에 trailing partial bytes 있으면 truncate 복구한다")
    void recoversByTruncating(@TempDir Path tempDir) throws IOException {
        try (SegmentDirectory directory = SegmentDirectory.openOrCreate(tempDir, DEFAULT_MAX_BYTES)) {
            directory.append(new Message(new Topic("topic"), Map.of("k", "v"))); // 정상 메시지 1개 커밋
        } // close(): 브로커 정상 종료 시뮬레이션

        final Path segmentFile = tempDir.resolve("segment-00000000000000000000.mmm"); // 첫 세그먼트 .mmm 파일 경로
        final long beforeSize = Files.size(segmentFile); // 정상 종료 후 .mmm 크기 보존
        try (FileChannel channel = FileChannel.open(segmentFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            channel.write(ByteBuffer.wrap("garbage".getBytes())); // 비정상 종료 시 fsync 안된 partial write 시뮬레이션
        }
        assertThat(Files.size(segmentFile)).isGreaterThan(beforeSize); // garbage 추가 확인

        try (SegmentDirectory directory = SegmentDirectory.openOrCreate(tempDir, DEFAULT_MAX_BYTES)) { // 재시작 시뮬레이션
            assertThat(Files.size(segmentFile)).isEqualTo(beforeSize); // recoverActiveSegment가 trailing bytes를 제거했는지 확인
            assertThat(directory.readAt(0L)).isNotNull(); // 커밋된 메시지 1개는 유지됨
            assertThat(directory.readAt(1L)).isNull();   // 그 이상은 없음
        }
    }

    @Test
    @DisplayName(".idx가 .mmm보다 더 멀리 가리키면 StorageException 던진다")
    void throwsWhenIndexBeyondSegment(@TempDir Path tempDir) throws IOException {
        try (SegmentDirectory directory = SegmentDirectory.openOrCreate(tempDir, DEFAULT_MAX_BYTES)) {
            directory.append(new Message(new Topic("topic"), Map.of("k", "v"))); // 정상 메시지 커밋
        }
        final Path segmentFile = tempDir.resolve("segment-00000000000000000000.mmm");
        try (FileChannel channel = FileChannel.open(segmentFile, StandardOpenOption.WRITE)) {
            channel.truncate(2L); // .mmm을 강제로 2 bytes로 잘라 .idx가 가리키는 위치보다 짧게 만들어 손상 시뮬레이션
        }

        assertThatThrownBy(() -> SegmentDirectory.openOrCreate(tempDir, DEFAULT_MAX_BYTES)) // 재시작 시도
                .isInstanceOf(StorageException.class); // 복구 불가 손상: fail-fast가 올바르게 작동해야 함
    }

    @Test
    @DisplayName("여러 segment가 존재하는 디렉토리는 모든 segment를 로드한다")
    void loadsAllSegments(@TempDir Path tempDir) {
        try (SegmentDirectory directory = SegmentDirectory.openOrCreate(tempDir, 1L)) { // 각 메시지마다 회전
            directory.append(new Message(new Topic("topic"), Map.of("seq", 1))); // segment-0
            directory.append(new Message(new Topic("topic"), Map.of("seq", 2))); // segment-1
            directory.append(new Message(new Topic("topic"), Map.of("seq", 3))); // segment-2
        } // 브로커 종료 시뮬레이션

        try (SegmentDirectory reopened = SegmentDirectory.openOrCreate(tempDir, 1L)) { // 재시작 시뮬레이션
            final Message first = reopened.readAt(0L); // segment-0에서 읽기
            final Message last = reopened.readAt(2L);  // segment-2에서 읽기
            assertThat(first).isNotNull();              // 첫 번째 세그먼트도 정상 로드됨
            assertThat(last).isNotNull();               // 마지막 세그먼트도 정상 로드됨
            assertThat(reopened.readAt(3L)).isNull();  // 3개 메시지가 모두 복원됨 (그 이상은 없음)
        }
    }
}
