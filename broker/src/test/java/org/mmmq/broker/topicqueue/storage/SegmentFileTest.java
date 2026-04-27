package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class SegmentFileTest {

    @Test
    @DisplayName("append 후 readAt 으로 같은 메시지를 읽을 수 있다")
    void roundTrip(@TempDir Path tempDir) {
        try (SegmentFile segment = SegmentFile.openOrCreate(tempDir, 0L)) { // startOffset=0으로 세그먼트 생성
            final Message message = new Message(new Topic("topic"), Map.of("k", "v"));

            segment.append(message); // .mmm + .idx에 fsync까지 완료

            final Optional<Message> read = segment.readAt(0L); // 절대 offset=0 위치 조회
            assertThat(read).contains(message); // 저장한 메시지와 동일해야 함
        }
    }

    @Test
    @DisplayName("readAt에 존재하지 않는 offset이면 Optional.empty 반환")
    void readAtNonExistentOffsetReturnsEmpty(@TempDir Path tempDir) {
        try (SegmentFile segment = SegmentFile.openOrCreate(tempDir, 0L)) { // 아무 메시지도 append하지 않은 빈 세그먼트
            assertThat(segment.readAt(0L)).isEmpty();   // 엔트리 0개이므로 어떤 offset도 없음
            assertThat(segment.readAt(100L)).isEmpty(); // 범위 밖 offset도 안전하게 empty 반환
        }
    }

    @Test
    @DisplayName("startOffset이 0이 아닌 segment의 경우 절대 offset 기준으로 readAt 한다")
    void readAtWithNonZeroStartOffset(@TempDir Path tempDir) {
        try (SegmentFile segment = SegmentFile.openOrCreate(tempDir, 100L)) { // startOffset=100으로 회전된 세그먼트 시뮬레이션
            final Message message = new Message(new Topic("topic"), Map.of("k", "v"));
            segment.append(message); // 이 메시지의 절대 offset = 100

            assertThat(segment.readAt(99L)).isEmpty();        // startOffset 미만은 empty
            assertThat(segment.readAt(100L)).contains(message); // 정확한 절대 offset으로 조회
            assertThat(segment.readAt(101L)).isEmpty();       // 메시지가 하나뿐이므로 101은 없음
        }
    }

    @Test
    @DisplayName("nextAbsoluteOffset은 startOffset + entryCount")
    void nextAbsoluteOffset(@TempDir Path tempDir) {
        try (SegmentFile segment = SegmentFile.openOrCreate(tempDir, 50L)) { // startOffset=50
            assertThat(segment.nextAbsoluteOffset()).isEqualTo(50L); // 비어있으면 50+0=50

            segment.append(new Message(new Topic("topic"), Map.of("k", "v"))); // offset=50
            segment.append(new Message(new Topic("topic"), Map.of("k", "v"))); // offset=51

            assertThat(segment.nextAbsoluteOffset()).isEqualTo(52L); // 50+2=52. 다음 세그먼트의 startOffset으로 사용될 값
        }
    }

}
