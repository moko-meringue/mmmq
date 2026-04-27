package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class SegmentTest {

    @Test
    @DisplayName("append 후 readAt 으로 같은 메시지를 읽을 수 있다")
    void roundTrip(@TempDir Path tempDir) {
        try (Segment segment = Segment.openOrCreate(tempDir, 0L)) {
            final Message message = new Message(new Topic("topic"), Map.of("k", "v"));

            segment.append(message);

            assertThat(segment.readAt(0L)).isEqualTo(message);
        }
    }

    @Test
    @DisplayName("readAt에 존재하지 않는 offset이면 null 반환")
    void readAtNonExistentOffsetReturnsNull(@TempDir Path tempDir) {
        try (Segment segment = Segment.openOrCreate(tempDir, 0L)) {
            assertThat(segment.readAt(0L)).isNull();
            assertThat(segment.readAt(100L)).isNull();
        }
    }

    @Test
    @DisplayName("readAt은 상대 offset 기준으로 동작한다")
    void readAtUsesRelativeOffset(@TempDir Path tempDir) {
        try (Segment segment = Segment.openOrCreate(tempDir, 100L)) {
            final Message message = new Message(new Topic("topic"), Map.of("k", "v"));
            segment.append(message);

            assertThat(segment.readAt(0L)).isEqualTo(message); // 상대 offset 0 = 첫 번째 메시지
            assertThat(segment.readAt(1L)).isNull();           // 상대 offset 1 = 메시지 없음
        }
    }

    @Test
    @DisplayName("count는 append한 메시지 수를 반환한다")
    void count(@TempDir Path tempDir) {
        try (Segment segment = Segment.openOrCreate(tempDir, 50L)) {
            assertThat(segment.count()).isZero();

            segment.append(new Message(new Topic("topic"), Map.of("k", "v")));
            segment.append(new Message(new Topic("topic"), Map.of("k", "v")));

            assertThat(segment.count()).isEqualTo(2L);
        }
    }
}
