package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class SegmentFileTest {

    @Test
    @DisplayName("append 후 readAt 으로 같은 메시지를 읽을 수 있다")
    void roundTrip(@TempDir Path tempDir) {
        try (SegmentFile segmentFile = SegmentFile.open(tempDir, 0L)) {
            final Message message = new Message(new Topic("topic"), Map.of("k", "v"));

            segmentFile.append(message);

            assertThat(segmentFile.readAt(0L)).isEqualTo(message);
        }
    }

    @Test
    @DisplayName("readAt에 존재하지 않는 offset이면 null 반환")
    void readAtNonExistentOffsetReturnsNull(@TempDir Path tempDir) {
        try (SegmentFile segmentFile = SegmentFile.open(tempDir, 0L)) {
            assertThat(segmentFile.readAt(0L)).isNull();
            assertThat(segmentFile.readAt(100L)).isNull();
        }
    }

    @Test
    @DisplayName("readAt은 상대 offset 기준으로 동작한다")
    void readAtUsesRelativeOffset(@TempDir Path tempDir) {
        try (SegmentFile segmentFile = SegmentFile.open(tempDir, 100L)) {
            final Message message = new Message(new Topic("topic"), Map.of("k", "v"));
            segmentFile.append(message);

            assertThat(segmentFile.readAt(0L)).isEqualTo(message);
            assertThat(segmentFile.readAt(1L)).isNull();
        }
    }

    @Test
    @DisplayName("count는 append한 메시지 수를 반환한다")
    void count(@TempDir Path tempDir) {
        try (SegmentFile segmentFile = SegmentFile.open(tempDir, 50L)) {
            assertThat(segmentFile.count()).isZero();

            segmentFile.append(new Message(new Topic("topic"), Map.of("k", "v")));
            segmentFile.append(new Message(new Topic("topic"), Map.of("k", "v")));

            assertThat(segmentFile.count()).isEqualTo(2L);
        }
    }
}
