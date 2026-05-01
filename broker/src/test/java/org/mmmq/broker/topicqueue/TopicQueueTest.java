package org.mmmq.broker.topicqueue;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class TopicQueueTest {

    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;

    @Test
    @DisplayName("offer 성공 시 true 반환")
    void offerReturnsTrueOnSuccess(@TempDir Path tempDir) {
        final TopicQueue queue = createQueue(tempDir, "topic");
        final Message message = new Message(new Topic("topic"), Map.of("k", "v"));

        assertThat(queue.offer(message)).isTrue();
    }

    @Test
    @DisplayName("subscribe 후 peek은 첫 메시지를 반환한다")
    void peekReturnsFirstMessage(@TempDir Path tempDir) {
        final TopicQueue queue = createQueue(tempDir, "topic");
        final Message message = new Message(new Topic("topic"), Map.of("k", "v"));
        queue.offer(message);

        final Offset offset = queue.subscribe("dispatcher-1");
        final Message peeked = queue.peek(offset);

        assertThat(peeked).isEqualTo(message);
    }

    @Test
    @DisplayName("commit 없이 peek만 반복하면 같은 메시지가 반환된다 (at-least-once)")
    void peekWithoutCommitReturnsSameMessage(@TempDir Path tempDir) {
        final TopicQueue queue = createQueue(tempDir, "topic");
        final Message message = new Message(new Topic("topic"), Map.of("k", "v"));
        queue.offer(message);

        final Offset offset = queue.subscribe("dispatcher-1");
        final Message first = queue.peek(offset);
        final Message second = queue.peek(offset);

        assertThat(first).isEqualTo(second);
    }

    @Test
    @DisplayName("commit 후 peek은 다음 메시지로 이동한다")
    void commitAdvancesOffset(@TempDir Path tempDir) {
        final TopicQueue queue = createQueue(tempDir, "topic");
        final Message first = new Message(new Topic("topic"), Map.of("seq", 1));
        final Message second = new Message(new Topic("topic"), Map.of("seq", 2));
        queue.offer(first);
        queue.offer(second);

        Offset offset = queue.subscribe("dispatcher-1");
        assertThat(queue.peek(offset)).isEqualTo(first);
        offset = queue.commit("dispatcher-1", offset);
        assertThat(queue.peek(offset)).isEqualTo(second);
    }

    @Test
    @DisplayName("재시작 후 subscribe는 마지막 commit 위치부터 재개된다")
    void resumesFromCommittedOffsetAfterRestart(@TempDir Path tempDir) {
        final TopicQueue queue = createQueue(tempDir, "topic");
        final Message first = new Message(new Topic("topic"), Map.of("seq", 1));
        final Message second = new Message(new Topic("topic"), Map.of("seq", 2));
        queue.offer(first);
        queue.offer(second);

        final Offset offset = queue.subscribe("dispatcher-1");
        queue.peek(offset);
        queue.commit("dispatcher-1", offset);

        final TopicQueue restarted = createQueue(tempDir, "topic");
        final Offset restoredOffset = restarted.subscribe("dispatcher-1");

        assertThat(restoredOffset.value()).isEqualTo(1L);
        assertThat(restarted.peek(restoredOffset)).isEqualTo(second);
    }

    @Test
    @DisplayName("commit 전 재시작 시 같은 메시지가 다시 peek된다")
    void redeliversAfterCrashBeforeCommit(@TempDir Path tempDir) {
        final TopicQueue queue = createQueue(tempDir, "topic");
        final Message message = new Message(new Topic("topic"), Map.of("k", "v"));
        queue.offer(message);

        final Offset offset = queue.subscribe("dispatcher-1");
        queue.peek(offset);

        final TopicQueue restarted = createQueue(tempDir, "topic");
        final Offset restoredOffset = restarted.subscribe("dispatcher-1");

        assertThat(restoredOffset.value()).isZero();
        assertThat(restarted.peek(restoredOffset)).isEqualTo(message);
    }

    private TopicQueue createQueue(Path baseDir, String topicName) {
        final Path topicDir = baseDir.resolve(topicName);
        try {
            Files.createDirectories(topicDir);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        final SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, DEFAULT_MAX_BYTES);
        final CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(topicDir);

        return new TopicQueue(new Topic(topicName), segmentFileChain, checkpointDirectory);
    }
}
