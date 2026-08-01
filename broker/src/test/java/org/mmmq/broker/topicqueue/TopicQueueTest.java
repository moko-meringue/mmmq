package org.mmmq.broker.topicqueue;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class TopicQueueTest {

    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;

    @Test
    @DisplayName("offer 성공 시 true 반환")
    void offerReturnsTrueOnSuccess(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        Message message = new Message(new Topic("topic"), Map.of("k", "v"));

        assertThat(queue.offer(message)).isTrue();
    }

    @Test
    @DisplayName("offset 0에 대한 peek은 첫 메시지를 반환한다")
    void peekReturnsFirstMessage(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        Message message = new Message(new Topic("topic"), Map.of("k", "v"));
        queue.offer(message);

        Message peeked = queue.peek(new Offset(0L));

        assertThat(peeked).isEqualTo(message);
    }

    @Test
    @DisplayName("다음 offset의 peek은 다음 메시지를 반환한다")
    void peekAdvancesToNextMessageByOffset(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        Message first = new Message(new Topic("topic"), Map.of("seq", 1));
        Message second = new Message(new Topic("topic"), Map.of("seq", 2));
        queue.offer(first);
        queue.offer(second);

        assertThat(queue.peek(new Offset(0L))).isEqualTo(first);
        assertThat(queue.peek(new Offset(1L))).isEqualTo(second);
    }

    @Test
    @DisplayName("아직 쓰이지 않은 offset의 peek은 null을 반환한다")
    void peekPastTailReturnsNull(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        queue.offer(new Message(new Topic("topic"), Map.of("k", "v")));

        assertThat(queue.peek(new Offset(1L))).isNull();
    }

    @Test
    @DisplayName("tailOffset은 다음에 쓰일 절대 offset을 반환한다")
    void tailOffsetReturnsNextWriteOffset(@TempDir Path tempDir) {
        TopicQueue queue = createQueue(tempDir, "topic");
        queue.offer(new Message(new Topic("topic"), Map.of("seq", 1)));
        queue.offer(new Message(new Topic("topic"), Map.of("seq", 2)));

        assertThat(queue.tailOffset()).isEqualTo(2L);
    }

    private TopicQueue createQueue(Path baseDir, String topicName) {
        Path topicDir = baseDir.resolve(topicName);
        try {
            Files.createDirectories(topicDir);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, DEFAULT_MAX_BYTES);

        return new TopicQueue(new Topic(topicName), segmentFileChain);
    }
}
