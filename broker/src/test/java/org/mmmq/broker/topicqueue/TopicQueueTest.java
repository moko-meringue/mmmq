package org.mmmq.broker.topicqueue;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.topicqueue.storage.OffsetCheckpointRegistry;
import org.mmmq.broker.topicqueue.storage.SegmentChain;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class TopicQueueTest {

    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024; // 회전이 발생하지 않도록 충분히 큰 값

    @Test
    @DisplayName("offer 성공 시 true 반환")
    void offerReturnsTrueOnSuccess(@TempDir Path tempDir) {
        final TopicQueue queue = createQueue(tempDir, "topic");
        final Message message = new Message(new Topic("topic"), Map.of("k", "v"));

        assertThat(queue.offer(message)).isTrue(); // 정상적인 디스크 쓰기 후 true 반환. Producer에 ACK 응답 조건
    }

    @Test
    @DisplayName("subscribe 후 peek은 첫 메시지를 반환한다")
    void peekReturnsFirstMessage(@TempDir Path tempDir) {
        final TopicQueue queue = createQueue(tempDir, "topic");
        final Message message = new Message(new Topic("topic"), Map.of("k", "v"));
        queue.offer(message); // offset=0에 메시지 저장

        final Offset offset = queue.subscribe("dispatcher-1"); // OffsetCheckpoint 없음 → offset 0으로 초기화
        final Message peeked = queue.peek(offset); // offset=0 위치의 메시지 조회

        assertThat(peeked).isEqualTo(message); // 저장한 메시지와 동일해야 함
    }

    @Test
    @DisplayName("commit 없이 peek만 반복하면 같은 메시지가 반환된다 (at-least-once)")
    void peekWithoutCommitReturnsSameMessage(@TempDir Path tempDir) {
        final TopicQueue queue = createQueue(tempDir, "topic");
        final Message message = new Message(new Topic("topic"), Map.of("k", "v"));
        queue.offer(message);

        final Offset offset = queue.subscribe("dispatcher-1");
        final Message first = queue.peek(offset);  // offset=0 조회, offset 변화 없음
        final Message second = queue.peek(offset); // 다시 offset=0 조회

        assertThat(first).isEqualTo(second); // peek은 offset을 전진시키지 않으므로 항상 같은 메시지 반환 (at-least-once 핵심)
    }

    @Test
    @DisplayName("commit 후 peek은 다음 메시지로 이동한다")
    void commitAdvancesOffset(@TempDir Path tempDir) {
        final TopicQueue queue = createQueue(tempDir, "topic");
        final Message first = new Message(new Topic("topic"), Map.of("seq", 1));
        final Message second = new Message(new Topic("topic"), Map.of("seq", 2));
        queue.offer(first);  // offset=0
        queue.offer(second); // offset=1

        Offset offset = queue.subscribe("dispatcher-1");
        assertThat(queue.peek(offset)).isEqualTo(first); // offset=0: 첫 번째 메시지
        offset = queue.commit("dispatcher-1", offset);   // 진전된 새 Offset 반환 + fsync
        assertThat(queue.peek(offset)).isEqualTo(second); // offset=1: 두 번째 메시지로 전진
    }

    @Test
    @DisplayName("재시작 후 subscribe는 마지막 commit 위치부터 재개된다")
    void resumesFromCommittedOffsetAfterRestart(@TempDir Path tempDir) {
        final TopicQueue queue = createQueue(tempDir, "topic");
        final Message first = new Message(new Topic("topic"), Map.of("seq", 1));
        final Message second = new Message(new Topic("topic"), Map.of("seq", 2));
        queue.offer(first);  // offset=0
        queue.offer(second); // offset=1

        final Offset offset = queue.subscribe("dispatcher-1");
        queue.peek(offset);                           // offset=0 조회 (commit 전)
        queue.commit("dispatcher-1", offset);         // offset=1로 전진 + fsync: 디스크에 1 기록
        // 브로커 재시작 시뮬레이션: 새 TopicQueue 인스턴스 생성

        final TopicQueue restarted = createQueue(tempDir, "topic"); // 같은 디렉토리를 가리켜 기존 파일 재사용
        final Offset restoredOffset = restarted.subscribe("dispatcher-1"); // OffsetCheckpoint에서 1을 읽어 Offset(1) 반환

        assertThat(restoredOffset.value()).isEqualTo(1L);         // 마지막 커밋 위치(1)부터 재개
        assertThat(restarted.peek(restoredOffset)).isEqualTo(second); // offset=1 → 두 번째 메시지
    }

    @Test
    @DisplayName("commit 전 재시작 시 같은 메시지가 다시 peek된다")
    void redeliversAfterCrashBeforeCommit(@TempDir Path tempDir) {
        final TopicQueue queue = createQueue(tempDir, "topic");
        final Message message = new Message(new Topic("topic"), Map.of("k", "v"));
        queue.offer(message); // offset=0에 저장

        final Offset offset = queue.subscribe("dispatcher-1");
        queue.peek(offset); // 메시지 조회했지만 commit 미호출 (브로커 크래시 시뮬레이션)

        final TopicQueue restarted = createQueue(tempDir, "topic"); // 재시작
        final Offset restoredOffset = restarted.subscribe("dispatcher-1"); // OffsetCheckpoint에 0이 저장되어 있음

        assertThat(restoredOffset.value()).isZero();                    // 커밋 없이 재시작했으므로 offset=0
        assertThat(restarted.peek(restoredOffset)).isEqualTo(message); // 같은 메시지가 다시 전달됨 (at-least-once)
    }

    private TopicQueue createQueue(Path baseDir, String topicName) { // 테스트용 TopicQueue 생성 헬퍼: @TempDir 기반 디렉토리 사용
        final Path topicDir = baseDir.resolve(topicName); // data/{topic}/ 역할
        try {
            Files.createDirectories(topicDir); // 토픽 디렉토리는 토픽 레이어 책임. storage 클래스들은 base 존재를 가정
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        final SegmentChain segmentChain = SegmentChain.open(topicDir, DEFAULT_MAX_BYTES);
        final OffsetCheckpointRegistry checkpointRegistry = OffsetCheckpointRegistry.open(topicDir);

        return new TopicQueue(new Topic(topicName), segmentChain, checkpointRegistry);
    }
}
