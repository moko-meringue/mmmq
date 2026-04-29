package org.mmmq.broker.topicqueue;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.config.TopicStorageProperties;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.topicqueue.storage.OffsetCheckpointRegistry;
import org.mmmq.broker.topicqueue.storage.SegmentChain;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

class TopicQueueRegistryRestoreTest {

    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;

    @Test
    @DisplayName("data 디렉토리에 존재하는 토픽들이 부팅 시 모두 복원된다")
    void restoresAllTopicsOnBoot(@TempDir Path tempDir) {
        seedTopic(tempDir, "topic-a", new Message(new Topic("topic-a"), Map.of("k", "v"))); // 재시작 전에 data/topic-a/ 세그먼트 파일 생성
        seedTopic(tempDir, "topic-b", new Message(new Topic("topic-b"), Map.of("k", "v"))); // 재시작 전에 data/topic-b/ 세그먼트 파일 생성

        final ObjectProvider<Dispatcher> emptyProvider = emptyDispatcherProvider(); // dispatcher 없이 순수 복원 로직만 검증
        final TopicStorageProperties properties = new TopicStorageProperties(
                tempDir.toAbsolutePath().toString(), // tempDir를 data/ 루트로 사용
                DEFAULT_MAX_BYTES
        );
        final TopicQueueRegistry registry = new TopicQueueRegistry(emptyProvider, properties);

        registry.afterSingletonsInstantiated(); // ContextRefreshedEvent 대신 직접 호출로 부팅 복원 시뮬레이션

        final TopicQueue queueA = registry.get(new Topic("topic-a")); // 복원된 큐 조회
        final TopicQueue queueB = registry.get(new Topic("topic-b"));
        final Offset offsetA = queueA.subscribe("dispatcher-1"); // OffsetCheckpoint 없음 → offset=0
        final Offset offsetB = queueB.subscribe("dispatcher-1");

        assertThat(queueA.peek(offsetA)).isNotNull(); // offset=0에 메시지가 존재해야 함
        assertThat(queueB.peek(offsetB)).isNotNull();
    }

    @Test
    @DisplayName("dispatcher가 마지막 commit 위치에서 재개한다")
    void resumesFromLastCommittedOffset(@TempDir Path tempDir) throws IOException {
        final Path topicDir = tempDir.resolve("topic-a");
        Files.createDirectories(topicDir); // 토픽 디렉토리는 토픽 레이어 책임
        final SegmentChain segmentChain = SegmentChain.open(topicDir, DEFAULT_MAX_BYTES);
        final OffsetCheckpointRegistry checkpointRegistry = OffsetCheckpointRegistry.open(topicDir);
        final TopicQueue queue = new TopicQueue(new Topic("topic-a"), segmentChain, checkpointRegistry);
        queue.offer(new Message(new Topic("topic-a"), Map.of("seq", 1))); // offset=0
        queue.offer(new Message(new Topic("topic-a"), Map.of("seq", 2))); // offset=1
        final Offset offset = queue.subscribe("dispatcher-1");
        queue.peek(offset);
        queue.commit("dispatcher-1", offset); // offset=1을 OffsetCheckpoint에 fsync. 재시작 후 이 위치부터 재개해야 함

        final TopicStorageProperties properties = new TopicStorageProperties(
                tempDir.toAbsolutePath().toString(),
                DEFAULT_MAX_BYTES
        );
        final TopicQueueRegistry registry = new TopicQueueRegistry(emptyDispatcherProvider(), properties);
        registry.afterSingletonsInstantiated(); // 부팅 복원

        final TopicQueue restored = registry.get(new Topic("topic-a")); // 복원된 큐
        final Offset restoredOffset = restored.subscribe("dispatcher-1"); // OffsetCheckpoint에서 1을 읽음

        assertThat(restoredOffset.value()).isEqualTo(1L); // 커밋된 위치(1)부터 재개됨을 검증
    }

    @Test
    @DisplayName("data 디렉토리가 없으면 정상 부팅된다")
    void noDataDirectoryDoesNotFail(@TempDir Path tempDir) {
        final TopicStorageProperties properties = new TopicStorageProperties(
                tempDir.resolve("nonexistent").toAbsolutePath().toString(), // 존재하지 않는 경로
                DEFAULT_MAX_BYTES
        );
        final TopicQueueRegistry registry = new TopicQueueRegistry(emptyDispatcherProvider(), properties);

        registry.afterSingletonsInstantiated(); // data/ 없으면 스캔 없이 정상 종료

        assertThat(registry.get(new Topic("anything"))).isNotNull(); // lazy 생성은 여전히 동작해야 함
    }

    private void seedTopic(Path baseDir, String topicName, Message message) { // 재시작 전 상태를 준비하는 헬퍼: 토픽 디렉토리와 세그먼트 파일 생성
        final Path topicDir = baseDir.resolve(topicName);
        try {
            Files.createDirectories(topicDir);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        try (SegmentChain segmentChain = SegmentChain.open(topicDir, DEFAULT_MAX_BYTES)) {
            segmentChain.append(message); // 메시지를 디스크에 기록하고 채널 닫기
        }
    }

    private ObjectProvider<Dispatcher> emptyDispatcherProvider() { // 등록된 Dispatcher 빈이 없는 빈 provider 생성. registry.create()에서 dispatcher.subscribe()를 호출하지 않도록
        final DefaultListableBeanFactory factory = new DefaultListableBeanFactory();

        return factory.getBeanProvider(Dispatcher.class); // 빈이 없는 팩토리에서 provider 반환
    }
}
