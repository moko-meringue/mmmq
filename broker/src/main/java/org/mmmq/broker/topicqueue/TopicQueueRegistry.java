package org.mmmq.broker.topicqueue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.mmmq.broker.config.TopicStorageProperties;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.topicqueue.storage.SegmentDirectory;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueRegistry implements
        SmartInitializingSingleton { // 토픽 이름 → TopicQueue 맵을 관리. 부팅 시 data/ 디렉토리를 스캔해 기존 큐를 복원

    private static final Logger log = LoggerFactory.getLogger(TopicQueueRegistry.class);

    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>(); // 토픽 → TopicQueue. 동시 접근 안전
    private final ObjectProvider<Dispatcher> dispatcherProvider; // 순환 의존성 방지: 직접 List<Dispatcher> 주입 대신 lazy 조회
    private final TopicStorageProperties properties; // dataDir, segmentMaxBytes 설정값

    public TopicQueueRegistry(ObjectProvider<Dispatcher> dispatcherProvider, TopicStorageProperties properties) {
        this.dispatcherProvider = dispatcherProvider;
        this.properties = properties;
    }

    public TopicQueue get(Topic topic) { // 토픽의 큐를 조회하거나 없으면 새로 생성. FrontDispatcher에서 메시지 수신 시마다 호출
        return queues.computeIfAbsent(topic, this::create); // 동일 토픽에 대해 create가 한 번만 실행됨을 보장
    }

    @Override
    public void afterSingletonsInstantiated() {// data/ 디렉토리를 스캔해 기존 토픽 큐를 모두 복원. 브로커 재시작 시 dispatcher가 중단 지점부터 재개
        final Path root = Path.of(properties.dataDir()); // 세그먼트 파일 루트 디렉토리
        if (!Files.isDirectory(root)) { // 첫 실행이거나 data/가 없으면 복원할 큐가 없음
            return;
        }
        try (Stream<Path> topics = Files.list(root)) {
            topics.filter(Files::isDirectory) // data/ 내 서브디렉토리만 토픽으로 간주
                    .map(path -> new Topic(path.getFileName().toString())) // 디렉토리명 → Topic 객체
                    .forEach(this::get); // get()이 computeIfAbsent로 create()를 호출해 큐 복원
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to scan data directory: " + root,
                    exception); // data/ 스캔 실패는 브로커 기동 실패로 처리
        }
    }

    private TopicQueue create(Topic topic) { // TopicQueue를 새로 생성하고 등록된 모든 Dispatcher에 구독을 연결
        final Path topicDir = Path.of(properties.dataDir(), topic.name()); // data/{topic}/ 경로
        final SegmentDirectory directory = SegmentDirectory.openOrCreate(topicDir,
                properties.segmentMaxBytes()); // 세그먼트 파일 열기/생성 + 마지막 세그먼트 정합성 복구
        final TopicQueue queue = new TopicQueue(topic, directory);
        log.info("Restored topic queue: {}", topic.name());
        dispatcherProvider.stream()
                .forEach(dispatcher -> dispatcher.subscribe(queue)); // 각 Dispatcher가 자신의 패턴과 비교해 매칭되는 토픽만 구독

        return queue;
    }
}
