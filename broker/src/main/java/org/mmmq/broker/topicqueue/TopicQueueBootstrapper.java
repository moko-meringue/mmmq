package org.mmmq.broker.topicqueue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.mmmq.broker.config.StorageProperties;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueBootstrapper implements SmartInitializingSingleton {

    private final Path root; // 세그먼트 파일 루트 디렉토리. Factory와는 별도로 보유 (디스크 enumeration 책임)
    private final TopicQueueFactory factory; // 발견한 토픽으로 TopicQueue를 조립
    private final TopicQueueRegistry registry; // 조립된 큐를 등록. register가 이벤트 발행을 담당하므로 Bootstrap은 직접 발행하지 않음

    public TopicQueueBootstrapper(StorageProperties storage, TopicQueueFactory factory, TopicQueueRegistry registry) {
        this.root = Path.of(storage.rootDir());
        this.factory = factory;
        this.registry = registry;
    }

    @Override
    public void afterSingletonsInstantiated() { // 모든 singleton(Dispatcher 포함) init 후 실행되어 이벤트 listener가 준비된 상태에서 발행이 가능
        if (!Files.isDirectory(root)) { // 첫 실행이거나 data/가 없으면 복원할 토픽 없음
            return;
        }
        try (Stream<Path> entries = Files.list(root)) {
            entries.filter(Files::isDirectory) // data/ 내 서브디렉토리만 토픽으로 간주
                    .map(path -> new Topic(path.getFileName().toString())) // 디렉토리명 → Topic 객체
                    .map(factory::create) // 각 토픽으로 TopicQueue 조립
                    .forEach(registry::register); // 등록 + 이벤트 발행
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to scan data directory: " + root, exception);
        }
    }
}
