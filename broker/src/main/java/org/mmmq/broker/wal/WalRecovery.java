package org.mmmq.broker.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueRegistry;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

// 브로커 재시작 시 WAL 파일에서 미소비 메시지를 TopicQueue에 복구한다.
// SmartInitializingSingleton을 구현하면 모든 빈 초기화 완료 후,
// 임베디드 서버 기동 전에 실행되므로 복구 완료 전 Producer 요청이 유입되지 않는다.
@Component
public class WalRecovery implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(WalRecovery.class);

    // 파일명에서 topicName과 segmentIndex를 추출하기 위한 패턴.
    // 예: "order-2.wal" → group(1)="order", group(2)="2"
    private static final Pattern WAL_FILE_PATTERN =
            Pattern.compile("^(.+)-(\\d+)\\.wal$");

    private final TopicQueueRegistry registry;
    private final ApplicationEventPublisher publisher;
    private final WalReader walReader = new WalReader();

    public WalRecovery(TopicQueueRegistry registry, ApplicationEventPublisher publisher) {
        this.registry = registry;
        this.publisher = publisher;
    }

    @Override
    public void afterSingletonsInstantiated() {
        Path walDir = registry.walDir();
        if (!Files.isDirectory(walDir)) {
            return;
        }

        collectWalFiles(walDir).forEach(this::recoverTopic);
    }

    private Map<String, List<WalFile>> collectWalFiles(Path walDir) {
        try (Stream<Path> files = Files.list(walDir)) {
            return files
                    .map(this::toWalFile)
                    .filter(Objects::nonNull)
                    .collect(Collectors.groupingBy(WalFile::topicName));
        } catch (IOException exception) {
            throw new RuntimeException("Failed to list WAL directory: " + walDir, exception);
        }
    }

    // 네이밍 규칙에 맞지 않는 파일(README, 임시 파일 등)은 null로 반환해 필터링한다.
    private WalFile toWalFile(Path path) {
        Matcher matcher = WAL_FILE_PATTERN.matcher(path.getFileName().toString());
        if (!matcher.matches()) {
            return null;
        }

        return new WalFile(matcher.group(1), Integer.parseInt(matcher.group(2)), path);
    }

    private void recoverTopic(String topicName, List<WalFile> walFiles) {
        // segmentIndex 오름차순으로 재생해야 메시지 순서가 보장된다.
        List<WalFile> sorted = walFiles.stream()
                .sorted(Comparator.comparingInt(WalFile::segmentIndex))
                .toList();

        // registry.get()이 TopicQueue를 생성하면서 Dispatcher 구독도 완료된다.
        // 따라서 이 시점부터 publish된 이벤트는 Dispatcher가 수신할 수 있다.
        TopicQueue topicQueue = registry.get(new Topic(topicName));

        // flatMap이 각 파일의 sub-stream을 소비 후 자동으로 close()하므로
        // 한 번에 한 줄씩만 역직렬화되어 메모리에 올라온다.
        // offerWithoutWal: 이미 WAL에 기록된 메시지이므로 이중 기록을 막기 위해 WAL을 건너뛴다.
        AtomicLong replayed = new AtomicLong();
        sorted.stream()
                .flatMap(walFile -> walReader.stream(walFile.path()))
                .forEach(entry -> {
                    topicQueue.offerWithoutWal(entry.message());
                    replayed.incrementAndGet();
                });

        if (replayed.get() > 0) {
            log.info("Recovered {} messages for topic '{}' from WAL", replayed, topicName);
            publisher.publishEvent(new TopicQueueRecoveredEvent(topicQueue));
        }
    }

    private record WalFile(
            String topicName,
            int segmentIndex,
            Path path
    ) {
    }
}
