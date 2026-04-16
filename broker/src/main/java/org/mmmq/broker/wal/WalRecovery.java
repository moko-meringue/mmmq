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

@Component
public class WalRecovery implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(WalRecovery.class);
    private static final Pattern WAL_FILE_PATTERN = Pattern.compile("^(.+)-(\\d+)\\.wal$");

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

    private WalFile toWalFile(Path path) {
        Matcher matcher = WAL_FILE_PATTERN.matcher(path.getFileName().toString());
        if (!matcher.matches()) {
            return null;
        }

        return new WalFile(matcher.group(1), Integer.parseInt(matcher.group(2)), path);
    }

    private void recoverTopic(String topicName, List<WalFile> walFiles) {
        List<WalFile> sorted = walFiles.stream()
                .sorted(Comparator.comparingInt(WalFile::segmentIndex))
                .toList();
        TopicQueue topicQueue = registry.get(new Topic(topicName));
        AtomicLong replayed = new AtomicLong();
        sorted.stream()
                .flatMap(walFile -> walReader.stream(walFile.path()))
                .forEach(entry -> {
                    topicQueue.offer(entry.message(), false);
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
