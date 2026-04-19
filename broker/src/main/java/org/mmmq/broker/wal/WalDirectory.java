package org.mmmq.broker.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.mmmq.broker.topicqueue.MessagePersistence;
import org.mmmq.broker.topicqueue.MessagePersistenceFactory;
import org.mmmq.broker.wal.codec.WalCodec;
import org.mmmq.broker.wal.flush.WalFlushPolicy;
import org.mmmq.broker.wal.flush.WalFlushPolicyFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class WalDirectory implements MessagePersistenceFactory {

    private final Path path;
    private final WalCodec codec;
    private final WalFlushPolicy flushPolicy;
    private final List<WalFile> segmentFiles;

    public WalDirectory(
            WalCodec codec,
            @Value("${mmmq.broker.wal.dir:./wal}") String walDir,
            @Value("${mmmq.broker.wal.flush-policy:page_cache}") String flushPolicyName
    ) {
        this.path = Paths.get(walDir);
        this.codec = codec;
        this.flushPolicy = WalFlushPolicyFactory.create(flushPolicyName);
        createDirectory();
        this.segmentFiles = loadWalFiles();
    }

    @Override
    public MessagePersistence create(String topicName) {
        return new WalMessagePersistence(this, topicName, codec, flushPolicy);
    }

    public List<WalFile> segmentFiles() {
        return segmentFiles;
    }

    Stream<WalEntry> read(WalFile segmentFile) {
        return segmentFile.read(codec);
    }

    List<String> topicNames() {
        return segmentFiles.stream()
                .map(WalFile::topicName)
                .distinct()
                .toList();
    }

    List<WalFile> segmentFiles(String topicName) {
        return segmentFiles.stream()
                .filter(segmentFile -> segmentFile.topicName().equals(topicName))
                .toList();
    }

    WalFile createWalFile(String topicName, int segmentIndex) {
        return WalFile.of(path, topicName, segmentIndex);
    }

    private void createDirectory() {
        try {
            Files.createDirectories(path);
        } catch (IOException exception) {
            throw new RuntimeException("Failed to create WAL directory: " + path, exception);
        }
    }

    private List<WalFile> loadWalFiles() {
        try (Stream<Path> files = Files.list(path)) {
            return files
                    .map(WalFile::parse)
                    .filter(Objects::nonNull)
                    .sorted(Comparator.comparingInt(WalFile::index))
                    .toList();
        } catch (IOException exception) {
            throw new RuntimeException("Failed to list WAL directory: " + path, exception);
        }
    }
}
