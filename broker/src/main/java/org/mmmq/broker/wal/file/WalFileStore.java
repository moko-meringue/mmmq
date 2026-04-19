package org.mmmq.broker.wal.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class WalFileStore {

    private final Path rootPath;

    public WalFileStore(@Value("${mmmq.broker.wal.dir:./wal}") String walDir) {
        this.rootPath = Paths.get(walDir);
        try {
            Files.createDirectories(rootPath);
        } catch (IOException exception) {
            throw new RuntimeException("Failed to create WAL directory: " + rootPath, exception);
        }
    }

    public WalFile create(String topicName, int walFileIndex) {
        return WalFile.of(rootPath, topicName, walFileIndex);
    }

    public void delete(String topicName, int walFileIndex) {
        try {
            Path walFilePath = rootPath.resolve(WalFile.pathOf(topicName, walFileIndex));
            Files.deleteIfExists(walFilePath);
        } catch (IOException exception) {
            throw new RuntimeException("Failed to delete WAL segment file: " + topicName + "-" + walFileIndex,
                    exception);
        }
    }

    public List<WalFile> walFiles() {
        try (Stream<Path> files = Files.list(rootPath)) {
            return files
                    .map(WalFile::parse)
                    .filter(Objects::nonNull)
                    .sorted(Comparator.comparingInt(WalFile::index))
                    .toList();
        } catch (IOException exception) {
            throw new RuntimeException("Failed to list WAL directory: " + rootPath, exception);
        }
    }
}
