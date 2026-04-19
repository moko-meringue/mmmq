package org.mmmq.broker.wal;

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
public class WalFileStore implements WalFileCreator {

    private final Path path;
    private final List<WalFile> walFiles;

    public WalFileStore(@Value("${mmmq.broker.wal.dir:./wal}") String walDir) {
        this.path = Paths.get(walDir);
        createDirectory();
        this.walFiles = loadWalFiles();
    }

    public List<WalFile> segmentFiles() {
        return walFiles;
    }

    @Override
    public WalFile create(String topicName, int index) {
        return WalFile.of(path, topicName, index);
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
