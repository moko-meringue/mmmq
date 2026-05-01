package org.mmmq.broker.topicqueue.storage;

import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CheckpointDirectory implements Closeable {

    private static final String SUBDIRECTORY_NAME = "checkpoints";

    private final Path path;
    private final Map<String, CheckpointFile> checkpoints = new ConcurrentHashMap<>();

    private CheckpointDirectory(Path path) {
        this.path = path;
    }

    public static CheckpointDirectory open(Path base) {
        Path path = base.resolve(SUBDIRECTORY_NAME);
        try {
            Files.createDirectories(path);
        } catch (IOException exception) {
            throw new StorageException("Failed to create checkpoints directory: " + path, exception);
        }
        CheckpointDirectory registry = new CheckpointDirectory(path);
        registry.bootstrap();
        return registry;
    }

    private void bootstrap() {
        CheckpointFile.openAll(path)
                .forEach(checkpointFile -> checkpoints.put(checkpointFile.getName(), checkpointFile));
    }

    public CheckpointFile register(String name) {
        return checkpoints.computeIfAbsent(name, key -> CheckpointFile.open(path, key));
    }

    @Nullable
    public CheckpointFile get(String name) {
        return checkpoints.get(name);
    }

    @Override
    public void close() {
        checkpoints.values()
                .forEach(CheckpointFile::close);
    }
}
