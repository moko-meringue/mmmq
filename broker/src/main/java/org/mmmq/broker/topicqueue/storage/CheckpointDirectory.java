package org.mmmq.broker.topicqueue.storage;

import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class CheckpointDirectory implements Closeable {

    private static final String SUBDIRECTORY_NAME = "checkpoints"; // Checkpoint 파일을 저장하는 서브디렉토리 이름

    private final Path path; // Checkpoint 파일이 저장되는 디렉토리 (base/checkpoints/)
    private final Map<String, CheckpointFile> checkpoints = new ConcurrentHashMap<>(); // 이름 → Checkpoint

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
        registry.bootstrap(); // 디스크의 기존 checkpoint 파일을 모두 로드. Dispatcher subscribe에 의존하지 않고 디스크 상태가 source of truth
        return registry;
    }

    private void bootstrap() {
        CheckpointFile.openAll(path)
                .forEach(checkpointFile -> checkpoints.put(checkpointFile.getName(), checkpointFile));
    }

    public CheckpointFile register(String name) { // 해당 이름의 Checkpoint가 있으면 반환, 없으면 새로 열어 등록
        return checkpoints.computeIfAbsent(name, key -> CheckpointFile.open(path, key));
    }

    @Nullable
    public CheckpointFile get(String name) { // 등록된 Checkpoint를 반환. 없으면 null. 부재 해석은 호출자 책임
        return checkpoints.get(name);
    }

    @Override
    public void close() { // 모든 Checkpoint의 파일 핸들을 닫아 fd 누수 방지
        checkpoints.values()
                .forEach(CheckpointFile::close);
    }
}
