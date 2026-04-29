package org.mmmq.broker.topicqueue.storage;

import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class OffsetCheckpointRegistry implements Closeable {

    private static final String SUBDIRECTORY_NAME = "checkpoints"; // OffsetCheckpoint 파일을 저장하는 서브디렉토리 이름

    private final Path path; // OffsetCheckpoint 파일이 저장되는 디렉토리 (base/checkpoints/)
    private final Map<String, OffsetCheckpoint> checkpoints = new ConcurrentHashMap<>(); // 이름 → OffsetCheckpoint

    private OffsetCheckpointRegistry(Path path) {
        this.path = path;
    }

    public static OffsetCheckpointRegistry open(Path base) {
        Path path = base.resolve(SUBDIRECTORY_NAME);
        try {
            Files.createDirectories(path);
        } catch (IOException exception) {
            throw new StorageException("Failed to create checkpoints directory: " + path, exception);
        }
        OffsetCheckpointRegistry registry = new OffsetCheckpointRegistry(path);
        registry.bootstrap(); // 디스크의 기존 checkpoint 파일을 모두 로드. Dispatcher subscribe에 의존하지 않고 디스크 상태가 source of truth

        return registry;
    }

    private void bootstrap() {
        OffsetCheckpoint.openAll(path)
                .forEach(checkpoint -> checkpoints.put(checkpoint.getName(), checkpoint));
    }

    public OffsetCheckpoint register(String name) { // 해당 이름의 OffsetCheckpoint가 있으면 반환, 없으면 새로 열어 등록
        return checkpoints.computeIfAbsent(name, key -> OffsetCheckpoint.open(path, key));
    }

    @Nullable
    public OffsetCheckpoint get(String name) { // 등록된 OffsetCheckpoint를 반환. 없으면 null. 부재 해석은 호출자 책임
        return checkpoints.get(name);
    }

    @Override
    public void close() { // 모든 OffsetCheckpoint의 파일 핸들을 닫아 fd 누수 방지
        checkpoints.values()
                .forEach(OffsetCheckpoint::close);
    }
}
