package org.mmmq.broker.topicqueue.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;
import org.mmmq.broker.topicqueue.storage.FileHandle.FlushMode;

public final class OffsetCheckpoint implements Closeable { // 이름과 연결된 8바이트 long offset 값을 파일에 영속화

    private static final int VALUE_BYTES = Long.BYTES; // offset 값의 크기: long = 8 bytes
    private static final String EXTENSION = ".checkpoint";

    private final Path file; // {base}/{name}.checkpoint 경로
    private final String name; // 식별자. 파일명에 인코딩됨
    private final FileHandle fileHandle; // positional read/write를 지원하는 NIO 채널

    private OffsetCheckpoint(Path file, String name, FileHandle fileHandle) { // 외부에서 직접 생성하지 않도록 private
        this.file = file;
        this.name = name;
        this.fileHandle = fileHandle;
    }

    static List<OffsetCheckpoint> openAll(
            Path base) { // base 내의 모든 checkpoint 파일을 스캔해 열어 반환. 식별자(파일명) 컨벤션을 OffsetCheckpoint 안에 캡슐화
        try (Stream<Path> entries = Files.list(base)) {
            return entries
                    .filter(Files::isRegularFile)
                    .map(file -> file.getFileName().toString())
                    .filter(filename -> filename.endsWith(EXTENSION))
                    .map(filename -> filename.substring(0, filename.length() - EXTENSION.length()))
                    .map(name -> open(base, name))
                    .toList();
        } catch (IOException exception) {
            throw new StorageException("Failed to list checkpoints in: " + base, exception);
        }
    }

    static OffsetCheckpoint open(Path base, String name) { // base는 caller가 존재 보장
        Path file = base.resolve(name + EXTENSION);
        try {
            FileHandle fileHandle = FileHandle.open(
                    file,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );
            OffsetCheckpoint checkpoint = new OffsetCheckpoint(file, name, fileHandle);
            // MOKO: 새 OffsetCheckpoint 생성 시 처음부터 시작할지, 최신부터 시작할지 옵션 고려.
            if (fileHandle.size() == 0) {
                checkpoint.write(0L);
            }

            return checkpoint;
        } catch (IOException exception) {
            throw new StorageException("Failed to open offset checkpoint: " + file, exception);
        }
    }

    public String getName() {
        return name;
    }

    public long read() { // 파일에서 8바이트를 읽어 마지막으로 저장된 offset을 반환
        try {
            if (fileHandle.size() < VALUE_BYTES) { // 8바이트 미만: 파일이 손상됐거나 외부에서 변조됨
                throw new StorageException("Offset checkpoint is corrupted: " + file);
            }

            return ByteBuffer.wrap(fileHandle.readFully(0, VALUE_BYTES)).getLong();
        } catch (IOException exception) {
            throw new StorageException("Failed to read offset checkpoint: " + file, exception);
        }
    }

    public void write(long offset) { // offset을 파일에 덮어쓰고 fsync. at-least-once의 commit 지점: 이 호출 완료 후에만 offset이 전진
        try {
            ByteBuffer buffer = ByteBuffer.allocate(VALUE_BYTES);
            buffer.putLong(offset).flip();
            fileHandle.writeFully(0, buffer, FlushMode.FSYNC); // checkpoint 파일은 항상 address 0에 덮어씀. 파일 크기는 항상 8바이트
        } catch (IOException exception) {
            throw new StorageException("Failed to write offset: " + file, exception);
        }
    }

    @Override
    public void close() { // 사용 완료 후 fd 누수 방지
        try {
            fileHandle.close();
        } catch (IOException exception) {
            throw new StorageException("Failed to close offset checkpoint: " + file, exception);
        }
    }
}
