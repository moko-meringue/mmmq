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

public class CheckpointFile implements Closeable {

    private static final int VALUE_BYTES = Long.BYTES;
    private static final String EXTENSION = ".checkpoint";

    private final Path file;
    private final String name;
    private final FileHandle fileHandle;

    private CheckpointFile(Path file, String name, FileHandle fileHandle) {
        this.file = file;
        this.name = name;
        this.fileHandle = fileHandle;
    }

    static List<CheckpointFile> openAll(Path base) {
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

    static CheckpointFile open(Path base, String name) {
        Path file = base.resolve(name + EXTENSION);
        try {
            FileHandle fileHandle = FileHandle.open(
                    file,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );
            CheckpointFile checkpointFile = new CheckpointFile(file, name, fileHandle);
            // MOKO: 새 Checkpoint 생성 시 처음부터 시작할지, 최신부터 시작할지 옵션 고려.
            if (fileHandle.size() == 0) {
                checkpointFile.write(0L);
            }
            return checkpointFile;
        } catch (IOException exception) {
            throw new StorageException("Failed to open offset checkpoint: " + file, exception);
        }
    }

    public String getName() {
        return name;
    }

    public long read() {
        try {
            if (fileHandle.size() < VALUE_BYTES) {
                throw new StorageException("Offset checkpoint is corrupted: " + file);
            }
            return ByteBuffer.wrap(fileHandle.readFully(0, VALUE_BYTES)).getLong();
        } catch (IOException exception) {
            throw new StorageException("Failed to read offset checkpoint: " + file, exception);
        }
    }

    public void write(long offset) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(VALUE_BYTES);
            buffer.putLong(offset).flip();
            fileHandle.writeFully(0, buffer, FlushMode.FSYNC);
        } catch (IOException exception) {
            throw new StorageException("Failed to write offset: " + file, exception);
        }
    }

    @Override
    public void close() {
        try {
            fileHandle.close();
        } catch (IOException exception) {
            throw new StorageException("Failed to close offset checkpoint: " + file, exception);
        }
    }
}
