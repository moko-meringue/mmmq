package org.mmmq.broker.topicqueue.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.mmmq.broker.topicqueue.storage.FileHandle.FlushMode;

class OffsetIndexFile implements Closeable {

    private static final String EXTENSION = ".idx";
    private static final int ENTRY_BYTES = Long.BYTES;
    private static final int OFFSET_DIGITS = Long.toString(Long.MAX_VALUE).length();

    private final Path path;
    private final FileHandle fileHandle;

    private OffsetIndexFile(Path path, FileHandle fileHandle) {
        this.path = path;
        this.fileHandle = fileHandle;
    }

    static OffsetIndexFile open(Path base, long startOffset) {
        Path path = base.resolve(
                "0".repeat(OFFSET_DIGITS - Long.toString(startOffset).length()) + startOffset + EXTENSION
        );
        try {
            FileHandle fileHandle = FileHandle.open(
                    path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );
            return new OffsetIndexFile(path, fileHandle);
        } catch (IOException exception) {
            throw new StorageException("Failed to open startOffset index: " + path, exception);
        }
    }

    void append(long address) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_BYTES);
            buffer.putLong(address).flip();
            fileHandle.appendFully(buffer, FlushMode.FSYNC);
        } catch (IOException exception) {
            throw new StorageException("Failed to append to offset index: " + path, exception);
        }
    }

    long readAt(long offset) {
        try {
            return ByteBuffer.wrap(fileHandle.readFully(offset * ENTRY_BYTES, ENTRY_BYTES)).getLong();
        } catch (IOException exception) {
            throw new StorageException("Failed to read offset index: " + path, exception);
        }
    }

    long count() {
        try {
            return fileHandle.size() / ENTRY_BYTES;
        } catch (IOException exception) {
            throw new StorageException("Failed to read size of offset index: " + path, exception);
        }
    }

    @Override
    public void close() {
        try {
            fileHandle.close();
        } catch (IOException exception) {
            throw new StorageException("Failed to close offset index: " + path, exception);
        }
    }
}
