package org.mmmq.broker.topicqueue.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;
import org.mmmq.broker.topicqueue.storage.FileHandle.FlushMode;
import org.mmmq.core.message.Message;

class SegmentFile implements Closeable {

    private static final String EXTENSION = ".mmm";
    private static final int OFFSET_DIGITS = Long.toString(Long.MAX_VALUE).length();

    private final Path path;
    private final long startOffset;
    private final FileHandle fileHandle;
    private final OffsetIndexFile offsetIndexFile;

    private SegmentFile(Path path, long startOffset, FileHandle fileHandle, OffsetIndexFile offsetIndexFile) {
        this.path = path;
        this.startOffset = startOffset;
        this.fileHandle = fileHandle;
        this.offsetIndexFile = offsetIndexFile;
        recover();
    }

    static List<SegmentFile> openAll(Path base) {
        try (Stream<Path> entries = Files.list(base)) {
            return entries
                    .filter(Files::isRegularFile)
                    .map(file -> file.getFileName().toString())
                    .filter(fileName -> fileName.endsWith(EXTENSION))
                    .filter(fileName -> fileName.length() == OFFSET_DIGITS + EXTENSION.length())
                    .map(fileName -> fileName.substring(0, OFFSET_DIGITS))
                    .filter(digits -> digits.chars().allMatch(Character::isDigit))
                    .map(Long::parseLong)
                    .map(startOffset -> open(base, startOffset))
                    .toList();
        } catch (IOException exception) {
            throw new StorageException("Failed to list segments in: " + base, exception);
        }
    }

    static SegmentFile open(Path base, long startOffset) {
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
            OffsetIndexFile offsetIndexFile = OffsetIndexFile.open(base, startOffset);
            return new SegmentFile(path, startOffset, fileHandle, offsetIndexFile);
        } catch (IOException exception) {
            throw new StorageException("Failed to open segment: " + path, exception);
        }
    }

    private void recover() {
        try {
            long indexEntryCount = count();
            long fileSize = fileHandle.size();
            if (indexEntryCount == 0) {
                if (fileSize > 0) {
                    fileHandle.truncate(0, FlushMode.FSYNC);
                }
                return;
            }
            long lastAddress = offsetIndexFile.readAt(indexEntryCount - 1);
            Entry lastEntry = Entry.readFrom(fileHandle, lastAddress);
            long expectedFileSize = lastAddress + lastEntry.size();
            if (expectedFileSize > fileSize) {
                throw new StorageException(
                        "Last entry is truncated: " + path
                                + ", entryStart=" + lastAddress + ", requiredEnd=" + expectedFileSize
                                + ", actualSize=" + fileSize
                );
            }
            if (fileSize > expectedFileSize) {
                fileHandle.truncate(expectedFileSize, FlushMode.FSYNC);
            }
        } catch (IOException exception) {
            throw new StorageException("Failed to recover segment: " + path, exception);
        }
    }

    void append(Message message) {
        Entry entry = Entry.from(message);
        try {
            long address = fileHandle.appendFully(entry.toByteBuffer(), FlushMode.FSYNC);
            offsetIndexFile.append(address);
        } catch (IOException exception) {
            throw new StorageException("Failed to append to segment: " + path, exception);
        }
    }

    @Nullable
    Message readAt(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative: " + offset);
        }
        if (offset >= count()) {
            return null;
        }
        long address = offsetIndexFile.readAt(offset);
        try {
            return Entry.readFrom(fileHandle, address).toMessage();
        } catch (IOException exception) {
            throw new StorageException("Failed to read from segment: " + path, exception);
        }
    }

    long startOffset() {
        return startOffset;
    }

    long count() {
        return offsetIndexFile.count();
    }

    boolean reaches(long size) {
        try {
            return fileHandle.size() >= size;
        } catch (IOException exception) {
            throw new StorageException("Failed to read size of segment: " + path, exception);
        }
    }

    @Override
    public void close() {
        try {
            fileHandle.close();
        } catch (IOException exception) {
            throw new StorageException("Failed to close segment: " + path, exception);
        }
        offsetIndexFile.close();
    }

    private record Entry(
            byte[] messageBytes
    ) {

        private static final int LENGTH_HEADER_SIZE = Integer.BYTES;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        Entry {
            if (messageBytes == null) {
                throw new IllegalArgumentException("messageBytes must not be null");
            }
        }

        static Entry from(Message message) {
            return new Entry(encode(message));
        }

        static Entry readFrom(FileHandle fileHandle, long address) throws IOException {
            int length = ByteBuffer.wrap(fileHandle.readFully(address, LENGTH_HEADER_SIZE)).getInt();
            return new Entry(fileHandle.readFully(address + LENGTH_HEADER_SIZE, length));
        }

        private static byte[] encode(Message message) {
            try {
                return MAPPER.writeValueAsBytes(message);
            } catch (JsonProcessingException exception) {
                throw new StorageException("Failed to encode message: " + message, exception);
            }
        }

        private static Message decode(byte[] messageBytes) {
            try {
                return MAPPER.readValue(messageBytes, Message.class);
            } catch (IOException exception) {
                throw new StorageException("Failed to decode message", exception);
            }
        }

        ByteBuffer toByteBuffer() {
            ByteBuffer buffer = ByteBuffer.allocate(size());
            buffer.putInt(messageBytes.length).put(messageBytes).flip();
            return buffer;
        }

        Message toMessage() {
            return decode(messageBytes);
        }

        int size() {
            return LENGTH_HEADER_SIZE + messageBytes.length;
        }
    }
}
