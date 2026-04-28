package org.mmmq.broker.topicqueue.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.mmmq.broker.topicqueue.storage.FileHandle.FlushMode;
import org.mmmq.core.message.Message;

final class Segment implements Closeable {

    private static final String EXTENSION = ".mmm";
    private static final String FILE_NAME_FORMAT = "segment-%020d"; // 20자리 zero-padding: lexicographic 정렬 = numeric 정렬 보장

    private final Path path; // 에러 메시지에 파일 경로를 포함하기 위해 보존
    private final FileHandle fileHandle; // positional read/write를 지원하는 NIO 채널. read(buf, pos)는 채널의 position을 변경하지 않아 thread-safe
    private final SegmentIndex index; // .idx 파일 핸들

    private Segment(Path path, FileHandle fileHandle, SegmentIndex index) {
        this.path = path;
        this.fileHandle = fileHandle;
        this.index = index;
        recover();
    }

    static Segment open(Path dir,
                        long startOffset) { // startOffset으로 파일명을 결정하고 .mmm/.idx를 각각 열거나 생성. 반환된 segment는 즉시 사용 가능한 일관된 상태
        String baseName = String.format(FILE_NAME_FORMAT, startOffset);
        Path segmentPath = dir.resolve(baseName + EXTENSION);
        try {
            FileHandle fileHandle = FileHandle.open(
                    segmentPath,
                    StandardOpenOption.CREATE,  // 파일이 없으면 새로 생성
                    StandardOpenOption.READ,    // recover에서 읽기도 필요
                    StandardOpenOption.WRITE    // append 쓰기에 필요
            );
            SegmentIndex index = SegmentIndex.open(dir, baseName);
            return new Segment(segmentPath, fileHandle, index); // 생성자가 recover까지 수행해 즉시 사용 가능 상태로 반환
        } catch (IOException exception) {
            throw new StorageException("Failed to open segment: " + segmentPath, exception);
        }
    }

    long count() {
        return index.count();
    }

    boolean reaches(long size) {
        try {
            return fileHandle.size() >= size;
        } catch (IOException exception) {
            throw new StorageException("Failed to read size of segment: " + path, exception);
        }
    }

    void append(Message message) {
        Entry entry = Entry.from(message);
        try {
            long address = fileHandle.appendFully(entry.toBuffer(), FlushMode.FSYNC); // .mmm 쓰기 + fsync #1
            index.appendAndForce(address); // .idx 쓰기 + fsync #2. segment fsync 완료 후 실행해야 불변식 유지
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
        long address = index.readAddressAt(offset); // .idx에서 .mmm 내 entry 시작 주소 조회
        try {
            return Entry.readFrom(fileHandle, address).toMessage();
        } catch (IOException exception) {
            throw new StorageException("Failed to read from segment: " + path, exception);
        }
    }

    private void recover() { // 생성자에서만 호출. 마지막 세그먼트의 정합성을 검사하고 미커밋 trailing bytes를 제거
        try {
            long entryCount = count();
            long segmentSize = fileHandle.size();
            if (entryCount == 0) {
                if (segmentSize > 0) {
                    truncate(0);
                }
                return;
            }
            long lastAddress = index.readAddressAt(entryCount - 1);
            Entry lastEntry = Entry.readFrom(fileHandle, lastAddress);
            long expectedSegmentSize = lastAddress + lastEntry.size();
            if (expectedSegmentSize > segmentSize) {
                throw new StorageException(
                        "Last entry is truncated: " + path
                                + ", entryStart=" + lastAddress + ", requiredEnd=" + expectedSegmentSize
                                + ", actualSize=" + segmentSize
                );
            }
            if (segmentSize > expectedSegmentSize) {
                truncate(expectedSegmentSize);
            }
        } catch (IOException exception) {
            throw new StorageException("Failed to recover segment: " + path, exception);
        }
    }

    private void truncate(long newSize) { // 파일을 newSize bytes로 잘라내고 fsync. 부팅 복구 시 미커밋 trailing bytes 제거에 사용
        try {
            fileHandle.truncate(newSize, FlushMode.FSYNC);
        } catch (IOException exception) {
            throw new StorageException("Failed to truncate segment: " + path, exception);
        }
    }

    @Override
    public void close() {
        try {
            fileHandle.close();
        } catch (IOException exception) {
            throw new StorageException("Failed to close segment: " + path, exception);
        }
        index.close();
    }

    private record Entry(
            byte[] messageBytes
    ) {

        private static final int LENGTH_HEADER_SIZE = Integer.BYTES; // entry 앞에 붙는 길이 헤더 크기 (4 bytes)
        private static final ObjectMapper MAPPER = new ObjectMapper(); // ObjectMapper는 thread-safe이므로 공유 인스턴스 하나로 재사용

        Entry {
            if (messageBytes == null) {
                throw new IllegalArgumentException("messageBytes must not be null");
            }
        }

        static Entry from(Message message) {
            return new Entry(encode(message));
        }

        static Entry readFrom(FileHandle file, long address) throws IOException {
            int length = ByteBuffer.wrap(file.readFully(address, LENGTH_HEADER_SIZE)).getInt();
            return new Entry(file.readFully(address + LENGTH_HEADER_SIZE, length));
        }

        private static byte[] encode(Message message) {
            try {
                return MAPPER.writeValueAsBytes(message);
            } catch (Exception exception) {
                throw new StorageException("Failed to encode message: " + message, exception);
            }
        }

        private static Message decode(byte[] messageBytes) {
            try {
                return MAPPER.readValue(messageBytes, Message.class);
            } catch (Exception exception) {
                throw new StorageException("Failed to decode message", exception);
            }
        }

        ByteBuffer toBuffer() {
            ByteBuffer buffer = ByteBuffer.allocate(size());
            buffer.putInt(messageBytes.length);
            buffer.put(messageBytes);
            buffer.flip();

            return buffer;
        }

        Message toMessage() {
            return decode(messageBytes);
        }

        int size() { // disk 상의 entry 전체 크기: 길이 헤더 + messageBytes
            return LENGTH_HEADER_SIZE + messageBytes.length;
        }
    }
}
