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

final class SegmentFile implements Closeable {

    private static final String EXTENSION = ".mmm";
    private static final int OFFSET_DIGITS = Long.toString(Long.MAX_VALUE).length();

    private final Path path; // 에러 메시지에 파일 경로를 포함하기 위해 보존
    private final long startOffset; // 이 segment의 첫 메시지의 absolute offset. 파일명에 인코딩된 식별자
    private final FileHandle fileHandle; // positional read/write를 지원하는 NIO 채널. read(buf, pos)는 채널의 position을 변경하지 않아 thread-safe
    private final OffsetIndexFile offsetIndexFile; // .idx 파일 핸들

    private SegmentFile(Path path, long startOffset, FileHandle fileHandle, OffsetIndexFile offsetIndexFile) {
        this.path = path;
        this.startOffset = startOffset;
        this.fileHandle = fileHandle;
        this.offsetIndexFile = offsetIndexFile;
        recover();
    }

    static List<SegmentFile> openAll(Path base) { // base 내의 모든 segment 파일을 스캔해 열어 반환. 식별자(파일명) 컨벤션을 Segment 안에 캡슐화
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
                    StandardOpenOption.CREATE,  // 파일이 없으면 새로 생성
                    StandardOpenOption.READ,    // recover에서 읽기도 필요
                    StandardOpenOption.WRITE    // append 쓰기에 필요
            );
            OffsetIndexFile offsetIndexFile = OffsetIndexFile.open(base, startOffset);
            return new SegmentFile(path, startOffset, fileHandle,
                    offsetIndexFile); // 생성자가 recover까지 수행해 즉시 사용 가능 상태로 반환
        } catch (IOException exception) {
            throw new StorageException("Failed to open segment: " + path, exception);
        }
    }


    /**
     * 진실의 원천 = OffsetIndexFile 이지? -> OffsetIndexFile에 있으면 SegmentFile에도 있어야 한다. -> OffsetIndexFile에 없으면 SegmentFile에도
     * 없어야 한다.
     * <p>
     * -> OffsetIndexFile에 없는데, SegmentFile에 있다면? 누가 잘못된거지? -> SegmentFile이 잘못된 것이다.
     * <p>
     * -> OffsetIndexFile에 있는데, SegmentFile에 없다면? 누가 잘못된거지? -> SegmentFile이 잘못된 것이다.
     * <p>
     * 왜? OffsetIndexFile이 진실의 원천이니까. -> OffsetIndexFile에 있으면 SegmentFile에도 있어야 한다. -> OffsetIndexFile에 없으면
     * SegmentFile에도 없어야 한다.
     */
    private void recover() { // 생성자에서만 호출. 마지막 세그먼트의 정합성을 검사하고 미커밋 trailing bytes를 제거
        try {
            long indexEntryCount = count(); // OffsetIndex.count() == 총 메시지 수
            long fileSize = fileHandle.size(); // 내 .mmm 파일 크기
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
            long address = fileHandle.appendFully(entry.toByteBuffer(), FlushMode.FSYNC); // .mmm 쓰기 + fsync #1
            offsetIndexFile.append(address); // .idx 쓰기 + fsync #2. segment fsync 완료 후 실행해야 불변식 유지
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
        long address = offsetIndexFile.readAt(offset); // .idx에서 .mmm 내 entry 시작 주소 조회
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

        // 성능?
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

        int size() { // disk 상의 entry 전체 크기: 길이 헤더 + messageBytes
            return LENGTH_HEADER_SIZE + messageBytes.length;
        }
    }
}
