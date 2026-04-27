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
    private static final int LENGTH_HEADER_SIZE = Integer.BYTES; // 레코드 앞에 붙는 길이 헤더 크기 (4 bytes)

    private final Path path; // 에러 메시지에 파일 경로를 포함하기 위해 보존
    private final FileHandle fileHandle; // positional read/write를 지원하는 NIO 채널. read(buf, pos)는 채널의 position을 변경하지 않아 thread-safe
    private final SegmentIndex index; // .idx 파일 핸들

    private Segment(Path path, FileHandle fileHandle, SegmentIndex index) {
        this.path = path;
        this.fileHandle = fileHandle;
        this.index = index;
    }

    static Segment openOrCreate(Path dir, long startOffset) { // startOffset으로 파일명을 결정하고 .mmm/.idx를 각각 열거나 생성
        String baseName = String.format(FILE_NAME_FORMAT, startOffset);
        Path segmentPath = dir.resolve(baseName + EXTENSION);
        try {
            FileHandle fileHandle = FileHandle.open(
                    segmentPath,
                    StandardOpenOption.CREATE,  // 파일이 없으면 새로 생성
                    StandardOpenOption.READ,    // recoverActiveSegment에서 읽기도 필요
                    StandardOpenOption.WRITE    // append 쓰기에 필요
            );
            SegmentIndex index = SegmentIndex.openOrCreate(dir, baseName);

            return new Segment(segmentPath, fileHandle, index);
        } catch (IOException exception) {
            throw new StorageException("Failed to open segment: " + segmentPath, exception);
        }
    }

    long count() {
        return index.count();
    }

    long size() { // 현재 .mmm 파일 크기. SegmentDirectory가 rotate 여부를 판단할 때 사용
        try {
            return fileHandle.size();
        } catch (IOException exception) {
            throw new StorageException("Failed to read size of segment: " + path, exception);
        }
    }

    void append(Message message) {
        byte[] payload = MessageCodec.encode(message);
        int length = payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(LENGTH_HEADER_SIZE + length);
        buffer.putInt(length);
        buffer.put(payload);

        try {
            long position = fileHandle.size(); // .mmm 쓰기 + fsync #1. 반환값은 record 시작 위치
            fileHandle.writeFully(position, ByteBuffer.wrap(buffer.array()), FlushMode.FSYNC);
            index.appendAndForce(position); // .idx 쓰기 + fsync #2. segment fsync 완료 후 실행해야 불변식 유지
        } catch (IOException exception) {
            throw new StorageException("Failed to append to segment: " + path, exception);
        }
    }

    @Nullable
    Message readAt(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative: " + offset);
        }
        if (offset >= index.count()) {
            return null;
        }
        long position = index.readPositionAt(offset); // .idx에서 .mmm 내 record 시작 위치 조회
        try {
            int payloadLength = ByteBuffer.wrap(fileHandle.readFully(position, LENGTH_HEADER_SIZE)).getInt();
            byte[] payload = fileHandle.readFully(position + LENGTH_HEADER_SIZE, payloadLength);

            return MessageCodec.decode(payload);
        } catch (IOException exception) {
            throw new StorageException("Failed to read from segment: " + path, exception);
        }
    }

    void recoverActiveSegment() { // 부팅 시 마지막 세그먼트의 정합성을 검사하고 미커밋 trailing bytes를 제거
        long entryCount = index.count();
        long actualSegmentSize = size();
        if (entryCount == 0) {
            if (actualSegmentSize > 0) {
                truncate(0);
            }
            return;
        }
        long lastIdxEntry = index.readPositionAt(entryCount - 1);
        if (lastIdxEntry + LENGTH_HEADER_SIZE > actualSegmentSize) {
            throw new StorageException(
                    "Index points beyond segment end. segmentSize=" + actualSegmentSize + ", lastEntry=" + lastIdxEntry
            );
        }
        byte[] header;
        try {
            header = fileHandle.readFully(lastIdxEntry, LENGTH_HEADER_SIZE);
        } catch (IOException exception) {
            throw new StorageException("Failed to read from segment: " + path, exception);
        }
        int payloadLength = ByteBuffer.wrap(header).getInt();
        long expectedSegmentEnd = lastIdxEntry + LENGTH_HEADER_SIZE + payloadLength;
        if (expectedSegmentEnd > actualSegmentSize) {
            throw new StorageException(
                    "Index points to incomplete record. segmentSize=" + actualSegmentSize
                            + ", lastEntry=" + lastIdxEntry + ", expectedEnd=" + expectedSegmentEnd
            );
        }
        if (actualSegmentSize > expectedSegmentEnd) {
            // MERINGUE: 애플리케이션이 마음대로 파일 내용을 조작하면 안됨.
            // 조작 없이 종료하도록 변경해야 함.
            // ~가 잘못되었으니 ~ 파일 내용 고쳐주세요 정도로 로그 남기고 종료하도록 변경해야 함.
            truncate(expectedSegmentEnd);
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

    private static final class MessageCodec {

        private static final ObjectMapper MAPPER = new ObjectMapper(); // ObjectMapper는 thread-safe이므로 공유 인스턴스 하나로 재사용

        private MessageCodec() {
        }

        static byte[] encode(Message message) throws StorageException {
            try {
                return MAPPER.writeValueAsBytes(message);
            } catch (Exception exception) {
                throw new StorageException("Failed to encode message: " + message, exception);
            }
        }

        static Message decode(byte[] payload) throws StorageException {
            try {
                return MAPPER.readValue(payload, Message.class);
            } catch (Exception exception) {
                throw new StorageException("Failed to decode message", exception);
            }
        }
    }
}
