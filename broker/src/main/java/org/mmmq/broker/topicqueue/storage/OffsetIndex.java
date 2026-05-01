package org.mmmq.broker.topicqueue.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.mmmq.broker.topicqueue.storage.FileHandle.FlushMode;

final class OffsetIndex implements Closeable { // .idx 파일 한 개를 캡슐화. 각 엔트리는 8바이트 long으로 .mmm 파일 내 주소를 저장

    private static final String EXTENSION = ".idx";
    private static final int ENTRY_BYTES = Long.BYTES; // 엔트리 1개의 크기. 각 엔트리는 .mmm 파일 내 address를 담는 long 1개
    private static final int OFFSET_DIGITS = Long.toString(Long.MAX_VALUE).length();

    private final Path path; // 에러 메시지에 파일 경로를 포함하기 위해 보존
    private final FileHandle fileHandle; // positional read/write를 지원하는 NIO 채널

    private OffsetIndex(Path path, FileHandle fileHandle) { // 외부에서 직접 생성하지 않도록 생성자를 private으로 제한
        this.path = path;
        this.fileHandle = fileHandle;
    }

    static OffsetIndex open(Path base, long startOffset) { // 파일이 없으면 생성, 있으면 기존 엔트리를 유지하며 열기
        Path path = base.resolve(
                "0".repeat(OFFSET_DIGITS - Long.toString(startOffset).length()) + startOffset + EXTENSION
        );
        try {
            FileHandle fileHandle = FileHandle.open(
                    path,
                    StandardOpenOption.CREATE,  // 파일이 없으면 새로 생성
                    StandardOpenOption.READ,    // readAddressAt, count에 필요
                    StandardOpenOption.WRITE    // appendAndForce에 필요
            );
            return new OffsetIndex(path, fileHandle);
        } catch (IOException exception) {
            throw new StorageException("Failed to open startOffset index: " + path, exception);
        }
    }

    void append(long address) { // address(8 bytes)를 파일 끝에 쓰고 fsync. Segment.force 후에 호출해야 함
        try {
            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_BYTES);
            buffer.putLong(address).flip();
            fileHandle.appendFully(buffer, FlushMode.FSYNC);
        } catch (IOException exception) {
            throw new StorageException("Failed to append to offset index: " + path, exception);
        }
    }

    long readAt(long offset) { // offset 번째 엔트리의 .mmm 파일 주소를 반환
        try {
            return ByteBuffer.wrap(fileHandle.readFully(offset * ENTRY_BYTES, ENTRY_BYTES)).getLong();
        } catch (IOException exception) {
            throw new StorageException("Failed to read offset index: " + path, exception);
        }
    }

    long count() { // 현재 인덱스에 기록된 엔트리 수를 반환. 파일 크기 ÷ 8 = 커밋된 메시지 수
        try {
            return fileHandle.size() / ENTRY_BYTES; // 8바이트 단위이므로 정확히 나누어 떨어짐
        } catch (IOException exception) {
            throw new StorageException("Failed to read size of offset index: " + path, exception);
        }
    }

    @Override
    public void close() { // 사용 완료 후 fd 누수 방지
        try {
            fileHandle.close();
        } catch (IOException exception) {
            throw new StorageException("Failed to close offset index: " + path, exception);
        }
    }
}
