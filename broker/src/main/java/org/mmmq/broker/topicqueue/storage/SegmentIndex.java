package org.mmmq.broker.topicqueue.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class SegmentIndex implements Closeable { // .idx 파일 한 개를 캡슐화. 각 엔트리는 8바이트 long으로 .log 파일 내 위치를 저장

    private static final int LOG_POSITION_BYTES = Long.BYTES; // .log 파일 내 물리 주소를 저장하는 크기: long = 8 bytes

    private final Path path; // 에러 메시지에 파일 경로를 포함하기 위해 보존
    private final FileChannel channel; // positional read/write를 지원하는 NIO 채널

    private SegmentIndex(Path path, FileChannel channel) { // 외부에서 직접 생성하지 않도록 생성자를 private으로 제한
        this.path = path;
        this.channel = channel;
    }

    static SegmentIndex openOrCreate(Path path) { // 파일이 없으면 생성, 있으면 기존 엔트리를 유지하며 열기
        try {
            final FileChannel channel = FileChannel.open(
                    path,
                    StandardOpenOption.CREATE,  // 파일이 없으면 새로 생성
                    StandardOpenOption.READ,    // readPositionAt, entryCount에 필요
                    StandardOpenOption.WRITE    // appendAndForce에 필요
            );

            return new SegmentIndex(path, channel);
        } catch (IOException exception) {
            throw new StorageException("Failed to open segment index: " + path, exception);
        }
    }

    void appendAndForce(long logPosition) { // logPosition(8 bytes)를 파일 끝에 쓰고 fsync. SegmentLog.force 후에 호출해야 함
        try {
            final ByteBuffer buffer = ByteBuffer.allocate(LOG_POSITION_BYTES); // 8바이트 버퍼 할당
            buffer.putLong(logPosition); // big-endian으로 long 값을 버퍼에 기록
            buffer.flip(); // write 모드 → read 모드 전환
            long writePosition = channel.size(); // 현재 파일 끝에 이어서 씀
            while (buffer.hasRemaining()) {
                writePosition += channel.write(buffer, writePosition); // partial write 방어: 8바이트 모두 쓸 때까지 반복
            }
            channel.force(true); // fsync #2: .log fsync 이후에 실행되어야 불변식(log >= idx)이 성립
        } catch (IOException exception) {
            throw new StorageException("Failed to append to segment index: " + path, exception);
        }
    }

    long readPositionAt(int relativeOffset) { // relativeOffset 번째 엔트리의 .log 파일 위치를 반환
        try {
            final long startPosition = (long) relativeOffset * LOG_POSITION_BYTES; // 인덱스 파일 내 해당 엔트리의 바이트 위치
            final ByteBuffer buffer = ByteBuffer.allocate(LOG_POSITION_BYTES);
            int totalRead = 0;
            while (totalRead < LOG_POSITION_BYTES) { // 8바이트 미만으로 읽힌 경우(partial read) 나머지를 다시 읽음
                final int read = channel.read(buffer, startPosition + totalRead); // positional read: 채널 position 변경 없음
                if (read <= 0) { // EOF에 도달했는데 8바이트를 다 못 읽었으면 인덱스 손상
                    throw new StorageException("Failed to read full index entry at offset: " + relativeOffset);
                }
                totalRead += read;
            }
            buffer.flip(); // write 모드 → read 모드 전환

            return buffer.getLong(); // big-endian long 값을 .log 파일 position으로 반환
        } catch (IOException exception) {
            throw new StorageException("Failed to read segment index: " + path, exception);
        }
    }

    long count() { // 현재 인덱스에 기록된 엔트리 수를 반환. 파일 크기 ÷ 8 = 커밋된 메시지 수
        try {
            return channel.size() / LOG_POSITION_BYTES; // 8바이트 단위이므로 정확히 나누어 떨어짐
        } catch (IOException exception) {
            throw new StorageException("Failed to read size of segment index: " + path, exception);
        }
    }

    @Override
    public void close() { // 사용 완료 후 FileChannel을 닫아 fd 누수 방지
        try {
            channel.close();
        } catch (IOException exception) {
            throw new StorageException("Failed to close segment index: " + path, exception);
        }
    }
}
