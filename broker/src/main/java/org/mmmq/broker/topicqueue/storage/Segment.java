package org.mmmq.broker.topicqueue.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class Segment implements Closeable { // .mmm 파일 한 개를 캡슐화. position 기반 read/write만 지원하며 직렬화 포맷이나 레코드 구획은 모름

    private final Path path; // 에러 메시지에 파일 경로를 포함하기 위해 보존
    private final FileChannel channel; // positional read/write를 지원하는 NIO 채널. channel.read(buf, pos)는 채널의 position을 변경하지 않아 thread-safe

    private Segment(Path path, FileChannel channel) { // 외부에서 직접 생성하지 않도록 생성자를 private으로 제한
        this.path = path;
        this.channel = channel;
    }

    static Segment openOrCreate(Path path) { // 파일이 없으면 생성, 있으면 기존 내용을 유지하며 열기
        try {
            final FileChannel channel = FileChannel.open(
                    path,
                    StandardOpenOption.CREATE,  // 파일이 없으면 새로 생성
                    StandardOpenOption.READ,    // recoverActiveSegment에서 읽기도 필요
                    StandardOpenOption.WRITE    // append 쓰기에 필요
            );

            return new Segment(path, channel);
        } catch (IOException exception) {
            throw new StorageException("Failed to open segment: " + path, exception);
        }
    }

    long appendAndForce(byte[] payload) { // payload를 파일 끝에 쓰고 fsync를 완료한 뒤 기록 시작 위치를 반환
        try {
            final long position = channel.size(); // 현재 파일 끝 = 이번 쓰기가 시작될 위치. SegmentIndex에 저장될 값
            final ByteBuffer buffer = ByteBuffer.wrap(payload); // 배열을 직접 래핑해 불필요한 복사 방지
            long writePosition = position; // partial write 가능성이 있어 루프로 전체 bytes를 모두 쓸 때까지 반복
            while (buffer.hasRemaining()) {
                writePosition += channel.write(buffer, writePosition); // positional write: 채널의 현재 position에 영향 없음
            }
            channel.force(true); // fsync #1: 메타데이터 포함 디스크에 강제 플러시. SegmentIndex.force 전에 반드시 완료되어야 함

            return position; // 이 엔트리가 시작된 파일 위치를 반환. SegmentIndex의 엔트리 값으로 저장됨
        } catch (IOException exception) {
            throw new StorageException("Failed to append to segment: " + path, exception);
        }
    }

    byte[] readAt(long position, int length) { // position부터 정확히 length bytes를 읽어 반환
        try {
            final long fileSize = channel.size();
            if (position < 0 || length < 0 || position + length > fileSize) { // 유효 범위 밖 read는 즉시 실패
                throw new StorageException(
                        "Read range out of bounds: position=" + position + ", length=" + length
                                + ", fileSize=" + fileSize + " for segment: " + path
                );
            }
            final ByteBuffer buffer = ByteBuffer.allocate(length); // 정확한 크기만 선행 할당
            int totalRead = 0; // partial read 가능성이 있어 length 바이트 전부 채울 때까지 반복
            while (totalRead < length) {
                final int read = channel.read(buffer, position + totalRead); // positional read: 채널 position 변경 없이 읽음
                if (read <= 0) { // 위에서 범위 검사를 했으므로 EOF가 나오면 안 됨
                    throw new StorageException(
                            "Unexpected EOF while reading segment: position=" + position
                                    + ", length=" + length + ", read=" + totalRead + " for segment: " + path
                    );
                }
                totalRead += read;
            }

            return buffer.array(); // allocate된 backing array는 정확히 length 크기
        } catch (IOException exception) {
            throw new StorageException("Failed to read from segment: " + path, exception);
        }
    }

    long size() { // 현재 파일 크기를 반환. shouldRotate 판단과 recoverActiveSegment에서 사용
        try {
            return channel.size();
        } catch (IOException exception) {
            throw new StorageException("Failed to read size of segment: " + path, exception);
        }
    }

    void truncate(long newSize) { // 파일을 newSize bytes로 잘라내고 fsync. 부팅 복구 시 미커밋 trailing bytes 제거에 사용
        try {
            channel.truncate(newSize); // newSize 이후 bytes를 제거
            channel.force(true); // truncate 후 즉시 fsync해서 디스크에 반영
        } catch (IOException exception) {
            throw new StorageException("Failed to truncate segment: " + path, exception);
        }
    }

    @Override
    public void close() { // 사용 완료 후 FileChannel을 닫아 fd 누수 방지
        try {
            channel.close();
        } catch (IOException exception) {
            throw new StorageException("Failed to close segment: " + path, exception);
        }
    }
}
