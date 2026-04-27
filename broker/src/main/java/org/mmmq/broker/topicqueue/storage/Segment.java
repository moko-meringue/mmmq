package org.mmmq.broker.topicqueue.storage;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class Segment implements Closeable { // .mmm 파일 한 개를 캡슐화. append-only 쓰기와 position 기반 읽기만 허용

    private static final byte LINE_TERMINATOR = (byte) '\n'; // 엔트리 경계 식별에 사용하는 줄 구분자
    private static final int READ_CHUNK_SIZE = 4096; // 한 번에 읽는 버퍼 크기: 시스템 콜 횟수를 줄이기 위해 4KB 단위로 읽음

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

    byte[] readLineAt(long position) { // position부터 '\n'까지 bytes를 읽어 반환. '\n' 포함
        try {
            final long fileSize = channel.size();
            if (position < 0 || position >= fileSize) { // 유효 범위 밖 위치 접근은 즉시 실패
                throw new StorageException("Read position out of bounds: " + position + " for segment: " + path);
            }
            final ByteArrayOutputStream output = new ByteArrayOutputStream(); // 크기를 미리 알 수 없어 동적으로 누적
            final ByteBuffer buffer = ByteBuffer.allocate(READ_CHUNK_SIZE); // 매번 새 버퍼를 할당하지 않고 재사용
            long readPosition = position;
            while (readPosition < fileSize) {
                buffer.clear(); // 이전 읽기 결과를 지우고 버퍼를 초기 상태로 리셋
                final int read = channel.read(buffer, readPosition); // positional read: 채널 position 변경 없이 읽음
                if (read <= 0) { // EOF이거나 읽을 데이터가 없으면 루프 종료
                    break;
                }
                buffer.flip(); // write 모드 → read 모드 전환
                while (buffer.hasRemaining()) {
                    final byte current = buffer.get();
                    output.write(current); // 읽은 바이트를 누적 버퍼에 추가
                    readPosition++;
                    if (current == LINE_TERMINATOR) { // '\n'을 만나면 하나의 엔트리를 다 읽은 것
                        return output.toByteArray();
                    }
                }
            }

            return output.toByteArray(); // '\n' 없이 EOF에 도달한 경우: 부팅 복구 중 잘린 라인
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
