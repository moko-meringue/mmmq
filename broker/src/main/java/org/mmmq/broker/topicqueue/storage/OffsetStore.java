package org.mmmq.broker.topicqueue.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class OffsetStore implements Closeable { // dispatcher 이름별 읽기 위치(offset)를 8바이트 long으로 파일에 영속화

    private static final int VALUE_BYTES = Long.BYTES; // offset 값의 크기: long = 8 bytes

    private final Path file; // data/{topic}/offsets/{dispatcherName}.offset 경로
    private final FileChannel channel; // positional read/write를 지원하는 NIO 채널

    private OffsetStore(Path file, FileChannel channel) { // 외부에서 직접 생성하지 않도록 private
        this.file = file;
        this.channel = channel;
    }

    public static OffsetStore openOrCreate(Path file) { // 파일이 없으면 생성해 0을 초기값으로 기록, 있으면 기존 값을 유지하며 열기
        try {
            Files.createDirectories(file.getParent()); // offsets/ 디렉토리가 없으면 생성
            final boolean exists = Files.exists(file); // 기존 파일 여부를 열기 전에 확인
            final FileChannel channel = FileChannel.open(
                    file,
                    StandardOpenOption.CREATE,  // 파일이 없으면 생성
                    StandardOpenOption.READ,    // read()에 필요
                    StandardOpenOption.WRITE    // writeAndForce()에 필요
            );
            final OffsetStore store = new OffsetStore(file, channel);
            // MOKO: Dispatcher 추가 시 처음부터 시작할지, 최신부터 시작할지 옵션 고려.
            if (!exists || channel.size() == 0) { // 새 파일이거나 내용이 없으면 초기값 0을 기록
                store.writeAndForce(0L); // 첫 쓰기 + fsync: 신규 dispatcher의 시작 offset은 0
            }

            return store;
        } catch (IOException exception) {
            throw new StorageException("Failed to open offset store: " + file, exception);
        }
    }

    public long read() { // 파일에서 8바이트를 읽어 dispatcher의 마지막 커밋 offset을 반환
        try {
            if (channel.size() < VALUE_BYTES) { // 8바이트 미만: 파일이 손상됐거나 외부에서 변조됨
                throw new StorageException("Offset store is corrupted: " + file);
            }
            final ByteBuffer buffer = ByteBuffer.allocate(VALUE_BYTES);
            int totalRead = 0;
            while (totalRead < VALUE_BYTES) { // partial read 방어: 8바이트 모두 읽을 때까지 반복
                final int read = channel.read(buffer, totalRead); // positional read: 채널 position 변경 없음
                if (read <= 0) { // EOF인데 8바이트를 다 못 읽었으면 파일 손상
                    throw new StorageException("Failed to read full offset value: " + file);
                }
                totalRead += read;
            }
            buffer.flip(); // write 모드 → read 모드 전환

            return buffer.getLong(); // big-endian으로 저장된 long 값을 offset으로 반환
        } catch (IOException exception) {
            throw new StorageException("Failed to read offset store: " + file, exception);
        }
    }

    public void writeAndForce(long value) { // value를 파일에 덮어쓰고 fsync. at-least-once의 commit 지점: 이 호출 완료 후에만 offset이 전진
        try {
            final ByteBuffer buffer = ByteBuffer.allocate(VALUE_BYTES);
            buffer.putLong(value); // big-endian으로 long 값을 버퍼에 기록
            buffer.flip(); // write 모드 → read 모드 전환
            long writePosition = 0; // offset 파일은 항상 위치 0에 덮어씀. 파일 크기는 항상 8바이트
            while (buffer.hasRemaining()) {
                writePosition += channel.write(buffer, writePosition); // partial write 방어
            }
            channel.force(true); // fsync #3: 이 시점부터 브로커 재시작 시 이 offset부터 재개
        } catch (IOException exception) {
            throw new StorageException("Failed to write offset store: " + file, exception);
        }
    }

    @Override
    public void close() { // 사용 완료 후 FileChannel을 닫아 fd 누수 방지
        try {
            channel.close();
        } catch (IOException exception) {
            throw new StorageException("Failed to close offset store: " + file, exception);
        }
    }
}
