package org.mmmq.broker.topicqueue.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.mmmq.broker.topicqueue.storage.FileChannels.FlushMode;

public final class OffsetStore implements Closeable { // dispatcher 이름별 읽기 위치(offset)를 8바이트 long으로 파일에 영속화

    private static final int VALUE_BYTES = Long.BYTES; // offset 값의 크기: long = 8 bytes
    private static final String EXTENSION = ".offset";

    private final Path file; // data/{topic}/offsets/{dispatcherName}.offset 경로
    private final FileChannel channel; // positional read/write를 지원하는 NIO 채널

    private OffsetStore(Path file, FileChannel channel) { // 외부에서 직접 생성하지 않도록 private
        this.file = file;
        this.channel = channel;
    }

    public static OffsetStore openOrCreate(Path base, String dispatcherName) {
        Path file = base.resolve(dispatcherName + EXTENSION);
        try {
            Files.createDirectories(base);
            FileChannel channel = FileChannel.open(
                    file,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );
            OffsetStore store = new OffsetStore(file, channel);
            // MOKO: Dispatcher 추가 시 처음부터 시작할지, 최신부터 시작할지 옵션 고려.
            if (channel.size() == 0) {
                store.write(0L);
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
            return ByteBuffer.wrap(FileChannels.readFully(channel, 0, VALUE_BYTES)).getLong();
        } catch (IOException exception) {
            throw new StorageException("Failed to read offset store: " + file, exception);
        }
    }

    public void write(long value) { // value를 파일에 덮어쓰고 fsync. at-least-once의 commit 지점: 이 호출 완료 후에만 offset이 전진
        try {
            ByteBuffer buffer = ByteBuffer.allocate(VALUE_BYTES);
            buffer.putLong(value);
            buffer.flip();
            FileChannels.writeFully(channel, 0, buffer, FlushMode.FSYNC); // offset 파일은 항상 위치 0에 덮어씀. 파일 크기는 항상 8바이트
        } catch (IOException exception) {
            throw new StorageException("Failed to write offset: " + file, exception);
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
