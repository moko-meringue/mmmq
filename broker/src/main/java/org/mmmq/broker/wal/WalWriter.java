package org.mmmq.broker.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.mmmq.broker.wal.flush.WalFlushPolicy;

// 토픽 하나의 WAL 파일을 관리한다.
// SegmentChain과 1:1 대응하며, 세그먼트 인덱스가 바뀔 때마다 파일을 교체(rotate)한다.
public class WalWriter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final byte[] NEWLINE = new byte[]{'\n'};

    private final Path walDir;
    private final String topicName;
    private final WalFlushPolicy flushPolicy;

    // FileChannel을 매 write마다 열고 닫으면 시스템 콜 비용이 크므로
    // tail 세그먼트에 해당하는 채널을 재사용한다.
    @Nullable
    private FileChannel currentChannel;
    private int currentSegmentIndex = -1;

    public WalWriter(Path walDir, String topicName, WalFlushPolicy flushPolicy) {
        this.walDir = walDir;
        this.topicName = topicName;
        this.flushPolicy = flushPolicy;
    }

    // write와 deleteSegmentFile 모두 SegmentChain의 lock 범위 밖에서 호출될 수 있으므로
    // 채널 상태를 보호하기 위해 synchronized로 선언한다.
    public synchronized void write(WalEntry entry, int segmentIndex) {
        try {
            ensureChannelFor(segmentIndex);
            byte[] payload = MAPPER.writeValueAsBytes(entry);
            ByteBuffer buffer = ByteBuffer.allocate(payload.length + NEWLINE.length);
            buffer.put(payload);
            buffer.put(NEWLINE);
            buffer.flip();
            while (buffer.hasRemaining()) {
                currentChannel.write(buffer);
            }
            flushPolicy.flush(currentChannel);
        } catch (IOException exception) {
            throw new RuntimeException("Failed to write WAL entry", exception);
        }
    }

    public synchronized void deleteSegmentFile(int segmentIndex) {
        try {
            if (segmentIndex == currentSegmentIndex && currentChannel != null) {
                currentChannel.close();
                currentChannel = null;
                currentSegmentIndex = -1;
            }
            Files.deleteIfExists(segmentFilePath(segmentIndex));
        } catch (IOException exception) {
            throw new RuntimeException("Failed to delete WAL segment file", exception);
        }
    }

    private void ensureChannelFor(int segmentIndex) throws IOException {
        if (segmentIndex == currentSegmentIndex && currentChannel != null) {
            return;
        }
        if (currentChannel != null) {
            currentChannel.close();
        }
        Path file = segmentFilePath(segmentIndex);
        currentChannel = FileChannel.open(
                file,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE
        );
        currentSegmentIndex = segmentIndex;
    }

    private Path segmentFilePath(int segmentIndex) {
        return walDir.resolve(topicName + "-" + segmentIndex + ".wal");
    }
}
