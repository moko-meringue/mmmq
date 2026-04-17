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

public class WalWriter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final byte[] NEWLINE = new byte[]{'\n'};

    private final Path walDir;
    private final String topicName;
    private final WalFlushPolicy flushPolicy;

    @Nullable
    private FileChannel currentChannel;
    private int currentSegmentIndex = -1;

    public WalWriter(Path walDir, String topicName, WalFlushPolicy flushPolicy) {
        this.walDir = walDir;
        this.topicName = topicName;
        this.flushPolicy = flushPolicy;
    }

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
