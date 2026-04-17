package org.mmmq.broker.wal;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.mmmq.broker.wal.codec.WalCodec;
import org.mmmq.broker.wal.flush.WalFlushPolicy;

class WalWriter implements WalAppender {

    private static final byte[] NEWLINE = new byte[]{'\n'};

    private static final Pattern SEGMENT_FILE_PATTERN = Pattern.compile("^(.+)-(\\d+)\\.wal$");

    private final Path walDir;
    private final String topicName;
    private final WalFlushPolicy flushPolicy;

    private final WalCodec walCodec;

    @Nullable
    private FileChannel currentChannel;
    private int currentSegmentIndex;

    WalWriter(
            Path walDir,
            String topicName,
            WalFlushPolicy flushPolicy,
            WalCodec walCodec
    ) {
        this.walDir = walDir;
        this.topicName = topicName;
        this.flushPolicy = flushPolicy;
        this.walCodec = walCodec;
        this.currentSegmentIndex = resolveInitialSegmentIndex();
    }

    private int resolveInitialSegmentIndex() {
        if (!Files.isDirectory(walDir)) {
            return -1;
        }
        try (Stream<Path> files = Files.list(walDir)) {
            return files
                    .map(path -> path.getFileName().toString())
                    .map(SEGMENT_FILE_PATTERN::matcher)
                    .filter(Matcher::matches)
                    .filter(matcher -> matcher.group(1).equals(topicName))
                    .map(matcher -> Integer.parseInt(matcher.group(2)))
                    .max(Comparator.naturalOrder())
                    .orElse(-1);
        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to scan WAL directory for topic: " + topicName + ", dir: " + walDir,
                    exception
            );
        }
    }

    public synchronized void write(WalEntry entry) {
        try {
            ensureChannelFor(entry.segmentIndex());
            byte[] payload = walCodec.encode(entry);
            ByteBuffer buffer = ByteBuffer.allocate(payload.length + NEWLINE.length);
            buffer.put(payload);
            buffer.put(NEWLINE);
            buffer.flip();
            while (buffer.hasRemaining()) {
                currentChannel.write(buffer);
            }
            flushPolicy.flush(currentChannel);
        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to write WAL entry for topic: " + topicName
                            + ", segmentIndex: " + entry.segmentIndex(),
                    exception
            );
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
            throw new RuntimeException(
                    "Failed to delete WAL segment file for topic: " + topicName
                            + ", segmentIndex: " + segmentIndex,
                    exception
            );
        }
    }

    private void ensureChannelFor(int segmentIndex) throws IOException {
        if (segmentIndex == currentSegmentIndex && currentChannel != null) {
            return;
        }
        if (currentChannel != null) {
            currentChannel.close();
            currentChannel = null;
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
