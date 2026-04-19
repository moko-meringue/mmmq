package org.mmmq.broker.wal.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.mmmq.broker.wal.flush.WalFlushPolicy;

public class WalFileChannel {

    private static final byte[] NEWLINE = new byte[]{'\n'};

    private final WalFile walFile;
    private final FileChannel channel;
    private final WalFlushPolicy flushPolicy;

    WalFileChannel(WalFile walFile, WalFlushPolicy flushPolicy) {
        this.walFile = walFile;
        this.channel = walFile.openChannel();
        this.flushPolicy = flushPolicy;
    }

    void write(byte[] payload) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(payload.length + NEWLINE.length);
            buffer.put(payload);
            buffer.put(NEWLINE);
            buffer.flip();
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            flushPolicy.flush(channel);
        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to write WAL entry for segment: " + walFile.index(),
                    exception
            );
        }
    }

    void close() {
        try {
            channel.close();
        } catch (IOException exception) {
            throw new RuntimeException(
                    "Failed to close WAL channel for segment: " + walFile.index(),
                    exception
            );
        }
    }

    int getWalFileIndex() {
        return walFile.index();
    }
}
