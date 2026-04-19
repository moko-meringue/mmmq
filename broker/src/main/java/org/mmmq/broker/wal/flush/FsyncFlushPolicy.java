package org.mmmq.broker.wal.flush;

import java.io.IOException;
import java.nio.channels.FileChannel;

public class FsyncFlushPolicy implements WalFlushPolicy {

    @Override
    public void flush(FileChannel channel) {
        try {
            channel.force(false);
        } catch (IOException exception) {
            throw new RuntimeException("Failed to fsync WAL channel: " + channel, exception);
        }
    }
}
