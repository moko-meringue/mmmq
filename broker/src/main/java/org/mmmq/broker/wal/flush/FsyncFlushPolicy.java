package org.mmmq.broker.wal.flush;

import java.io.IOException;
import java.nio.channels.FileChannel;

public class FsyncFlushPolicy implements WalFlushPolicy {

    @Override
    public void flush(FileChannel channel) throws IOException {
        channel.force(false);
    }
}
