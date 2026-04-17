package org.mmmq.broker.wal.flush;

import java.nio.channels.FileChannel;

public class PageCacheFlushPolicy implements WalFlushPolicy {

    @Override
    public void flush(FileChannel channel) {
    }
}
