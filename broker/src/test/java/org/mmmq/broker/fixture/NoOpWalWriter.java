package org.mmmq.broker.fixture;

import org.mmmq.broker.wal.flush.PageCacheFlushPolicy;
import org.mmmq.broker.wal.WalEntry;
import org.mmmq.broker.wal.WalWriter;

public class NoOpWalWriter extends WalWriter {

    public NoOpWalWriter() {
        super(null, null, new PageCacheFlushPolicy());
    }

    @Override
    public synchronized void write(WalEntry entry, int segmentIndex) {
    }

    @Override
    public synchronized void deleteSegmentFile(int segmentIndex) {
    }
}
