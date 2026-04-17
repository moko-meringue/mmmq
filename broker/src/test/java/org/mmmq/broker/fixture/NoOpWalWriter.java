package org.mmmq.broker.fixture;

import org.mmmq.broker.wal.WalAppender;
import org.mmmq.broker.wal.WalEntry;

public class NoOpWalWriter implements WalAppender {

    @Override
    public void write(WalEntry entry) {
    }

    @Override
    public void deleteSegmentFile(int segmentIndex) {
    }
}
