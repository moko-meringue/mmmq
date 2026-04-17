package org.mmmq.broker.wal;

public interface WalAppender {

    void write(WalEntry entry);

    void deleteSegmentFile(int segmentIndex);
}
