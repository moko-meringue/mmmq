package org.mmmq.broker.wal.flush;

import java.io.IOException;
import java.nio.channels.FileChannel;

public interface WalFlushPolicy {

    void flush(FileChannel channel) throws IOException;
}
