package org.mmmq.broker.wal.flush;

import java.io.IOException;
import java.nio.channels.FileChannel;

// 매 write마다 물리 디스크 쓰기를 강제한다.
// force(false)는 데이터만 fsync하므로 force(true)(메타데이터 포함)보다 빠르면서
// 내구성 목적에는 충분하다.
public class FsyncFlushPolicy implements WalFlushPolicy {

    @Override
    public void flush(FileChannel channel) throws IOException {
        channel.force(false);
    }
}
