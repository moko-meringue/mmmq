package org.mmmq.broker.wal.flush;

import java.nio.channels.FileChannel;

// OS의 page cache에만 기록하고 디스크 쓰기는 OS에 위임한다.
// 브로커가 비정상 종료되어도 OS가 살아있다면 데이터 유실이 없으므로
// 일반적인 운영 환경에서는 이 설정으로 충분하다.
public class PageCacheFlushPolicy implements WalFlushPolicy {

    @Override
    public void flush(FileChannel channel) {
    }
}
