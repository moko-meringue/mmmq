package org.mmmq.broker.wal.file;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.wal.flush.PageCacheFlushPolicy;

class WalFileChannelTest {

    @Test
    @DisplayName("write()는 payload를 라인 단위로 파일에 기록한다")
    void writesPayloadAsLine(@TempDir Path tempDir) throws Exception {
        WalFile segmentFile = WalFile.of(tempDir, "order", 0);
        WalFileChannel channel = new WalFileChannel(segmentFile, new PageCacheFlushPolicy());

        channel.write("hello".getBytes());
        channel.write("world".getBytes());
        channel.close();

        final List<String> lines = Files.readAllLines(tempDir.resolve("order-0.wal"));
        assertThat(lines).containsExactly("hello", "world");
    }
}
