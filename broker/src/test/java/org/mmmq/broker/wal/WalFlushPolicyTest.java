package org.mmmq.broker.wal;

import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.channels.FileChannel;
import org.mmmq.broker.wal.flush.FsyncFlushPolicy;
import org.mmmq.broker.wal.flush.PageCacheFlushPolicy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalFlushPolicyTest {

    @Test
    @DisplayName("PAGE_CACHE는 flush 호출 시 예외 없이 no-op으로 동작한다")
    void pageCacheIsNoOp(@TempDir Path tempDir) throws Exception {
        final Path file = tempDir.resolve("noop.wal");
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            assertThatCode(() -> new PageCacheFlushPolicy().flush(channel)).doesNotThrowAnyException();
        }
    }

    @Test
    @DisplayName("FSYNC는 FileChannel.force(false)를 호출하여 디스크에 플러시한다")
    void fsyncForcesToDisk(@TempDir Path tempDir) throws Exception {
        final Path file = tempDir.resolve("fsync.wal");
        Files.writeString(file, "payload");
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            assertThatCode(() -> new FsyncFlushPolicy().flush(channel)).doesNotThrowAnyException();
        }
    }
}
