package org.mmmq.broker.wal.file;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalFileStoreTest {

    @Test
    @DisplayName("delete()는 파일을 삭제한다")
    void deletesFile(@TempDir Path tempDir) throws Exception {
        Files.writeString(tempDir.resolve("order-0.wal"), "content");
        final WalFileStore store = new WalFileStore(tempDir.toString());

        store.delete("order", 0);

        assertThat(Files.exists(tempDir.resolve("order-0.wal"))).isFalse();
    }

    @Test
    @DisplayName("delete()는 파일이 없어도 예외를 던지지 않는다")
    void deleteMissingFileDoesNotThrow(@TempDir Path tempDir) {
        final WalFileStore store = new WalFileStore(tempDir.toString());

        store.delete("order", 0);
    }
}
