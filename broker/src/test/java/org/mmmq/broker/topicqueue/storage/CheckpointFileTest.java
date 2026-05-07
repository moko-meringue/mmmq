package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointFileTest {

    @Test
    @DisplayName("새로 생성한 store의 read는 0L을 반환한다")
    void newStoreReturnsZero(@TempDir Path tempDir) {
        try (CheckpointFile store = CheckpointFile.open(tempDir, "dispatcher-a")) {
            assertThat(store.read()).isZero();
        }
    }

    @Test
    @DisplayName("write 후 read는 동일한 값을 반환한다")
    void writeAndRead(@TempDir Path tempDir) {
        try (CheckpointFile store = CheckpointFile.open(tempDir, "dispatcher-a")) {
            store.write(123L);

            assertThat(store.read()).isEqualTo(123L);
        }
    }

    @Test
    @DisplayName("write 후 새로 open한 store도 동일한 값을 읽는다")
    void persistsAcrossInstances(@TempDir Path tempDir) {
        try (CheckpointFile store = CheckpointFile.open(tempDir, "dispatcher-a")) {
            store.write(42L);
        }

        try (CheckpointFile reopened = CheckpointFile.open(tempDir, "dispatcher-a")) {
            assertThat(reopened.read()).isEqualTo(42L);
        }
    }
}
