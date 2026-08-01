package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointDirectoryTest {

    @Test
    @DisplayName("deregister하면 get이 null이고 파일도 사라진다")
    void deregisterRemovesCheckpoint(@TempDir Path tempDir) {
        try (CheckpointDirectory directory = CheckpointDirectory.open(tempDir)) {
            Path checkpoint = tempDir.resolve("checkpoints").resolve("dispatcher-a.checkpoint");
            directory.register("dispatcher-a");
            assertThat(checkpoint).exists();

            directory.deregister("dispatcher-a");

            assertThat(directory.get("dispatcher-a")).isNull();
            assertThat(checkpoint).doesNotExist();
        }
    }

    @Test
    @DisplayName("없는 이름으로 deregister해도 아무 일도 없다")
    void deregisterUnknownNameIsNoop(@TempDir Path tempDir) {
        try (CheckpointDirectory directory = CheckpointDirectory.open(tempDir)) {
            assertThatCode(() -> directory.deregister("absent")).doesNotThrowAnyException();
        }
    }
}
