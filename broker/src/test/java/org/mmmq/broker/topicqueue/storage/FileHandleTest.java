package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.topicqueue.storage.FileHandle.FlushMode;

class FileHandleTest {

    @Test
    @DisplayName("writeFully는 buffer의 모든 bytes를 파일에 기록한다")
    void writeFullyWritesAllBytes(@TempDir Path tempDir) throws IOException {
        try (FileHandle fileHandle = open(tempDir)) {
            byte[] data = "hello".getBytes();

            fileHandle.writeFully(0, ByteBuffer.wrap(data), FlushMode.NONE);

            assertThat(fileHandle.size()).isEqualTo(data.length);
        }
    }

    @Test
    @DisplayName("readFully는 지정 position에서 length bytes를 모두 읽는다")
    void readFullyReadsAllBytes(@TempDir Path tempDir) throws IOException {
        try (FileHandle fileHandle = open(tempDir)) {
            byte[] data = "hello".getBytes();
            fileHandle.writeFully(0, ByteBuffer.wrap(data), FlushMode.NONE);

            byte[] result = fileHandle.readFully(0, data.length);

            assertThat(result).isEqualTo(data);
        }
    }

    @Test
    @DisplayName("write/read round-trip 시 원본 데이터와 동일하다")
    void roundTrip(@TempDir Path tempDir) throws IOException {
        try (FileHandle fileHandle = open(tempDir)) {
            byte[] original = "hello world 1234567890".getBytes();
            fileHandle.writeFully(0, ByteBuffer.wrap(original), FlushMode.NONE);

            byte[] result = fileHandle.readFully(0, original.length);

            assertThat(result).isEqualTo(original);
        }
    }

    @Test
    @DisplayName("non-zero position에서 write/read가 정확히 동작한다")
    void writeAndReadAtNonZeroPosition(@TempDir Path tempDir) throws IOException {
        try (FileHandle fileHandle = open(tempDir)) {
            byte[] first = "hello".getBytes();
            byte[] second = "world".getBytes();
            fileHandle.writeFully(0, ByteBuffer.wrap(first), FlushMode.NONE);
            fileHandle.writeFully(first.length, ByteBuffer.wrap(second), FlushMode.NONE);

            assertThat(fileHandle.readFully(0, first.length)).isEqualTo(first);
            assertThat(fileHandle.readFully(first.length, second.length)).isEqualTo(second);
        }
    }

    @Test
    @DisplayName("파일 끝을 초과하는 readFully는 IOException을 던진다")
    void throwsOnEOF(@TempDir Path tempDir) throws IOException {
        try (FileHandle fileHandle = open(tempDir)) {
            fileHandle.writeFully(0, ByteBuffer.wrap("hi".getBytes()), FlushMode.NONE);

            assertThatThrownBy(() -> fileHandle.readFully(0, 100))
                    .isInstanceOf(IOException.class);
        }
    }

    private FileHandle open(Path dir) throws IOException {
        return FileHandle.open(
                dir.resolve("test.bin"),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
    }
}
