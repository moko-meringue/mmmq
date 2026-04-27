package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.topicqueue.storage.FileChannels.FlushMode;

class FileChannelsTest {

    @Test
    @DisplayName("writeFully는 buffer의 모든 bytes를 파일에 기록한다")
    void writeFullyWritesAllBytes(@TempDir Path tempDir) throws IOException {
        try (FileChannel channel = open(tempDir)) {
            byte[] data = "hello".getBytes();

            FileChannels.writeFully(channel, 0, ByteBuffer.wrap(data), FlushMode.NONE);

            assertThat(channel.size()).isEqualTo(data.length);
        }
    }

    @Test
    @DisplayName("readFully는 지정 position에서 length bytes를 모두 읽는다")
    void readFullyReadsAllBytes(@TempDir Path tempDir) throws IOException {
        try (FileChannel channel = open(tempDir)) {
            byte[] data = "hello".getBytes();
            FileChannels.writeFully(channel, 0, ByteBuffer.wrap(data), FlushMode.NONE);

            byte[] result = FileChannels.readFully(channel, 0, data.length);

            assertThat(result).isEqualTo(data);
        }
    }

    @Test
    @DisplayName("write/read round-trip 시 원본 데이터와 동일하다")
    void roundTrip(@TempDir Path tempDir) throws IOException {
        try (FileChannel channel = open(tempDir)) {
            byte[] original = "hello world 1234567890".getBytes();
            FileChannels.writeFully(channel, 0, ByteBuffer.wrap(original), FlushMode.NONE);

            byte[] result = FileChannels.readFully(channel, 0, original.length);

            assertThat(result).isEqualTo(original);
        }
    }

    @Test
    @DisplayName("non-zero position에서 write/read가 정확히 동작한다")
    void writeAndReadAtNonZeroPosition(@TempDir Path tempDir) throws IOException {
        try (FileChannel channel = open(tempDir)) {
            byte[] first = "hello".getBytes();
            byte[] second = "world".getBytes();
            FileChannels.writeFully(channel, 0, ByteBuffer.wrap(first), FlushMode.NONE);
            FileChannels.writeFully(channel, first.length, ByteBuffer.wrap(second), FlushMode.NONE);

            assertThat(FileChannels.readFully(channel, 0, first.length)).isEqualTo(first);
            assertThat(FileChannels.readFully(channel, first.length, second.length)).isEqualTo(second);
        }
    }

    @Test
    @DisplayName("파일 끝을 초과하는 readFully는 IOException을 던진다")
    void throwsOnEOF(@TempDir Path tempDir) throws IOException {
        try (FileChannel channel = open(tempDir)) {
            FileChannels.writeFully(channel, 0, ByteBuffer.wrap("hi".getBytes()), FlushMode.NONE);

            assertThatThrownBy(() -> FileChannels.readFully(channel, 0, 100))
                    .isInstanceOf(IOException.class);
        }
    }

    private FileChannel open(Path dir) throws IOException {
        return FileChannel.open(
                dir.resolve("test.bin"),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
    }
}
