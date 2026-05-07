package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class SegmentFileChainTest {

    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;

    @Test
    @DisplayName("빈 디렉토리에서 첫 segment 가 startOffset=0 으로 생성된다")
    void createsFirstSegmentAtZero(@TempDir Path tempDir) {
        try (SegmentFileChain directory = SegmentFileChain.open(tempDir, DEFAULT_MAX_BYTES)) {
            assertThat(directory.readAt(0L)).isNull();
        }
    }

    @Test
    @DisplayName("rotate 이후 새 segment의 startOffset은 이전 segment의 nextAbsoluteOffset")
    void rotatesAtThreshold(@TempDir Path tempDir) {
        try (SegmentFileChain directory = SegmentFileChain.open(tempDir, 1L)) {
            final Message message = new Message(new Topic("topic"), Map.of("k", "v"));
            directory.append(message);
            directory.append(message);

            assertThat(directory.readAt(0L)).isEqualTo(message);
            assertThat(directory.readAt(1L)).isEqualTo(message);
            assertThat(directory.readAt(2L)).isNull();
        }
    }

    @Test
    @DisplayName("cross-segment readAt이 동작한다")
    void crossSegmentRead(@TempDir Path tempDir) {
        try (SegmentFileChain directory = SegmentFileChain.open(tempDir, 1L)) {
            final Message first = new Message(new Topic("topic"), Map.of("seq", 1));
            final Message second = new Message(new Topic("topic"), Map.of("seq", 2));
            final Message third = new Message(new Topic("topic"), Map.of("seq", 3));

            directory.append(first);
            directory.append(second);
            directory.append(third);

            assertThat(directory.readAt(0L)).isEqualTo(first);
            assertThat(directory.readAt(1L)).isEqualTo(second);
            assertThat(directory.readAt(2L)).isEqualTo(third);
        }
    }

    @Test
    @DisplayName(".mmm에 trailing partial bytes 있으면 truncate 복구한다")
    void recoversByTruncating(@TempDir Path tempDir) throws IOException {
        try (SegmentFileChain directory = SegmentFileChain.open(tempDir, DEFAULT_MAX_BYTES)) {
            directory.append(new Message(new Topic("topic"), Map.of("k", "v")));
        }

        final Path segmentFile = tempDir.resolve("0000000000000000000.mmm");
        final long beforeSize = Files.size(segmentFile);
        try (FileChannel channel = FileChannel.open(segmentFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            channel.write(ByteBuffer.wrap("garbage".getBytes()));
        }
        assertThat(Files.size(segmentFile)).isGreaterThan(beforeSize);

        try (SegmentFileChain directory = SegmentFileChain.open(tempDir, DEFAULT_MAX_BYTES)) {
            assertThat(Files.size(segmentFile)).isEqualTo(beforeSize);
            assertThat(directory.readAt(0L)).isNotNull();
            assertThat(directory.readAt(1L)).isNull();
        }
    }

    @Test
    @DisplayName(".idx가 .mmm보다 더 멀리 가리키면 StorageException 던진다")
    void throwsWhenIndexBeyondSegment(@TempDir Path tempDir) throws IOException {
        try (SegmentFileChain directory = SegmentFileChain.open(tempDir, DEFAULT_MAX_BYTES)) {
            directory.append(new Message(new Topic("topic"), Map.of("k", "v")));
        }
        final Path segmentFile = tempDir.resolve("0000000000000000000.mmm");
        try (FileChannel channel = FileChannel.open(segmentFile, StandardOpenOption.WRITE)) {
            channel.truncate(2L);
        }

        assertThatThrownBy(() -> SegmentFileChain.open(tempDir, DEFAULT_MAX_BYTES))
                .isInstanceOf(StorageException.class);
    }

    @Test
    @DisplayName("여러 segment가 존재하는 디렉토리는 모든 segment를 로드한다")
    void loadsAllSegments(@TempDir Path tempDir) {
        try (SegmentFileChain directory = SegmentFileChain.open(tempDir, 1L)) {
            directory.append(new Message(new Topic("topic"), Map.of("seq", 1)));
            directory.append(new Message(new Topic("topic"), Map.of("seq", 2)));
            directory.append(new Message(new Topic("topic"), Map.of("seq", 3)));
        }

        try (SegmentFileChain reopened = SegmentFileChain.open(tempDir, 1L)) {
            final Message first = reopened.readAt(0L);
            final Message last = reopened.readAt(2L);
            assertThat(first).isNotNull();
            assertThat(last).isNotNull();
            assertThat(reopened.readAt(3L)).isNull();
        }
    }
}
