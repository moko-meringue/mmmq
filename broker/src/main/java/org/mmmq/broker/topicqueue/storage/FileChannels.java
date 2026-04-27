package org.mmmq.broker.topicqueue.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

final class FileChannels {

    private FileChannels() {
    }

    static void writeFully(FileChannel channel, long position, ByteBuffer buffer, FlushMode flushMode)
            throws IOException {
        long writePosition = position;
        while (buffer.hasRemaining()) {
            writePosition += channel.write(buffer, writePosition);
        }
        flushMode.flush(channel);
    }

    static byte[] readFully(FileChannel channel, long position, int length) throws IOException {
        if (position < 0 || length < 0) {
            throw new IllegalArgumentException("position and length must be non-negative: position=" + position + ", length=" + length);
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        while (buffer.hasRemaining()) {
            if (channel.read(buffer, position + buffer.position()) <= 0) {
                throw new IOException("Unexpected EOF at position " + (position + buffer.position()));
            }
        }

        return buffer.array();
    }

    enum FlushMode {

        FSYNC {
            @Override
            void flush(FileChannel channel) throws IOException {
                channel.force(true);
            }
        },
        NONE {
            @Override
            void flush(FileChannel channel) {
            }
        };

        abstract void flush(FileChannel channel) throws IOException;
    }
}
