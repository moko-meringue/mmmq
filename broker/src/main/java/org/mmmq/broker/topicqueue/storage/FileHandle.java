package org.mmmq.broker.topicqueue.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class FileHandle implements Closeable {

    private final FileChannel channel;

    private FileHandle(FileChannel channel) {
        this.channel = channel;
    }

    static FileHandle open(Path path, StandardOpenOption... options) throws IOException {
        return new FileHandle(FileChannel.open(path, options));
    }

    long appendFully(ByteBuffer buffer, FlushMode flushMode) throws IOException {
        long position = channel.size();
        writeFully(position, buffer, flushMode);

        return position;
    }

    void writeFully(long position, ByteBuffer buffer, FlushMode flushMode) throws IOException {
        long writePosition = position;
        while (buffer.hasRemaining()) {
            writePosition += channel.write(buffer, writePosition);
        }
        flushMode.flush(channel);
    }

    byte[] readFully(long position, int length) throws IOException {
        if (position < 0 || length < 0) {
            throw new IllegalArgumentException(
                    "position and length must be non-negative: position=" + position + ", length=" + length);
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        while (buffer.hasRemaining()) {
            if (channel.read(buffer, position + buffer.position()) <= 0) {
                throw new IOException("Unexpected EOF at position " + (position + buffer.position()));
            }
        }

        return buffer.array();
    }

    void truncate(long newSize, FlushMode flushMode) throws IOException {
        channel.truncate(newSize);
        flushMode.flush(channel);
    }

    long size() throws IOException {
        return channel.size();
    }

    @Override
    public void close() throws IOException {
        channel.close();
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
