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
        long address = channel.size();
        writeFully(address, buffer, flushMode);
        return address;
    }

    void writeFully(long address, ByteBuffer buffer, FlushMode flushMode) throws IOException {
        long writeAddress = address;
        while (buffer.hasRemaining()) {
            writeAddress += channel.write(buffer, writeAddress);
        }
        flushMode.flush(channel);
    }

    byte[] readFully(long address, int length) throws IOException {
        if (address < 0 || length < 0) {
            throw new IllegalArgumentException(
                    "address and length must be non-negative: address=" + address + ", length=" + length);
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        while (buffer.hasRemaining()) {
            if (channel.read(buffer, address + buffer.position()) <= 0) {
                throw new IOException("Unexpected EOF at address " + (address + buffer.position()));
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
