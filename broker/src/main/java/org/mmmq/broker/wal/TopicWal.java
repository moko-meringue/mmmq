package org.mmmq.broker.wal;

import jakarta.annotation.Nullable;
import org.mmmq.broker.wal.codec.WalCodec;
import org.mmmq.broker.wal.flush.WalFlushPolicy;
import org.mmmq.core.message.Message;

public class TopicWal {

    private final WalDirectory directory;
    private final String topicName;
    private final WalCodec codec;
    private final WalFlushPolicy flushPolicy;
    @Nullable
    private WalFileChannel currentChannel;

    TopicWal(WalDirectory directory, String topicName, WalCodec codec, WalFlushPolicy flushPolicy) {
        this.directory = directory;
        this.topicName = topicName;
        this.codec = codec;
        this.flushPolicy = flushPolicy;
    }

    public synchronized void write(int segmentIndex, Message message) {
        if (currentChannel == null || currentChannel.index() != segmentIndex) {
            if (currentChannel != null) {
                currentChannel.close();
            }
            WalFile next = directory.createWalFile(topicName, segmentIndex);
            currentChannel = new WalFileChannel(next, flushPolicy);
        }
        currentChannel.write(codec.encode(new WalEntry(message)));
    }

    public synchronized void deleteSegment(int segmentIndex) {
        if (currentChannel != null && currentChannel.index() == segmentIndex) {
            currentChannel.close();
            currentChannel = null;
        }
        directory.createWalFile(topicName, segmentIndex).delete();
    }
}
