package org.mmmq.broker.wal;

import jakarta.annotation.Nullable;
import org.mmmq.broker.topicqueue.MessagePersistence;
import org.mmmq.broker.wal.codec.WalCodec;
import org.mmmq.broker.wal.flush.WalFlushPolicy;
import org.mmmq.core.message.Message;

class WalMessagePersistence implements MessagePersistence {

    private final WalDirectory directory;
    private final String topicName;
    private final WalCodec codec;
    private final WalFlushPolicy flushPolicy;
    @Nullable
    private WalFileChannel currentChannel;

    WalMessagePersistence(WalDirectory directory, String topicName, WalCodec codec, WalFlushPolicy flushPolicy) {
        this.directory = directory;
        this.topicName = topicName;
        this.codec = codec;
        this.flushPolicy = flushPolicy;
    }

    @Override
    public synchronized void persist(Message message, int index) {
        if (currentChannel == null || currentChannel.index() != index) {
            if (currentChannel != null) {
                currentChannel.close();
            }
            WalFile next = directory.createWalFile(topicName, index);
            currentChannel = new WalFileChannel(next, flushPolicy);
        }
        currentChannel.write(codec.encode(new WalEntry(message)));
    }

    @Override
    public synchronized void evict(int index) {
        if (currentChannel != null && currentChannel.index() == index) {
            currentChannel.close();
            currentChannel = null;
        }
        directory.createWalFile(topicName, index).delete();
    }
}
