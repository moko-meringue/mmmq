package org.mmmq.broker.wal.file;

import jakarta.annotation.Nullable;
import org.mmmq.broker.topicqueue.MessagePersistence;
import org.mmmq.broker.wal.WalEntry;
import org.mmmq.broker.wal.codec.WalCodec;
import org.mmmq.broker.wal.flush.WalFlushPolicy;
import org.mmmq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WalMessagePersistence implements MessagePersistence {

    private static final Logger log = LoggerFactory.getLogger(WalMessagePersistence.class);

    private final WalCodec codec;
    private final String topicName;
    private final WalFileStore walFileStore;
    private final WalFlushPolicy flushPolicy;

    @Nullable
    private WalFileChannel currentChannel;

    WalMessagePersistence(WalCodec codec, String topicName, WalFileStore walFileStore, WalFlushPolicy flushPolicy) {
        this.codec = codec;
        this.topicName = topicName;
        this.walFileStore = walFileStore;
        this.flushPolicy = flushPolicy;
    }

    @Override
    public synchronized void persist(Message message, int walFileIndex) {
        if (currentChannel == null || currentChannel.getWalFileIndex() != walFileIndex) {
            closeCurrentChannel();
            WalFile next = walFileStore.create(topicName, walFileIndex);
            currentChannel = new WalFileChannel(next, flushPolicy);
        }
        currentChannel.write(codec.encode(new WalEntry(message)));
    }

    @Override
    public synchronized void evict(int walFileIndex) {
        if (currentChannel != null && currentChannel.getWalFileIndex() == walFileIndex) {
            closeCurrentChannel();
            currentChannel = null;
        }
        walFileStore.delete(topicName, walFileIndex);
    }

    private void closeCurrentChannel() {
        if (currentChannel == null) {
            return;
        }
        try {
            currentChannel.close();
        } catch (RuntimeException exception) {
            log.warn("Failed to close WAL channel for index: {}", currentChannel.getWalFileIndex(), exception);
        }
    }
}
