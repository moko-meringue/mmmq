package org.mmmq.broker.wal;

import org.mmmq.broker.topicqueue.MessagePersistence;
import org.mmmq.broker.topicqueue.MessagePersistenceFactory;
import org.mmmq.broker.wal.codec.WalCodec;
import org.mmmq.broker.wal.flush.WalFlushPolicy;
import org.mmmq.broker.wal.flush.WalFlushPolicyFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class WalMessagePersistenceFactory implements MessagePersistenceFactory {

    private final WalCodec codec;
    private final WalFileStore walDirectory;
    private final WalFlushPolicy flushPolicy;

    WalMessagePersistenceFactory(
            WalFileStore walDirectory,
            WalCodec codec,
            @Value("${mmmq.broker.wal.flush-policy:page_cache}") String flushPolicyName
    ) {
        this.walDirectory = walDirectory;
        this.codec = codec;
        this.flushPolicy = WalFlushPolicyFactory.create(flushPolicyName);
    }

    @Override
    public MessagePersistence create(String topicName) {
        return new WalMessagePersistence(walDirectory, topicName, codec, flushPolicy);
    }
}
