package org.mmmq.broker.wal;

import java.util.List;
import java.util.stream.Stream;
import org.mmmq.broker.topicqueue.DispatchReadyEvent;
import org.mmmq.broker.topicqueue.MessageRestorer;
import org.mmmq.broker.topicqueue.TopicQueueReadyEvent;
import org.mmmq.broker.wal.codec.WalCodec;
import org.mmmq.broker.wal.file.WalFile;
import org.mmmq.broker.wal.file.WalFileStore;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class WalRecovery implements SmartInitializingSingleton {

    private final WalCodec codec;
    private final WalFileStore walFileStore;
    private final MessageRestorer messageRestorer;
    private final ApplicationEventPublisher applicationEventPublisher;

    public WalRecovery(
            WalCodec codec,
            WalFileStore walFileStore,
            MessageRestorer messageRestorer,
            ApplicationEventPublisher applicationEventPublisher
    ) {
        this.codec = codec;
        this.walFileStore = walFileStore;
        this.messageRestorer = messageRestorer;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public void afterSingletonsInstantiated() {
        List<WalFile> walFiles = walFileStore.walFiles();
        walFiles.forEach(walFile -> {
            try (Stream<WalEntry> entries = walFile.read(codec)) {
                entries.forEach(entry -> messageRestorer.restore(entry.message(), walFile.index()));
            }
        });
        applicationEventPublisher.publishEvent(new TopicQueueReadyEvent());
        applicationEventPublisher.publishEvent(new DispatchReadyEvent());
    }
}
