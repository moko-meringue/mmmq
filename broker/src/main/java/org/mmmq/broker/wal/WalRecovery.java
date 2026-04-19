package org.mmmq.broker.wal;

import java.util.List;
import java.util.stream.Stream;
import org.mmmq.broker.topicqueue.MessageRestorer;
import org.mmmq.broker.topicqueue.RestoreCompletedEvent;
import org.mmmq.broker.wal.codec.WalCodec;
import org.mmmq.broker.wal.file.WalFile;
import org.mmmq.broker.wal.file.WalFileStore;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class WalRecovery implements SmartInitializingSingleton {

    private final WalCodec codec;
    private final MessageRestorer restorer;
    private final WalFileStore walFileStore;
    private final ApplicationEventPublisher publisher;

    public WalRecovery(
            WalCodec codec, MessageRestorer restorer,
            WalFileStore walFileStore,
            ApplicationEventPublisher publisher
    ) {
        this.codec = codec;
        this.restorer = restorer;
        this.walFileStore = walFileStore;
        this.publisher = publisher;
    }

    @Override
    public void afterSingletonsInstantiated() {
        List<WalFile> walFiles = walFileStore.walFiles();
        try (Stream<WalEntry> walEntryStream = walFiles.stream()
                .flatMap(walFile -> walFile.read(codec))) {
            walEntryStream.forEach(entry -> restorer.restore(entry.message()));
        }
        publisher.publishEvent(new RestoreCompletedEvent());
    }
}
