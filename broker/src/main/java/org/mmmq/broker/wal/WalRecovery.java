package org.mmmq.broker.wal;

import java.util.stream.Stream;
import org.mmmq.broker.topicqueue.MessageRestorer;
import org.mmmq.broker.topicqueue.RestoreCompletedEvent;
import org.mmmq.broker.wal.codec.WalCodec;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class WalRecovery implements SmartInitializingSingleton {

    private final MessageRestorer restorer;
    private final WalStore walStore;
    private final WalCodec codec;
    private final ApplicationEventPublisher publisher;

    public WalRecovery(
            MessageRestorer restorer,
            WalStore walStore,
            WalCodec codec,
            ApplicationEventPublisher publisher
    ) {
        this.restorer = restorer;
        this.walStore = walStore;
        this.codec = codec;
        this.publisher = publisher;
    }

    @Override
    public void afterSingletonsInstantiated() {
        try (Stream<WalEntry> entries = walStore.segmentFiles().stream()
                .flatMap(segmentFile -> segmentFile.read(codec))) {
            entries.forEach(entry -> restorer.restore(entry.message()));
        }
        publisher.publishEvent(new RestoreCompletedEvent());
    }
}
