package org.mmmq.broker.wal;

import java.util.stream.Stream;
import org.mmmq.broker.topicqueue.MessageRestorer;
import org.mmmq.broker.topicqueue.RestoreCompletedEvent;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class WalRecovery implements SmartInitializingSingleton {

    private final MessageRestorer restorer;
    private final WalDirectory walDirectory;
    private final ApplicationEventPublisher publisher;

    public WalRecovery(MessageRestorer restorer, WalDirectory walDirectory, ApplicationEventPublisher publisher) {
        this.restorer = restorer;
        this.walDirectory = walDirectory;
        this.publisher = publisher;
    }

    @Override
    public void afterSingletonsInstantiated() {
        try (Stream<WalEntry> entries = walDirectory.segmentFiles().stream().flatMap(walDirectory::read)) {
            entries.forEach(entry -> restorer.restore(entry.message()));
        }
        publisher.publishEvent(new RestoreCompletedEvent());
    }
}
