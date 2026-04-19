package org.mmmq.broker.fixture;

import java.nio.file.Path;
import org.mmmq.broker.wal.TopicWal;
import org.mmmq.broker.wal.WalDirectory;
import org.mmmq.broker.wal.codec.JsonWalCodec;

public class NoOpTopicWal {

    public static TopicWal create(Path tempDir, String topicName) {
        WalDirectory walDirectory = new WalDirectory(new JsonWalCodec(), tempDir.toString(), "page_cache");

        return walDirectory.topicWalFor(topicName);
    }
}
