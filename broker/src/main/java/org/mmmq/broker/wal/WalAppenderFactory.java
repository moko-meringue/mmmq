package org.mmmq.broker.wal;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.mmmq.broker.wal.codec.WalCodec;
import org.mmmq.broker.wal.flush.WalFlushPolicy;
import org.mmmq.broker.wal.flush.WalFlushPolicyFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class WalAppenderFactory {

    private final WalCodec walCodec;
    private final WalFlushPolicy flushPolicy;

    private final Path walDir;

    public WalAppenderFactory(
            WalCodec walCodec,
            @Value("${mmmq.broker.wal.flush-policy:page_cache}") String flushPolicy,
            @Value("${mmmq.broker.wal.dir:./wal}") String walDir
    ) {
        this.walCodec = walCodec;
        this.flushPolicy = WalFlushPolicyFactory.create(flushPolicy);
        this.walDir = Paths.get(walDir);
    }

    public WalAppender create(String topicName) {
        return new WalWriter(walDir, topicName, flushPolicy, walCodec);
    }
}
