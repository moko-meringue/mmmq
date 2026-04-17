package org.mmmq.broker.wal.flush;

public class WalFlushPolicyFactory {

    private WalFlushPolicyFactory() {
    }

    public static WalFlushPolicy create(String name) {
        return switch (name.toUpperCase()) {
            case "PAGE_CACHE" -> new PageCacheFlushPolicy();
            case "FSYNC" -> new FsyncFlushPolicy();
            default -> throw new IllegalArgumentException("Unknown flush policy: " + name);
        };
    }
}
