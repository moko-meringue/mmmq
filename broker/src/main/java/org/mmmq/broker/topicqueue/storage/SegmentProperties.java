package org.mmmq.broker.topicqueue.storage;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("mmmq.broker.segment")
public record SegmentProperties(
        long maxBytes
) {

    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;

    public SegmentProperties {
        if (maxBytes <= 0) {
            maxBytes = DEFAULT_MAX_BYTES;
        }
    }
}
