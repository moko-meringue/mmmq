package org.mmmq.broker.topicqueue.storage;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("mmmq.broker.storage")
public record StorageProperties(
        String rootDir
) {

    private static final String DEFAULT_ROOT_DIR = "./data";

    public StorageProperties {
        if (rootDir == null || rootDir.isBlank()) {
            rootDir = DEFAULT_ROOT_DIR;
        }
    }
}
