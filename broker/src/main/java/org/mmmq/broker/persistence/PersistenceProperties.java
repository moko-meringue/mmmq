package org.mmmq.broker.persistence;

import java.nio.file.Path;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("mmmq.broker.persistence")
public record PersistenceProperties(
        String rootDir,
        Segment segment
) {

    private static final String DEFAULT_ROOT_DIR = "./mmmq";
    private static final String DISPATCHERS_FILE_NAME = "dispatchers.json";
    private static final String TOPICS_DIR_NAME = "topics";

    public PersistenceProperties {
        if (rootDir == null || rootDir.isBlank()) {
            rootDir = DEFAULT_ROOT_DIR;
        }
        if (segment == null) {
            segment = new Segment(0);
        }
    }

    public Path dispatchersFile() {
        return Path.of(rootDir).resolve(DISPATCHERS_FILE_NAME);
    }

    public Path topicsDir() {
        return Path.of(rootDir).resolve(TOPICS_DIR_NAME);
    }

    public record Segment(
            long maxBytes
    ) {

        private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;

        public Segment {
            if (maxBytes <= 0) {
                maxBytes = DEFAULT_MAX_BYTES;
            }
        }
    }
}
