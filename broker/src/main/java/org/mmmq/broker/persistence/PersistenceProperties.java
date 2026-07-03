package org.mmmq.broker.persistence;

import java.nio.file.Path;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

@ConfigurationProperties(PersistenceProperties.PREFIX)
public record PersistenceProperties(
        String rootDir,
        Segment segment
) {

    public static final String PREFIX = "mmmq.broker.persistence";

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

    // ConfigurationProperties 빈이 아직 없는 단계(예: ImportBeanDefinitionRegistrar)에서 사용한다.
    public static PersistenceProperties bind(Environment environment) {
        return Binder.get(environment)
                .bind(PREFIX, PersistenceProperties.class)
                .orElseGet(() -> new PersistenceProperties(null, null));
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
