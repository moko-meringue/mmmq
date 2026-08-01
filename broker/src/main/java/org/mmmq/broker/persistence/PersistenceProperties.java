package org.mmmq.broker.persistence;

import java.nio.file.Path;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 브로커가 디스크에 쓰는 모든 것의 위치를 정하는 설정({@code mmmq.broker.persistence.*}).
 *
 * <p>경로 조립의 단일 출처다 — 설정 파일과 토픽 디렉터리 위치를 여기서만 만들기 때문에
 * 쓰는 쪽이 {@code root-dir}에 무엇이 들어왔는지 알 필요가 없다. 값이 비면 기본값으로 채워 넣으므로 설정 없이도 기동한다.
 */
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

    public Path dispatchersFilePath() {
        return Path.of(rootDir).resolve(DISPATCHERS_FILE_NAME);
    }

    public Path topicsDirPath() {
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
