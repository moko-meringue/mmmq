package org.mmmq.broker.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.persistence.PersistenceProperties.Segment;

class PersistencePropertiesTest {

    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;

    @Test
    @DisplayName("미설정 시 기본값이 적용된다")
    void appliesDefaults() {
        PersistenceProperties properties = new PersistenceProperties(null, null);

        assertThat(properties.rootDir()).isEqualTo("./mmmq");
        assertThat(properties.segment().maxBytes()).isEqualTo(DEFAULT_MAX_BYTES);
    }

    @Test
    @DisplayName("rootDir이 공백이면 기본값으로 대체된다")
    void replacesBlankRootDir() {
        PersistenceProperties properties = new PersistenceProperties("  ", null);

        assertThat(properties.rootDir()).isEqualTo("./mmmq");
    }

    @Test
    @DisplayName("maxBytes가 0 이하이면 기본값으로 대체된다")
    void replacesInvalidMaxBytes() {
        PersistenceProperties properties = new PersistenceProperties("./mmmq", new Segment(0));

        assertThat(properties.segment().maxBytes()).isEqualTo(DEFAULT_MAX_BYTES);
    }

    @Test
    @DisplayName("dispatchers.json 경로는 root-dir 바로 아래로 고정된다")
    void resolvesDispatchersFilePath() {
        PersistenceProperties properties = new PersistenceProperties("/var/mmmq", null);

        assertThat(properties.dispatchersFilePath()).isEqualTo(Path.of("/var/mmmq/dispatchers.json"));
    }

    @Test
    @DisplayName("토픽 데이터 경로는 root-dir/topics로 고정된다")
    void resolvesTopicsDirPath() {
        PersistenceProperties properties = new PersistenceProperties("/var/mmmq", null);

        assertThat(properties.topicsDirPath()).isEqualTo(Path.of("/var/mmmq/topics"));
    }
}
