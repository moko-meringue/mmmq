package org.mmmq.broker.dispatcher.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.persistence.PersistenceProperties;

class DispatchersFileTest {

    private static final String FILE_NAME = "dispatchers.json";

    @TempDir
    Path tempDir;

    DispatchersFile dispatchersFile;

    @BeforeEach
    void setUp() {
        dispatchersFile = new DispatchersFile(new PersistenceProperties(tempDir.toString(), null));
    }

    @Test
    @DisplayName("쓰고 다시 읽으면 같은 목록이 나온다")
    void roundTrips() {
        List<DispatcherEntry> entries = List.of(
                new DispatcherEntry("order-created", "http://consumer-host:8080", "order.created"),
                new DispatcherEntry("order-shipped", "https://other-host:8443", "order.shipped")
        );

        dispatchersFile.write(entries);

        assertThat(dispatchersFile.read()).isEqualTo(entries);
    }

    @Test
    @DisplayName("root-dir 디렉터리가 없으면 생성자가 디렉터리와 빈 파일을 만든다")
    void constructorCreatesRootDirWhenMissing() {
        Path absentRoot = tempDir.resolve("absent-root");

        DispatchersFile file = new DispatchersFile(new PersistenceProperties(absentRoot.toString(), null));

        // read() 호출 전에 이미 파일이 있어야 한다 — 생성이 read()가 아니라 생성자 책임임을 확인한다.
        assertThat(absentRoot.resolve(FILE_NAME)).exists();
        assertThat(file.read()).isEmpty();
    }

    @Test
    @DisplayName("깨진 JSON은 예외를 던진다")
    void rejectsMalformedJson() throws IOException {
        Files.writeString(tempDir.resolve(FILE_NAME), "not json");

        assertThatThrownBy(dispatchersFile::read)
                .isInstanceOf(IllegalStateException.class);
    }
}
