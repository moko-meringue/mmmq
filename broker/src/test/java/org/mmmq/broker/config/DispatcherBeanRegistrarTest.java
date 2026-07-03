package org.mmmq.broker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.dispatcher.DispatcherBeanRegistrar;
import org.mmmq.broker.dispatcher.DispatcherContainer;
import org.mmmq.core.identifier.ConsumerId;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

class DispatcherBeanRegistrarTest {

    @TempDir
    Path tempDir;

    ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    @DisplayName("유효한 2개 정의 → Dispatcher 빈 2개가 등록된다")
    void registersDispatchersFromFile() throws IOException {
        write("""
                [
                  {"consumerId":"order-created","host":{"protocol":"HTTP","address":"127.0.0.1","port":8080},"pattern":"order.created"},
                  {"consumerId":"order-shipped","host":{"protocol":"HTTP","address":"127.0.0.1","port":8080},"pattern":"order.shipped"}
                ]
                """);

        runner.withPropertyValues("mmmq.broker.persistence.root-dir=" + tempDir)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBeansOfType(Dispatcher.class).values())
                            .extracting(Dispatcher::consumerId)
                            .containsExactlyInAnyOrder(
                                    new ConsumerId("order-created"),
                                    new ConsumerId("order-shipped")
                            );
                });
    }

    @Test
    @DisplayName("파일이 없으면 → 빈 파일을 만들고 0개로 기동한다")
    void createsEmptyFileWhenMissing() {
        runner.withPropertyValues("mmmq.broker.persistence.root-dir=" + tempDir)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBeansOfType(Dispatcher.class)).isEmpty();
                });

        assertThat(tempDir.resolve("dispatchers.json")).exists();
        assertThat(readDispatchersFile()).isEqualTo("[]");
    }

    @Test
    @DisplayName("root-dir 디렉토리가 없으면 → 디렉토리와 빈 파일을 만들고 기동한다")
    void createsRootDirWhenMissing() {
        Path absentRoot = tempDir.resolve("absent-root");

        runner.withPropertyValues("mmmq.broker.persistence.root-dir=" + absentRoot)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBeansOfType(Dispatcher.class)).isEmpty();
                });

        assertThat(absentRoot.resolve("dispatchers.json")).exists();
    }

    @Test
    @DisplayName("중복 consumerId → 기동에 실패한다")
    void failsOnDuplicateConsumerId() throws IOException {
        write("""
                [
                  {"consumerId":"dup","host":{"protocol":"HTTP","address":"127.0.0.1","port":8080},"pattern":"a"},
                  {"consumerId":"dup","host":{"protocol":"HTTP","address":"127.0.0.1","port":8080},"pattern":"b"}
                ]
                """);

        runner.withPropertyValues("mmmq.broker.persistence.root-dir=" + tempDir)
                .run(context -> assertThat(context)
                        .hasFailed()
                        .getFailure()
                        .hasStackTraceContaining("Duplicate consumerId 'dup' in dispatcher file"));
    }

    @Test
    @DisplayName("consumerId가 regex에 어긋나면 → 기동에 실패한다")
    void failsOnInvalidConsumerId() throws IOException {
        write("""
                [
                  {"consumerId":"invalid id!","host":{"protocol":"HTTP","address":"127.0.0.1","port":8080},"pattern":"a"}
                ]
                """);

        runner.withPropertyValues("mmmq.broker.persistence.root-dir=" + tempDir)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    @DisplayName("알 수 없는 protocol → 기동에 실패한다")
    void failsOnUnknownProtocol() throws IOException {
        write("""
                [
                  {"consumerId":"x","host":{"protocol":"ftp","address":"127.0.0.1","port":8080},"pattern":"a"}
                ]
                """);

        runner.withPropertyValues("mmmq.broker.persistence.root-dir=" + tempDir)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    @DisplayName("빈 파일 → 기동에 실패한다")
    void failsOnEmptyFile() throws IOException {
        write("");

        runner.withPropertyValues("mmmq.broker.persistence.root-dir=" + tempDir)
                .run(context -> assertThat(context).hasFailed());
    }

    private void write(String json) throws IOException {
        Files.writeString(tempDir.resolve("dispatchers.json"), json);
    }

    private String readDispatchersFile() {
        try {
            return Files.readString(tempDir.resolve("dispatchers.json"));
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to read dispatcher file", exception);
        }
    }

    @Configuration
    @Import(DispatcherBeanRegistrar.class)
    static class TestConfig {

        @Bean
        DispatcherContainer dispatcherContainer(Collection<Dispatcher> dispatchers) {
            return new DispatcherContainer(dispatchers);
        }
    }
}
