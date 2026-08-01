package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.dispatcher.exception.DispatcherNotFoundException;
import org.mmmq.broker.dispatcher.exception.DuplicateConsumerIdException;
import org.mmmq.broker.dispatcher.storage.DispatcherEntry;
import org.mmmq.broker.dispatcher.storage.DispatchersFile;
import org.mmmq.broker.persistence.PersistenceProperties;
import org.mmmq.core.Host;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;
import org.mockito.ArgumentCaptor;

class DispatcherContainerTest {

    private static final String HOST = "http://consumer-host:8080";

    @TempDir
    Path tempDir;

    DispatchersFile dispatchersFile;
    DispatcherRematcher rematcher;
    DispatcherContainer container;

    @BeforeEach
    void setUp() {
        PersistenceProperties properties = new PersistenceProperties(tempDir.toString(), null);
        dispatchersFile = new DispatchersFile(properties);
        rematcher = mock(DispatcherRematcher.class);
        container = new DispatcherContainer(dispatchersFile, rematcher);
    }

    @Test
    @DisplayName("생성 시 파일에서 읽은 Dispatcher 전체로 rematchAll을 호출한다")
    void constructionRematchesLoadedDispatchers() {
        write("""
                [
                  {"consumerId":"order-created","host":"http://127.0.0.1:8080","pattern":"order.created"}
                ]
                """);

        DispatcherRematcher freshRematcher = mock(DispatcherRematcher.class);
        new DispatcherContainer(dispatchersFile, freshRematcher);

        ArgumentCaptor<List<Dispatcher>> captor = ArgumentCaptor.forClass(List.class);
        verify(freshRematcher).rematchAll(captor.capture());
        assertThat(captor.getValue())
                .extracting(Dispatcher::consumerId)
                .containsExactly(new ConsumerId("order-created"));
    }

    @Test
    @DisplayName("추가하면 파일에 반영되고 현재 Dispatcher 전체로 rematchAll이 호출된다")
    void addPersistsAndRematches() {
        container.add(new ConsumerId("order-created"), Host.from(HOST), new TopicPattern("order.*"));

        assertThat(dispatchersFile.read())
                .containsExactly(new DispatcherEntry("order-created", HOST, "order.*"));
        // 생성자에서 한 번(빈 목록), add에서 한 번 — 총 두 번.
        verify(rematcher, times(2)).rematchAll(anyList());
    }

    @Test
    @DisplayName("중복 consumerId는 DuplicateConsumerIdException을 던지고 파일을 바꾸지 않는다")
    void rejectsDuplicateConsumerId() {
        container.add(new ConsumerId("order-created"), Host.from(HOST), new TopicPattern("order.*"));

        assertThatThrownBy(() -> container.add(
                new ConsumerId("order-created"), Host.from(HOST), new TopicPattern("other.*")))
                .isInstanceOf(DuplicateConsumerIdException.class);
        assertThat(dispatchersFile.read())
                .containsExactly(new DispatcherEntry("order-created", HOST, "order.*"));
    }

    @Test
    @DisplayName("host만 바꾸면 파일에 새 host가 쓰이고 rematchAll이 다시 호출된다")
    void modifyHostPersistsAndRematches() {
        container.add(new ConsumerId("order-created"), Host.from(HOST), new TopicPattern("order.*"));

        container.modify(
                new ConsumerId("order-created"),
                Host.from("http://moved-host:9090"),
                new TopicPattern("order.*"));

        assertThat(dispatchersFile.read())
                .containsExactly(new DispatcherEntry("order-created", "http://moved-host:9090", "order.*"));
        // 생성자·add·modify 각 한 번씩 — 총 세 번.
        verify(rematcher, times(3)).rematchAll(anyList());
    }

    @Test
    @DisplayName("삭제하면 파일에서 빠지고 rematchAll이 다시 호출된다")
    void removePersistsAndRematches() {
        container.add(new ConsumerId("consumer"), Host.from(HOST), new TopicPattern("**"));

        container.remove(new ConsumerId("consumer"));

        assertThat(dispatchersFile.read()).isEmpty();
        // 생성자·add·remove 각 한 번씩 — 총 세 번.
        verify(rematcher, times(3)).rematchAll(anyList());
    }

    @Test
    @DisplayName("다른 Dispatcher의 정의는 그대로 남는다")
    void mutationLeavesOtherDispatcherIntact() {
        container.add(new ConsumerId("keeper"), Host.from(HOST), new TopicPattern("stock.*"));
        container.add(new ConsumerId("leaver"), Host.from(HOST), new TopicPattern("stock.low"));

        assertThat(dispatchersFile.read()).containsExactly(
                new DispatcherEntry("keeper", HOST, "stock.*"),
                new DispatcherEntry("leaver", HOST, "stock.low"));

        container.modify(
                new ConsumerId("leaver"),
                Host.from("https://relocated-host:9443"),
                new TopicPattern("stock.low"));

        assertThat(dispatchersFile.read()).containsExactly(
                new DispatcherEntry("keeper", HOST, "stock.*"),
                new DispatcherEntry("leaver", "https://relocated-host:9443", "stock.low"));

        container.remove(new ConsumerId("leaver"));

        assertThat(dispatchersFile.read()).containsExactly(new DispatcherEntry("keeper", HOST, "stock.*"));
    }

    @Test
    @DisplayName("없는 consumerId로 수정하거나 삭제하면 DispatcherNotFoundException을 던진다")
    void rejectsUnknownConsumerId() {
        assertThatThrownBy(() ->
                container.modify(new ConsumerId("absent"), Host.from(HOST), new TopicPattern("**")))
                .isInstanceOf(DispatcherNotFoundException.class);
        assertThatThrownBy(() -> container.remove(new ConsumerId("absent")))
                .isInstanceOf(DispatcherNotFoundException.class);
    }

    @Test
    @DisplayName("파일 쓰기가 실패하면 메모리에 등록되지 않는다")
    void keepsMemoryIntactWhenWriteFails() {
        DispatchersFile failing = mock(DispatchersFile.class);
        when(failing.read()).thenReturn(List.of());
        doThrow(new IllegalStateException("disk full")).when(failing).write(anyList());
        DispatcherContainer failingContainer = new DispatcherContainer(failing, rematcher);

        assertThatThrownBy(() -> failingContainer.add(
                new ConsumerId("order-created"), Host.from(HOST), new TopicPattern("order.*")))
                .isInstanceOf(IllegalStateException.class);

        assertThat(failingContainer.snapshots()).isEmpty();
    }

    @Test
    @DisplayName("파일의 정의를 순서대로 읽어 Dispatcher를 만든다")
    void loadsDefinitionsFromFile() {
        write("""
                [
                  {"consumerId":"order-created","host":"http://127.0.0.1:8080","pattern":"order.created"},
                  {"consumerId":"order-shipped","host":"http://127.0.0.1:8080","pattern":"order.shipped"}
                ]
                """);

        DispatcherContainer loaded = new DispatcherContainer(dispatchersFile, mock(DispatcherRematcher.class));

        assertThat(loaded.snapshots()).containsExactly(
                new DispatcherSnapshot(
                        new ConsumerId("order-created"),
                        Host.from("http://127.0.0.1:8080"),
                        new TopicPattern("order.created")),
                new DispatcherSnapshot(
                        new ConsumerId("order-shipped"),
                        Host.from("http://127.0.0.1:8080"),
                        new TopicPattern("order.shipped")));
    }

    @Test
    @DisplayName("파일에 중복 consumerId가 있으면 생성에 실패한다")
    void failsOnDuplicateConsumerIdInFile() {
        write("""
                [
                  {"consumerId":"dup","host":"http://127.0.0.1:8080","pattern":"a"},
                  {"consumerId":"dup","host":"http://127.0.0.1:8080","pattern":"b"}
                ]
                """);

        assertThatThrownBy(() -> new DispatcherContainer(dispatchersFile, mock(DispatcherRematcher.class)))
                .isInstanceOf(DuplicateConsumerIdException.class);
    }

    private void write(String json) {
        try {
            Files.writeString(tempDir.resolve("dispatchers.json"), json);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to write dispatcher file", exception);
        }
    }
}
