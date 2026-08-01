package org.mmmq.broker.dispatcher.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.dispatcher.DispatcherContainer;
import org.mmmq.broker.dispatcher.DispatcherSnapshot;
import org.mmmq.broker.dispatcher.exception.DispatcherNotFoundException;
import org.mmmq.broker.dispatcher.exception.DuplicateConsumerIdException;
import org.mmmq.core.Host;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class DispatcherControllerTest {

    private static final String HOST = "http://consumer-host:8080";

    DispatcherContainer container;
    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        container = mock(DispatcherContainer.class);
        mockMvc = MockMvcBuilders.standaloneSetup(new DispatcherController(container)).build();
    }

    @Test
    @DisplayName("GET은 200과 현재 정의 목록을 돌려준다")
    void getReturnsDefinitions() throws Exception {
        when(container.snapshots()).thenReturn(List.of(new DispatcherSnapshot(
                new ConsumerId("order-created"), Host.from(HOST), new TopicPattern("order.*"))));

        mockMvc.perform(get("/mmmq/dispatchers"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].consumerId").value("order-created"))
                .andExpect(jsonPath("$[0].host").value(HOST))
                .andExpect(jsonPath("$[0].pattern").value("order.*"));
    }

    @Test
    @DisplayName("POST 성공 시 201과 등록된 정의를 돌려준다")
    void postReturnsCreated() throws Exception {
        when(container.add(
                eq(new ConsumerId("order-created")),
                eq(Host.from(HOST)),
                eq(new TopicPattern("order.*"))
        )).thenReturn(new DispatcherSnapshot(
                new ConsumerId("order-created"), Host.from(HOST), new TopicPattern("order.*")));

        mockMvc.perform(post("/mmmq/dispatchers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"consumerId":"order-created","host":"HTTP://consumer-host:8080","pattern":"order.*"}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.consumerId").value("order-created"))
                .andExpect(jsonPath("$.host").value(HOST));
    }

    @Test
    @DisplayName("POST에서 중복 consumerId는 409를 돌려준다")
    void postReturnsConflictOnDuplicate() throws Exception {
        when(container.add(any(), any(), any()))
                .thenThrow(new DuplicateConsumerIdException(new ConsumerId("order-created")));

        mockMvc.perform(post("/mmmq/dispatchers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"consumerId":"order-created","host":"http://consumer-host:8080","pattern":"order.*"}
                                """))
                .andExpect(status().isConflict());
    }

    @Test
    @DisplayName("PUT 성공 시 200과 바뀐 정의를 돌려준다")
    void putReturnsOk() throws Exception {
        when(container.modify(
                eq(new ConsumerId("order-shipped")),
                eq(Host.from("http://moved-host:9090")),
                eq(new TopicPattern("order.*"))
        )).thenReturn(new DispatcherSnapshot(
                new ConsumerId("order-shipped"), Host.from("http://moved-host:9090"), new TopicPattern("order.*")));

        mockMvc.perform(put("/mmmq/dispatchers/order-shipped")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"host":"http://moved-host:9090","pattern":"order.*"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.host").value("http://moved-host:9090"));
    }

    @Test
    @DisplayName("PUT에서 없는 consumerId는 404를 돌려준다")
    void putReturnsNotFound() throws Exception {
        when(container.modify(any(), any(), any()))
                .thenThrow(new DispatcherNotFoundException(new ConsumerId("absent")));

        mockMvc.perform(put("/mmmq/dispatchers/absent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"host":"http://consumer-host:8080","pattern":"order.*"}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("DELETE 성공 시 204를 돌려준다")
    void deleteReturnsNoContent() throws Exception {
        mockMvc.perform(delete("/mmmq/dispatchers/order-created"))
                .andExpect(status().isNoContent());

        verify(container).remove(new ConsumerId("order-created"));
    }

    @Test
    @DisplayName("경로 변수의 consumerId가 regex에 어긋나면 400을 돌려준다")
    void rejectsInvalidConsumerIdInPath() throws Exception {
        mockMvc.perform(delete("/mmmq/dispatchers/invalid+id"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("예상하지 못한 예외는 500을 돌려주고 내부 메시지를 노출하지 않는다")
    void unexpectedFailureReturnsInternalServerError() throws Exception {
        when(container.snapshots())
                .thenThrow(new IllegalStateException("Failed to write dispatcher file: /var/mmmq/dispatchers.json"));

        mockMvc.perform(get("/mmmq/dispatchers"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().string(not(containsString("/var/mmmq"))));
    }
}
