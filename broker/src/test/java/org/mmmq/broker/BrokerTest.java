package org.mmmq.broker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.dispatcher.FrontDispatcher;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = BrokerTest.TestConfiguration.class
)
class BrokerTest {

    @TempDir
    static Path tempDataDir;

    @LocalServerPort
    int port;

    @Autowired
    ObjectMapper objectMapper;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("mmmq.broker.storage.root-dir", () -> tempDataDir.toAbsolutePath().toString());
    }

    @BeforeEach
    void setup() {
        RestAssured.port = port;
    }

    @Test
    @DisplayName("Manager는 외부로부터 메시지를 받을 수 있다.")
    void receiveMessageTest() throws JsonProcessingException {
        final Message message = new Message(new Topic("topic"), Map.of("key", "value"));
        RestAssured.given().log().all()
                .body(objectMapper.writeValueAsString(message))
                .contentType("application/json")
                .when().log().all()
                .post("/mmmq/messages")
                .then().log().all()
                .statusCode(200);
    }

    @Test
    @DisplayName("Manager는 전달받은 메시지를 브로커에게 전달할 수 있다.")
    void forwardToBrokerTest() {
        final Message message = new Message(new Topic("topic"), Map.of("key", "value"));
        final FrontDispatcher frontDispatcher = mock(FrontDispatcher.class);
        final Broker broker = new Broker(frontDispatcher);

        broker.postMessage(message);

        verify(frontDispatcher).dispatch(message);
    }

    @Configuration
    @EnableAutoConfiguration
    public static class TestConfiguration {

    }
}
