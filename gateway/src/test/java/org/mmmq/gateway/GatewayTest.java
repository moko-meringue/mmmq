package org.mmmq.gateway;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.message.Message;
import org.mmmq.gateway.broker.Broker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = GatewayTest.TestConfiguration.class
)
class GatewayTest {

    @LocalServerPort
    int port;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        RestAssured.port = port;
    }

    @Test
    @DisplayName("Manager는 외부로부터 메시지를 받을 수 있다.")
    void receiveMessageTest() throws JsonProcessingException {
        Message message = new Message("topic", Map.of("key", "value"));
        RestAssured.given().log().all()
                .body(objectMapper.writeValueAsString(message))
                .contentType("application/json")
                .when().log().all()
                .post("/messages")
                .then().log().all()
                .statusCode(200);
    }

    @Test
    @DisplayName("Manager는 전달받은 메시지를 브로커에게 전달할 수 있다.")
    void forwardToBrokerTest() {
        Message message = new Message("topic", Map.of("key", "value"));
        Broker broker = mock(Broker.class);
        Gateway Gateway = new Gateway(broker);

        Gateway.postMessage(message);

        verify(broker).push(message);
    }

    @Configuration
    @EnableAutoConfiguration
    public static class TestConfiguration {

    }
}
