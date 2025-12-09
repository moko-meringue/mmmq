package org.mmmq.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withBadRequest;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.net.InetAddress;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.Host;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.GatewayAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.MessageDeliveryException;
import org.springframework.boot.test.web.client.MockServerRestClientCustomizer;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

class GatewayTest {

    private RestClient restClient;
    private MockRestServiceServer server;
    private ObjectMapper objectMapper;
    private Host host;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        host = createTestHost();
        RestClient tempClient = RestClient.builder()
                .baseUrl(host.toUri())
                .defaultStatusHandler(
                        status -> status.is4xxClientError() || status.is5xxServerError(),
                        (request, response) -> {
                            throw new MessageDeliveryException(
                                    "Failed to send message to gateway: " + response.getStatusCode().value(),
                                    response.getStatusCode().value()
                            );
                        }
                )
                .build();
        
        RestClient.Builder builder = tempClient.mutate();
        MockServerRestClientCustomizer customizer = new MockServerRestClientCustomizer();
        customizer.customize(builder);
        server = customizer.getServer();
        restClient = builder.build();
    }

    private Host createTestHost() {
        return new Host("http", "localhost", 8080) {
            @Override
            public boolean healthCheck(InetAddress host) {
                return true;
            }
        };
    }

    @Test
    @DisplayName("Gateway 객체를 Host로 생성할 수 있다")
    void createsGatewayWithHost() {
        Gateway gateway = new Gateway(host);

        assertThat(gateway.host).isEqualTo(host);
        assertThat(gateway.restClient).isNotNull();
    }

    @Test
    @DisplayName("메시지 전송 성공시 ACK 응답을 받는다")
    void successReturnsAck() throws JsonProcessingException {
        Gateway gateway = new Gateway(host);
        Message message = new Message("test-topic", Map.of("key", "value"));
        GatewayAcknowledgement expectedResponse = new GatewayAcknowledgement(Acknowledgement.ACK);

        server.expect(ExpectedCount.once(), requestTo(host.toUri() + "/messages"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess(
                        objectMapper.writeValueAsString(expectedResponse),
                        MediaType.APPLICATION_JSON
                ));

        gateway.restClient = restClient;
        GatewayAcknowledgement result = gateway.send(message);

        assertThat(result.isAck()).isTrue();
        server.verify();
    }

    @Test
    @DisplayName("4xx 에러 응답시 MessageDeliveryException을 던진다")
    void throwsExceptionOn4xxError() {
        Gateway gateway = new Gateway(host);
        Message message = new Message("test-topic", Map.of("key", "value"));

        server.expect(ExpectedCount.once(), requestTo(host.toUri() + "/messages"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withBadRequest().body("Bad request"));

        gateway.restClient = restClient;

        assertThatThrownBy(() -> gateway.send(message))
                .isInstanceOf(MessageDeliveryException.class);

        server.verify();
    }

    @Test
    @DisplayName("5xx 에러 응답시 MessageDeliveryException을 던진다")
    void throwsExceptionOn5xxError() {
        Gateway gateway = new Gateway(host);
        Message message = new Message("test-topic", Map.of("key", "value"));

        server.expect(ExpectedCount.once(), requestTo(host.toUri() + "/messages"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withServerError());

        gateway.restClient = restClient;

        assertThatThrownBy(() -> gateway.send(message))
                .isInstanceOf(MessageDeliveryException.class);

        server.verify();
    }
}
