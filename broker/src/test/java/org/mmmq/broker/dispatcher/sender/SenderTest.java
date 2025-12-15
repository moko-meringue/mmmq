package org.mmmq.broker.dispatcher.sender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.fixture.HostFixture;
import org.mmmq.broker.fixture.MockRestServiceServerFixture;
import org.mmmq.core.Host;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.ConsumerAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.MessageDeliveryException;
import org.mmmq.core.message.Topic;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class SenderTest {

    RestClient restClient;
    MockRestServiceServer server;
    ObjectMapper objectMapper;
    Host host;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        host = HostFixture.localhost();
        RestClient tempClient = RestClient.builder()
                .baseUrl(host.toUri())
                .defaultStatusHandler(
                        status -> status.is4xxClientError() || status.is5xxServerError(),
                        (request, response) -> {
                            throw new MessageDeliveryException(
                                    "Failed to send message to consumer: " + response.getStatusCode().value(),
                                    response.getStatusCode().value()
                            );
                        }
                )
                .build();
        MockRestServiceServerFixture serverFixture = MockRestServiceServerFixture.create(tempClient);
        restClient = serverFixture.getRestClient();
        server = serverFixture.getMockServer();
    }

    @Test
    @DisplayName("특정 host에게 메시지를 전달할 수 있다.")
    void sendMessageTest() throws JsonProcessingException {
        Sender sender = new Sender(restClient);
        Host host = HostFixture.localhost();

        server.expect(ExpectedCount.once(), requestTo(host.toUri() + "/messages"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess(
                        objectMapper.writeValueAsString(new ConsumerAcknowledgement(Acknowledgement.ACK)),
                        MediaType.APPLICATION_JSON
                ));

        sender.send(new Message(new Topic("topic"), Map.of("key", "value")));

        server.verify();
    }

    @Test
    @DisplayName("MessageSender는 메시지를 전달한 후, Host로부터 응답(ACK/NAK)을 받을 수 있다.")
    void receiveResponseTest() throws JsonProcessingException {
        Sender sender = new Sender(restClient);

        server.expect(ExpectedCount.once(), requestTo(host.toUri() + "/messages"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(
                        withSuccess()
                                .body(objectMapper.writeValueAsBytes(
                                        new ConsumerAcknowledgement(Acknowledgement.ACK)))
                                .contentType(MediaType.APPLICATION_JSON)
                );

        assertThat(sender.send(new Message(new Topic("topic"), Map.of("key", "value")))).isTrue();
    }
}
