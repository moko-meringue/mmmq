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
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
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
                                    "Failed to send message to consumer: " + response.getStatusCode().value()
                            );
                        }
                )
                .build();
        MockRestServiceServerFixture serverFixture = MockRestServiceServerFixture.create(tempClient);
        restClient = serverFixture.getRestClient();
        server = serverFixture.getMockServer();
    }

    @Test
    @DisplayName("메시지 전송 성공(ACK) 시 true를 반환한다.")
    void sendSuccessReturnTrueTest() throws JsonProcessingException {
        Sender sender = new Sender(restClient);

        server.expect(ExpectedCount.once(), requestTo(host.toUri() + "/messages"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess(
                        objectMapper.writeValueAsString(new ConsumerAcknowledgement(Acknowledgement.ACK)),
                        MediaType.APPLICATION_JSON
                ));

        boolean result = sender.send(new Message(new Topic("topic"), Map.of("key", "value")), 1);

        assertThat(result).isTrue();
        server.verify();
    }

    @Test
    @DisplayName("메시지 전송 실패(NACK) 후 재시도 횟수 초과 시 false를 반환한다.")
    void sendNackReturnFalseAfterRetriesTest() throws JsonProcessingException {
        Sender sender = new Sender(restClient);

        server.expect(ExpectedCount.twice(), requestTo(host.toUri() + "/messages"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess(
                        objectMapper.writeValueAsString(new ConsumerAcknowledgement(Acknowledgement.NACK)),
                        MediaType.APPLICATION_JSON
                ));

        boolean result = sender.send(new Message(new Topic("topic"), Map.of("key", "value")), 2);

        assertThat(result).isFalse();
        server.verify();
    }

    @Test
    @DisplayName("NACK 발생 시 maxRetryCount만큼 재시도한다.")
    void retryOnNackTest() throws JsonProcessingException {
        Sender sender = new Sender(restClient);

        // 첫 번째 시도는 NACK, 두 번째 시도는 ACK
        server.expect(ExpectedCount.once(), requestTo(host.toUri() + "/messages"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess(
                        objectMapper.writeValueAsString(new ConsumerAcknowledgement(Acknowledgement.NACK)),
                        MediaType.APPLICATION_JSON
                ));
        server.expect(ExpectedCount.once(), requestTo(host.toUri() + "/messages"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess(
                        objectMapper.writeValueAsString(new ConsumerAcknowledgement(Acknowledgement.ACK)),
                        MediaType.APPLICATION_JSON
                ));

        boolean result = sender.send(new Message(new Topic("topic"), Map.of("key", "value")), 3);

        assertThat(result).isTrue();
        server.verify();
    }

    @Test
    @DisplayName("통신 장애 발생 시 즉시 예외를 던진다.")
    void throwExceptionOnCommunicationFailureTest() {
        Sender sender = new Sender(restClient);

        server.expect(ExpectedCount.once(), requestTo(host.toUri() + "/messages"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.INTERNAL_SERVER_ERROR));

        assertThatThrownBy(() -> sender.send(new Message(new Topic("topic"), Map.of("key", "value")), 3))
                .isInstanceOf(MessageDeliveryException.class);
    }
}
