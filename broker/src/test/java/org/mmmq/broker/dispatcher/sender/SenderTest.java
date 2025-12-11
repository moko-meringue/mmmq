package org.mmmq.broker.dispatcher.sender;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.util.Map;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    // @Test
    // @DisplayName("ACK가 오면 메시지를 재전송하지 않는다.")
    // void ackTest() throws Exception {
    //     dispatcher.start();
    //     Sender sender = mock(Sender.class);
    //     Message message = new Message(new Topic("test"), Map.of("key", "value"));
    //     when(sender.send(message)).thenReturn(true);
    //     Field filed = Dispatcher.class.getDeclaredField("sender");
    //     filed.setAccessible(true);
    //     filed.set(dispatcher, sender);
    //
    //     dispatcher.push(message);
    //
    //     Thread.sleep(500L);
    //     verify(sender, times(1)).send(message);
    // }

    // @Test
    // @DisplayName("NAK가 오면 메시지를 3회 재전송한다.")
    // void nakTest() throws Exception {
    //     dispatcher.start();
    //     Sender sender = mock(Sender.class);
    //     Message message = new Message(new Topic("test"), Map.of("key", "value"));
    //     when(sender.send(message)).thenReturn(false);
    //     Field filed = Dispatcher.class.getDeclaredField("sender");
    //     filed.setAccessible(true);
    //     filed.set(dispatcher, sender);
    //
    //     dispatcher.push(message);
    //
    //     Thread.sleep(1000L);
    //     verify(sender, times(1 + Dispatcher.MAX_RETRY_COUNT)).send(message);
    // }
}
