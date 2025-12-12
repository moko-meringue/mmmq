package org.mmmq.broker.dispatcher.sender;

import org.mmmq.core.Host;
import org.mmmq.core.acknowledgement.ConsumerAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.MessageDeliveryException;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

public class Sender {

    final RestClient restClient;

    public Sender(RestClient restClient) {
        this.restClient = restClient;
    }

    public static Sender from(Host host) {
        RestClient restClient = RestClient.builder()
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
        return new Sender(restClient);
    }

    public boolean send(Message message, int maxRetryCount) {
        for (int tryCount = 0; tryCount < maxRetryCount; tryCount++) {
            try {
                if (send(message)) {
                    return true;
                }
            } catch (Exception e) {
                if (tryCount == maxRetryCount - 1) {
                    throw e;
                }
            }
        }
        return false;
    }

    public boolean send(Message message) {
        return post(message).isAck();
    }

    ConsumerAcknowledgement post(Message message) {
        return restClient.post()
                .uri("/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .body(message)
                .retrieve()
                .toEntity(ConsumerAcknowledgement.class)
                .getBody();
    }
}
