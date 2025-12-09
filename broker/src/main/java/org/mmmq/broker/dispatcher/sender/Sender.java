package org.mmmq.broker.dispatcher.sender;

import org.mmmq.core.acknowledgement.ConsumerAcknowledgement;
import org.mmmq.core.message.Message;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

public class Sender {

    final RestClient restClient;

    public Sender(RestClient restClient) {
        this.restClient = restClient;
    }

    public ConsumerAcknowledgement send(Message message) {
        return restClient.post()
                .uri("/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .body(message)
                .retrieve()
                .toEntity(ConsumerAcknowledgement.class)
                .getBody();
    }
}
