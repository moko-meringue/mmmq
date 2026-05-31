package org.mmmq.broker.dispatcher.sender;

import org.mmmq.core.Host;
import org.mmmq.core.acknowledgement.ConsumerAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.MessageDeliveryException;
import org.mmmq.core.metadata.Metadata;
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
                            throw new MessageDeliveryException("Failed to send message: " + response.getStatusText());
                        }
                )
                .build();
        return new Sender(restClient);
    }

    public boolean send(Message message, String handlerId, int maxRetryCount) {
        for (int attempt = 1; attempt <= maxRetryCount; attempt++) {
            if (post(message, handlerId).isAck()) {
                return true;
            }
        }
        return false;
    }

    ConsumerAcknowledgement post(Message message, String handlerId) {
        Metadata metadata = new Metadata();
        metadata.setHandlerId(handlerId);
        return restClient.post()
                .uri("/mmmq/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .headers(httpHeaders -> metadata.toMap().forEach(httpHeaders::set))
                .body(message)
                .retrieve()
                .toEntity(ConsumerAcknowledgement.class)
                .getBody();
    }
}
