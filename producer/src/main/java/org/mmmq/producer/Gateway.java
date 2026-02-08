package org.mmmq.producer;

import org.mmmq.core.Host;
import org.mmmq.core.acknowledgement.BrokerAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.MessageDeliveryException;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

class Gateway {

    final Host host;
    RestClient restClient;

    Gateway(Host host) {
        this.host = host;
        this.restClient = createRestClient(host);
    }

    private static RestClient createRestClient(Host host) {
        return RestClient.builder()
                .baseUrl(host.toUri())
                .defaultStatusHandler(
                        status -> status.is4xxClientError() || status.is5xxServerError(),
                        (request, response) -> {
                            throw new MessageDeliveryException(
                                    "Failed to send message to gateway: " + response.getStatusCode().value(), null
                            );
                        }
                )
                .build();
    }

    public BrokerAcknowledgement send(Message message) {
        return restClient.post()
                .uri("/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .body(message)
                .retrieve()
                .toEntity(BrokerAcknowledgement.class)
                .getBody();
    }
}
