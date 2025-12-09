package org.mmmq.publisher;

import org.mmmq.core.Host;
import org.mmmq.core.acknowledgement.GatewayAcknowledgement;
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
                        "Failed to send message to gateway: " + response.getStatusCode().value(),
                        response.getStatusCode().value()
                    );
                }
            )
            .build();
    }

    public GatewayAcknowledgement send(Message message) {
        return restClient.post()
            .uri("/messages")
            .contentType(MediaType.APPLICATION_JSON)
            .body(message)
            .retrieve()
            .toEntity(GatewayAcknowledgement.class)
            .getBody();
    }
}
