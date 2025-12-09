package org.mmmq.gateway.dispatcher;

import org.mmmq.core.Host;
import org.mmmq.core.message.MessageDeliveryException;
import org.springframework.web.client.RestClient;

public class MessageSenderFactory {

    public static MessageSender create(Host host) {
        return new MessageSender(createRestClient(host));
    }

    private static RestClient createRestClient(Host host) {
        return RestClient.builder()
                .baseUrl(host.toUri())
                .defaultStatusHandler(
                        status -> status.is4xxClientError() || status.is5xxServerError(),
                        (request, response) -> {
                            throw new MessageDeliveryException(
                                    "Failed to send message to subscriber: " + response.getStatusCode().value(),
                                    response.getStatusCode().value()
                            );
                        }
                )
                .build();
    }
}
