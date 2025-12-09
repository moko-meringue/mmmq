package org.mmmq.broker.dispatcher.sender;

import org.mmmq.core.Host;
import org.mmmq.core.message.MessageDeliveryException;
import org.springframework.web.client.RestClient;

public class SenderFactory {

    public static Sender create(Host host) {
        return new Sender(createRestClient(host));
    }

    private static RestClient createRestClient(Host host) {
        return RestClient.builder()
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
    }
}
