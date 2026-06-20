package org.mmmq.broker.dispatcher;

public record DispatcherDefinition(
        String consumerId,
        HostDefinition host,
        String pattern
) {
}
