package org.mmmq.broker.dispatcher;

import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;

public record DispatcherDefinition(

        String consumerId,
        HostDefinition host,
        String pattern
) {

    public Dispatcher toDispatcher() {
        return new Dispatcher(
                host.toHost(),
                new ConsumerId(consumerId),
                new TopicPattern(pattern)
        );
    }
}
