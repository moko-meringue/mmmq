package org.mmmq.broker.dispatcher;

import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;

public record DispatcherDefinition(
        String consumerId,
        HostDefinition host,
        String pattern
) {

    public Host toHost() {
        return host.toHost();
    }

    public record HostDefinition(
            String protocol,
            String address,
            int port
    ) {

        public Host toHost() {
            return new Host(WebProtocol.from(protocol), address, port);
        }
    }
}
