package org.mmmq.broker.dispatcher;

import java.util.Locale;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;

public record HostDefinition(

        String protocol,
        String address,
        int port
) {

    public HostDefinition {
        if (address == null || address.isBlank()) {
            throw new IllegalArgumentException("host.address must not be null or blank");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("host.port must be between 1 and 65535, but was: " + port);
        }
    }

    public Host toHost() {
        return new Host(WebProtocol.valueOf(protocol.toUpperCase(Locale.ROOT)), address, port);
    }
}
