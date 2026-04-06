package org.mmmq.broker.fixture;

import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;

import java.net.InetAddress;

public class HostFixture {

    public static Host localhost() {
        try {
            return new Host(WebProtocol.HTTP, "localhost", 8080) {
                @Override
                public boolean healthCheck(InetAddress host) {
                    return true;
                }
            };
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
