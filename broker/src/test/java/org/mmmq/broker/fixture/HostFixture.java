package org.mmmq.broker.fixture;

import java.net.InetAddress;

import org.mmmq.core.Host;

public class HostFixture {

    public static Host localhost() {
        try {
            return new Host("http", "localhost", 8080) {
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
