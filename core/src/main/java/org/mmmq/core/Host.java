package org.mmmq.core;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class Host {

    final WebProtocol protocol;
    final InetAddress address;
    final int port;

    public Host(WebProtocol webProtocol, String address, int port) {
        this.protocol = webProtocol;
        this.address = convertAddress(address);
        this.port = port;
    }

    private InetAddress convertAddress(String address) {
        try {
            InetAddress inetAddress = InetAddress.getByName(address);
            if (!healthCheck(inetAddress)) {
                throw new IllegalArgumentException("Host is not reachable: " + inetAddress);
            }
            return inetAddress;
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public boolean healthCheck(InetAddress host) {
        try {
            return host.isReachable(5000);
        } catch (Exception ignored) {
            return false;
        }
    }

    public String toUri() {
        return String.format("%s://%s:%d", protocol.getScheme(), address.getHostAddress(), port);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Host host)) {
            return false;
        }
        return Objects.equals(address, host.address);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(address);
    }
}
