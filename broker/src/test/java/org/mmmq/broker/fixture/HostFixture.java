package org.mmmq.broker.fixture;

import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;

public class HostFixture {

    public static Host localhost() {
        return new Host(WebProtocol.HTTP, "localhost", 8080);
    }
}
