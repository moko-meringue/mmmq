package org.mmmq.core;

public enum WebProtocol {

    HTTP("http"),
    HTTPS("https");

    private final String scheme;

    WebProtocol(String scheme) {
        this.scheme = scheme;
    }

    public static WebProtocol from(String scheme) {
        for (WebProtocol protocol : values()) {
            if (protocol.scheme.equalsIgnoreCase(scheme)) {
                return protocol;
            }
        }
        throw new IllegalArgumentException("scheme must be http or https, but was: " + scheme);
    }

    public String getScheme() {
        return scheme;
    }
}
