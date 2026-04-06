package org.mmmq.core;

public enum WebProtocol {

    HTTP("http"),
    HTTPS("https");

    private final String scheme;

    WebProtocol(String scheme) {
        this.scheme = scheme;
    }

    public String getScheme() {
        return scheme;
    }
}
