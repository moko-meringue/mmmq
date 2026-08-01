package org.mmmq.core;

import java.net.URI;

/**
 * 메시지를 보낼 상대의 주소. {@code Sender}·{@code Gateway}가 요청 baseUrl로 쓴다.
 *
 * <p>주소를 이름 그대로 들고 있고 해석하지 않는다 — DNS에 아직 없는 소비자도 등록할 수 있어야 하고,
 * IP가 바뀌면 재기동 없이 따라가야 하기 때문이다. 실제 해석은 요청 시점에 HTTP 클라이언트가 한다.
 *
 * <p>{@link #from(String)}은 {@code scheme://address:port} 꼴 문자열을 해석하는 진입점이고,
 * 설정 파일과 관리 API가 host를 URL 한 줄로 표현하기 때문에 있다.
 */
public record Host(
        WebProtocol protocol,
        String address,
        int port
) {

    public Host {
        if (address == null || address.isBlank()) {
            throw new IllegalArgumentException("address must not be blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be in 1..65535, but was: " + port);
        }
    }

    public static Host from(String url) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("url must not be blank");
        }
        URI uri = URI.create(url);
        if (uri.getScheme() == null || uri.getHost() == null) {
            throw new IllegalArgumentException("url must be an absolute URL, but was: " + url);
        }
        if (uri.getPort() == -1) {
            throw new IllegalArgumentException("url must include a port, but was: " + url);
        }
        if (!uri.getPath().isEmpty() || uri.getQuery() != null
                || uri.getUserInfo() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException(
                    "url must be scheme://address:port only, but was: " + url
            );
        }
        return new Host(WebProtocol.from(uri.getScheme()), uri.getHost(), uri.getPort());
    }

    public String toUri() {
        return String.format("%s://%s:%d", protocol.getScheme(), address, port);
    }
}
