package org.mmmq.broker.dispatcher.api;

/**
 * PUT 요청 본문. 기존 Dispatcher의 전송 대상과 구독 범위를 바꾼다.
 *
 * <p>{@code consumerId}가 없는 것은 수정 대상 식별자가 경로 변수로 오기 때문이다 —
 * 본문에 두면 경로와 어긋나는 요청을 어떻게 처리할지가 새 문제가 된다.
 */
public record DispatcherRoute(
        String host,
        String pattern
) {
}
