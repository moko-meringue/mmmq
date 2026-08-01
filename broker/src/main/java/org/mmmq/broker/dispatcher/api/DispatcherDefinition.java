package org.mmmq.broker.dispatcher.api;

import org.mmmq.broker.dispatcher.DispatcherSnapshot;

/**
 * Dispatcher 관리 API의 요청·응답 본문. 컴포넌트 이름이 곧 JSON 키다.
 *
 * <p>{@link DispatcherController}가 POST 본문으로 받고 네 엔드포인트 모두의 응답으로 내보낸다.
 * 값이 전부 문자열인 것은 이것이 파싱 이전의 입력이기 때문이고, 도메인 값 타입으로의 해석은
 * 컨트롤러가 경계에서 한다.
 *
 * <p>{@link org.mmmq.broker.dispatcher.storage.DispatcherEntry}와 컴포넌트가 같지만 따로 두는 이유는
 * 한 클래스가 하나의 직렬화 정책만 가질 수 있어서다 — 공유하면 HTTP 표현을 바꿀 때 디스크 포맷이 함께 바뀐다.
 */
public record DispatcherDefinition(
        String consumerId,
        String host,
        String pattern
) {

    public static DispatcherDefinition from(DispatcherSnapshot snapshot) {
        return new DispatcherDefinition(
                snapshot.consumerId().value(),
                snapshot.host().toUri(),
                snapshot.pattern().value()
        );
    }
}
