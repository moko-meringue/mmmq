package org.mmmq.broker.dispatcher;

import org.mmmq.core.Host;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;

/**
 * 등록된 {@link Dispatcher}의 상태를 도메인 값 타입 그대로 내보내는 스냅샷.
 *
 * <p>{@link DispatcherContainer}가 조회·추가·수정의 결과로 돌려주며, {@code api} 계층이
 * 이것을 받아 HTTP 응답으로 옮긴다. {@link Dispatcher} 자체를 밖으로 내보내지 않으려고 둔다 —
 * 그러면 워커 풀과 전송 기능까지 딸려 나간다.
 *
 * <p>컴포넌트가 {@link org.mmmq.core.identifier.ConsumerId}·{@link org.mmmq.core.Host}·
 * {@link org.mmmq.core.message.TopicPattern}인 것이 문자열을 쓰는 바깥 계층 타입들과의 차이다.
 */
public record DispatcherSnapshot(
        ConsumerId consumerId,
        Host host,
        TopicPattern pattern
) {
}
