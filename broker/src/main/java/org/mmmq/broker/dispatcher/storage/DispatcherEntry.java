package org.mmmq.broker.dispatcher.storage;

/**
 * {@code dispatchers.json} 한 행의 스키마. 컴포넌트 이름이 곧 JSON 키다.
 *
 * <p>{@link DispatchersFile}이 읽고 쓰며, {@link org.mmmq.broker.dispatcher.DispatcherContainer}가
 * 도메인 객체와의 변환을 맡는다. 컴포넌트 이름을 바꾸면 기존 파일이 조용히 빈 값으로 읽히므로
 * 디스크 호환성을 깨는 변경이다.
 *
 * <p>HTTP가 쓰는 {@code api.DispatcherDefinition}과 필드가 같지만 계층이 달라 따로 둔다.
 */
public record DispatcherEntry(
        String consumerId,
        String host,
        String pattern
) {
}
