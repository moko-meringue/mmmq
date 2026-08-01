package org.mmmq.broker.dispatcher;

import java.util.List;

/**
 * Dispatcher 구성이 바뀔 때마다 구독을 다시 맞추는 통로.
 *
 * <p>{@link DispatcherContainer}가 add/modify/remove로 파일과 메모리를 갱신한 직후, 그리고 부팅 시
 * 파일을 읽어 초기 목록을 구성한 직후 현재 Dispatcher 전체를 넘겨 호출하고,
 * {@code subscription.SubscriptionContainer}가 구현해 이미 등록된 모든 TopicQueue의 구독을 다시 매칭한다.
 *
 * <p>이 인터페이스가 없으면 {@code dispatcher}가 {@code subscription}을 직접 알아야 해서
 * 두 패키지가 서로를 참조하게 된다. 의존 방향을 {@code subscription → dispatcher} 한쪽으로 두려고 있다.
 * {@link DispatcherContainer}가 구독자 목록을 스스로 들고 있지 않고 매번 현재 상태 전체를 넘기는
 * 이유이기도 하다 — {@code SubscriptionContainer}가 {@link DispatcherContainer}를 되받아 참조하면
 * 생성자 주입 순환이 되어 컨텍스트가 뜨지 않는다.
 */
public interface DispatcherRematcher {

    void rematchAll(List<Dispatcher> dispatchers);
}
