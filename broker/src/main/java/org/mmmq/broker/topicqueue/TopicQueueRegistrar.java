package org.mmmq.broker.topicqueue;

/**
 * 새로 만들어진 {@link TopicQueue}를 구독자와 이어 주는 통로.
 *
 * <p>{@link TopicQueueContainer}가 큐를 지연 생성할 때마다 호출하고,
 * {@code subscription.SubscriptionContainer}가 구현해 매칭되는 Dispatcher를 붙인다.
 *
 * <p>이 인터페이스가 없으면 {@code topicqueue}가 {@code subscription}을 직접 알아야 해서
 * 두 패키지가 서로를 참조하게 된다. 의존 방향을 {@code subscription → topicqueue} 한쪽으로 두려고 있다.
 */
public interface TopicQueueRegistrar {

    void register(TopicQueue topicQueue);
}
