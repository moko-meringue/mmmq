package org.mmmq.broker.subscription;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.dispatcher.DispatcherRematcher;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueFactory;
import org.mmmq.broker.topicqueue.TopicQueueRegistrar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 모든 구독의 소유자. (TopicQueue, Dispatcher) 짝의 상태 — 오프셋·체크포인트·워커 — 가
 * 흩어지지 않고 여기 하나로 모이게 하는 자리다.
 *
 * <p>{@link TopicQueueRegistrar}로서 새 TopicQueue가 생길 때 현재 알고 있는 Dispatcher와 매칭하고,
 * {@link DispatcherRematcher}로서 Dispatcher 구성이 바뀔 때 이미 등록된 모든 TopicQueue를 다시 매칭한다.
 * 매칭 대상 Dispatcher 목록을 자체 캐시({@code dispatchers})로 들고 있는 이유는
 * {@link org.mmmq.broker.dispatcher.DispatcherContainer}를 되받아 참조할 수 없어서다 — 그러면
 * 둘 다 생성자 주입으로 서로를 필요로 하는 순환이 되어 컨텍스트가 뜨지 않는다. 그래서 최신 상태를
 * 매번 push 받아 캐시해 두고, 새 TopicQueue가 register될 때 이 캐시로 매칭한다.
 *
 * <p>{@link #trigger}는 메시지 도착마다 호출되는 핫패스라 락을 타지 않는다 — {@link TopicQueue}별
 * {@link TopicSubscriptions}를 {@link ConcurrentHashMap}에서 읽기만 하고, 실제 구독 목록 교체는
 * {@link TopicSubscriptions} 내부에서 volatile 필드 swap으로 이뤄진다. 뮤테이션(register·rematchAll·close)은
 * private {@link ReentrantLock}으로 직렬화한다 — {@code synchronized(this)}를 쓰면 이 클래스가
 * public 스프링 빈이라 외부 코드가 인스턴스를 락으로 잡아 핫패스를 막을 수 있어서, 이 저장소의
 * 다른 컨테이너들({@code TopicQueue.writeLock}, {@code DispatcherContainer.mutationLock})과 같은 형태를 쓴다.
 *
 * <p>{@link #close()}를 {@code @PreDestroy}가 아니라 {@link ContextClosedEvent} 리스너로 두는 이유는
 * {@link org.mmmq.broker.topicqueue.TopicQueueContainer}가 생성자로 이 빈을 참조하고 있어서다 — 스프링은
 * 의존받는 빈을 나중에 소멸시키므로 {@code @PreDestroy}로는 {@code TopicQueueContainer.destroy()}(세그먼트
 * 파일을 닫는다)가 먼저 불리는 순서가 고정된다. 그러면 큐가 닫힌 뒤에도 drain 워커가 남아 있다가 닫힌
 * 파일을 읽어 예외를 낸다. {@code ContextClosedEvent}는 어떤 빈의 소멸 전에 발행되므로, 이걸로 옮기면
 * 워커가 먼저 멈추고 그다음에 큐가 닫힌다(실측: {@code ShutdownOrderTest}).
 */
@Component
public class SubscriptionContainer implements TopicQueueRegistrar, DispatcherRematcher {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionContainer.class);

    private final TopicQueueFactory topicQueueFactory;
    private final Map<TopicQueue, TopicSubscriptions> topicSubscriptions = new ConcurrentHashMap<>();
    private final ReentrantLock mutationLock = new ReentrantLock();
    private List<Dispatcher> dispatchers = List.of();

    public SubscriptionContainer(TopicQueueFactory topicQueueFactory) {
        this.topicQueueFactory = topicQueueFactory;
    }

    @Override
    public void register(TopicQueue topicQueue) {
        mutationLock.lock();
        try {
            TopicSubscriptions subscriptions = new TopicSubscriptions(
                    topicQueueFactory.openCheckpointDirectory(topicQueue.getTopic())
            );
            subscriptions.rematch(topicQueue, matched(topicQueue));
            topicSubscriptions.put(topicQueue, subscriptions);
        } finally {
            mutationLock.unlock();
        }
    }

    @Override
    public void rematchAll(List<Dispatcher> dispatchers) {
        mutationLock.lock();
        try {
            this.dispatchers = List.copyOf(dispatchers);
            topicSubscriptions.forEach((topicQueue, subscriptions) -> subscriptions.rematch(topicQueue, matched(topicQueue)));
        } finally {
            mutationLock.unlock();
        }
    }

    public void trigger(TopicQueue topicQueue) {
        TopicSubscriptions subscriptions = topicSubscriptions.get(topicQueue);
        if (subscriptions != null) {
            subscriptions.trigger();
        }
    }

    @EventListener(ContextClosedEvent.class)
    public void close() {
        mutationLock.lock();
        try {
            topicSubscriptions.values().forEach(TopicSubscriptions::close);
            log.info("Subscription workers stopped");
        } finally {
            mutationLock.unlock();
        }
    }

    private List<Dispatcher> matched(TopicQueue topicQueue) {
        return dispatchers.stream()
                .filter(dispatcher -> dispatcher.canDispatch(topicQueue.getTopic()))
                .toList();
    }
}
