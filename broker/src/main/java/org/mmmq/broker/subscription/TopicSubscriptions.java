package org.mmmq.broker.subscription;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.core.identifier.ConsumerId;

/**
 * 토픽 하나에 딸린 {@link CheckpointDirectory}와 {@link Subscription} 목록을 한 쌍으로 묶는다.
 *
 * <p>둘 다 같은 키({@link TopicQueue})로 색인되고 반드시 함께 바뀌어야 한다 — 구독이 빠지면
 * 그 체크포인트 파일도 같이 사라져야 하는데, 두 컬렉션이 따로 놀면 그 동기화가 규약으로만
 * 유지된다. 이 타입이 존재하는 이유는 그 불변식을 코드가 강제하게 만드는 것, 그 하나다.
 *
 * <p>{@link #subscriptions}를 통째로 교체하는 방식(volatile 필드에 불변 리스트를 swap)으로 바꾸는 이유는
 * {@link #trigger()}가 메시지 핫패스라 락을 타지 않아야 해서다 — 뮤테이션은 {@link SubscriptionContainer}가
 * 자기 락 안에서 순차적으로만 호출한다.
 *
 * <p>{@link #rematch}는 같은 consumerId라도 {@link Dispatcher} 인스턴스가 바뀌었는지(참조 동등성)를 구분한다 —
 * {@code DispatcherContainer.modify}가 host/pattern이 바뀔 때마다 같은 consumerId로 새 인스턴스를 만들기 때문이다.
 * 인스턴스가 그대로면 손대지 않고, 바뀌었으면 워커만 새로 열고 체크포인트는 지우지 않는다 — 그래야
 * {@link Subscription#open}이 기존 오프셋을 읽어 밀린 메시지부터 이어 보낸다. {@code equals}를 Dispatcher에
 * 주지 않고 참조 동등성을 쓰는 이유는 Dispatcher가 {@code Sender}라는 살아있는 협력자를 들고 있어서다 —
 * 값 기반 동등성은 "같은 설정을 가리키는 서로 다른 전송 주체"를 같다고 오판할 위험이 있다.
 */
class TopicSubscriptions {

    private final CheckpointDirectory checkpointDirectory;
    private volatile List<Subscription> subscriptions = List.of();

    TopicSubscriptions(CheckpointDirectory checkpointDirectory) {
        this.checkpointDirectory = checkpointDirectory;
    }

    void trigger() {
        subscriptions.forEach(Subscription::trigger);
    }

    void rematch(TopicQueue topicQueue, List<Dispatcher> matched) {
        Map<ConsumerId, Dispatcher> matchedByConsumerId = matched.stream()
                .collect(Collectors.toMap(Dispatcher::consumerId, dispatcher -> dispatcher));

        // 1회차: 더 이상 쓰이지 않는 옛 구독을 먼저 정리한다. matched에서 아예 빠졌으면 체크포인트도 지우고,
        // 인스턴스만 교체됐으면(체크포인트는 새 Subscription이 이어받아야 하므로) 워커만 종료한다.
        // 새 Subscription을 열기(2회차) 전에 옛 워커부터 끊어야, 옛 워커가 체크포인트에 쓰다가 인터럽트를
        // 맞을지언정 적어도 새 구독은 그 인터럽트가 지나간 뒤의(더 최신일 수 있는) 값을 읽는다.
        subscriptions.forEach(subscription -> {
            Dispatcher candidate = matchedByConsumerId.get(subscription.dispatcher().consumerId());
            if (candidate == subscription.dispatcher()) {
                return;
            }
            subscription.close();
            if (candidate == null) {
                checkpointDirectory.deregister(subscription.dispatcher().consumerId().value());
            }
        });

        // 2회차: 여전히 매칭되는 구독만 남긴다 — 인스턴스가 그대로면 그 객체를, 바뀌었으면 위에서 이미
        // 워커를 끈 뒤이므로 새 Subscription을 연다. map은 변환만 하고 부수효과는 위 forEach 한 곳에서만 한다.
        List<Subscription> kept = subscriptions.stream()
                .filter(subscription -> matchedByConsumerId.containsKey(subscription.dispatcher().consumerId()))
                .map(subscription -> {
                    Dispatcher candidate = matchedByConsumerId.get(subscription.dispatcher().consumerId());
                    return candidate == subscription.dispatcher()
                            ? subscription
                            : Subscription.open(topicQueue, candidate, checkpointDirectory);
                })
                .toList();

        // 3회차: 처음 보는 consumerId만 새로 연다.
        List<ConsumerId> knownIds = subscriptions.stream()
                .map(subscription -> subscription.dispatcher().consumerId())
                .toList();
        List<Subscription> added = matched.stream()
                .filter(dispatcher -> !knownIds.contains(dispatcher.consumerId()))
                .map(dispatcher -> Subscription.open(topicQueue, dispatcher, checkpointDirectory))
                .toList();

        subscriptions = Stream.concat(kept.stream(), added.stream()).toList();
    }

    boolean containsSubscriptionFor(ConsumerId consumerId) {
        return subscriptions.stream()
                .anyMatch(subscription -> subscription.dispatcher().consumerId().equals(consumerId));
    }

    void close() {
        subscriptions.forEach(Subscription::close);
        checkpointDirectory.close();
    }
}
