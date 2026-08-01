package org.mmmq.broker.dispatcher;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import org.mmmq.broker.dispatcher.exception.DispatcherNotFoundException;
import org.mmmq.broker.dispatcher.exception.DuplicateConsumerIdException;
import org.mmmq.broker.dispatcher.storage.DispatcherEntry;
import org.mmmq.broker.dispatcher.storage.DispatchersFile;
import org.mmmq.core.Host;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;
import org.springframework.stereotype.Component;

/**
 * 모든 {@link Dispatcher}의 소유자. Dispatcher가 태어나고 사라지는 유일한 길이다.
 *
 * <p>부팅 시 {@link DispatchersFile}을 읽어 Dispatcher를 만들고, 관리 API의 추가·수정·삭제를 받아
 * 파일과 메모리를 함께 갱신한다. 구독 상태는 스스로 들고 있지 않는다 — 대신 목록이 바뀔 때마다
 * (부팅 직후 포함) 현재 Dispatcher 전체를 {@link DispatcherRematcher}에게 넘겨 다시 매칭하게 한다.
 * 그 구현체인 {@code subscription.SubscriptionContainer}를 이 클래스가 되받아 참조하면 둘이 생성자
 * 주입으로 서로를 필요로 하는 순환이 되어 컨텍스트가 뜨지 않기 때문이다.
 *
 * <p>계층 간 변환이 여기로 모여 있다 — 저장·API 타입을 {@link Dispatcher}가 알지 못하게 하려면
 * 양쪽을 다 아는 자리가 하나 필요하고, 이 클래스가 {@link Dispatcher}와 같은 패키지라 그 일을 할 수 있다.
 *
 * <p>뮤테이션은 락 하나로 직렬화한다. 쓰기는 언제나 검증 → 파일 → 메모리 → 재매칭 순서라,
 * 파일이 실패하면 메모리는 손대지 않은 상태로 남는다.
 */
@Component
public class DispatcherContainer {

    private final DispatchersFile dispatchersFile;
    private final DispatcherRematcher rematcher;
    private final Map<ConsumerId, Dispatcher> dispatchers = new LinkedHashMap<>();
    private final ReentrantLock mutationLock = new ReentrantLock();

    public DispatcherContainer(DispatchersFile dispatchersFile, DispatcherRematcher rematcher) {
        this.dispatchersFile = dispatchersFile;
        this.rematcher = rematcher;
        dispatchersFile.read().forEach(record -> {
            Dispatcher dispatcher = toDispatcher(record);
            if (dispatchers.putIfAbsent(dispatcher.consumerId(), dispatcher) != null) {
                throw new DuplicateConsumerIdException(dispatcher.consumerId());
            }
        });
        rematcher.rematchAll(List.copyOf(dispatchers.values()));
    }

    public List<DispatcherSnapshot> snapshots() {
        mutationLock.lock();
        try {
            return dispatchers.values().stream()
                    .map(this::toSnapshot)
                    .toList();
        } finally {
            mutationLock.unlock();
        }
    }

    public DispatcherSnapshot add(ConsumerId consumerId, Host host, TopicPattern pattern) {
        mutationLock.lock();
        try {
            if (dispatchers.containsKey(consumerId)) {
                throw new DuplicateConsumerIdException(consumerId);
            }
            Dispatcher dispatcher = new Dispatcher(host, consumerId, pattern);
            dispatchersFile.write(Stream.concat(entries().stream(), Stream.of(toEntry(dispatcher))).toList());
            dispatchers.put(consumerId, dispatcher);
            rematcher.rematchAll(List.copyOf(dispatchers.values()));
            return toSnapshot(dispatcher);
        } finally {
            mutationLock.unlock();
        }
    }

    public DispatcherSnapshot modify(ConsumerId consumerId, Host host, TopicPattern pattern) {
        mutationLock.lock();
        try {
            Dispatcher previous = dispatchers.get(consumerId);
            if (previous == null) {
                throw new DispatcherNotFoundException(consumerId);
            }
            Dispatcher next = new Dispatcher(host, consumerId, pattern);
            dispatchersFile.write(entries().stream()
                    .map(existing -> existing.consumerId().equals(consumerId.value()) ? toEntry(next) : existing)
                    .toList());
            dispatchers.put(consumerId, next);
            rematcher.rematchAll(List.copyOf(dispatchers.values()));
            return toSnapshot(next);
        } finally {
            mutationLock.unlock();
        }
    }

    public void remove(ConsumerId consumerId) {
        mutationLock.lock();
        try {
            Dispatcher dispatcher = dispatchers.get(consumerId);
            if (dispatcher == null) {
                throw new DispatcherNotFoundException(consumerId);
            }
            dispatchersFile.write(entries().stream()
                    .filter(existing -> !existing.consumerId().equals(consumerId.value()))
                    .toList());
            dispatchers.remove(consumerId);
            rematcher.rematchAll(List.copyOf(dispatchers.values()));
        } finally {
            mutationLock.unlock();
        }
    }

    private List<DispatcherEntry> entries() {
        return dispatchers.values().stream()
                .map(this::toEntry)
                .toList();
    }

    private DispatcherSnapshot toSnapshot(Dispatcher dispatcher) {
        return new DispatcherSnapshot(
                dispatcher.consumerId(),
                dispatcher.host(),
                dispatcher.pattern()
        );
    }

    private DispatcherEntry toEntry(Dispatcher dispatcher) {
        return new DispatcherEntry(
                dispatcher.consumerId().value(),
                dispatcher.host().toUri(),
                dispatcher.pattern().value()
        );
    }

    private Dispatcher toDispatcher(DispatcherEntry record) {
        return new Dispatcher(
                Host.from(record.host()),
                new ConsumerId(record.consumerId()),
                new TopicPattern(record.pattern())
        );
    }
}
