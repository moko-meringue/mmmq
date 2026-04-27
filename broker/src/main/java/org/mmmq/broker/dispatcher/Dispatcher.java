package org.mmmq.broker.dispatcher;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.dlq.DeadLetter;
import org.mmmq.broker.dlq.DeadLetterQueue;
import org.mmmq.broker.topicqueue.Offset;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;

public class Dispatcher { // 하나의 Consumer를 향해 패턴 매칭되는 토픽의 메시지를 전달하는 객체

    static final int MAX_NACK_RETRY_COUNT = 3; // Consumer가 NACK를 연속으로 이 횟수만큼 반환하면 DLQ로 이동
    static final long INITIAL_BACKOFF_DELAY_MS = 1000; // 통신 오류 시 첫 재시도 대기 시간: 1초
    static final long MAX_BACKOFF_DELAY_MS = 60000; // 지수 백오프의 상한선: 60초
    static final int BACKOFF_MULTIPLIER = 2; // 매 실패마다 대기 시간을 2배로 증가

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);
    private static final Pattern NAME_PATTERN = Pattern.compile(
            "[A-Za-z0-9._-]+"); // Dispatcher 이름은 offset 파일명으로 사용되므로 파일 시스템에 안전한 문자만 허용

    final String name; // Dispatcher 식별자. offset 파일명({name}.offset)으로도 사용됨
    final Host host; // 메시지를 전달할 Consumer의 주소
    final List<org.mmmq.core.message.Pattern> patterns; // 이 Dispatcher가 처리할 토픽 패턴 목록 (Ant-style 와일드카드)
    final List<DeadLetterQueue> deadLetterQueues; // NACK 소진 시 실패 메시지를 보낼 DLQ 목록
    final ConcurrentHashMap<TopicQueue, Subscription> subscriptions = new ConcurrentHashMap<>(); // 토픽큐 → Subscription. 각 토픽별로 독립적인 worker thread와 offset을 보유
    Sender sender; // Consumer에게 HTTP로 메시지를 전달하는 객체

    public Dispatcher(String name, Host host, List<org.mmmq.core.message.Pattern> patterns) { // DLQ 없는 단순 생성자
        this(name, host, patterns, List.of());
    }

    public Dispatcher(
            String name,
            Host host,
            List<org.mmmq.core.message.Pattern> patterns,
            List<DeadLetterQueue> deadLetterQueues
    ) {
        if (!NAME_PATTERN.matcher(name).matches()) { // 파일 시스템 안전 문자 검증: offset 파일명이 될 이름에 특수문자 방지
            throw new IllegalArgumentException("Dispatcher name must match [A-Za-z0-9._-]+, but was: " + name);
        }
        this.name = name;
        this.host = host;
        this.patterns = patterns;
        this.deadLetterQueues = deadLetterQueues;
        this.sender = Sender.from(host); // Consumer 호스트 정보로 HTTP 클라이언트 생성
    }

    public void subscribe(TopicQueue topicQueue) { // 토픽 큐가 이 Dispatcher의 패턴과 매칭되면 Subscription을 생성해 등록
        if (!matches(topicQueue.getTopic())) { // 패턴 불일치 토픽은 무시
            return;
        }
        subscriptions.computeIfAbsent(topicQueue,
                queue -> new Subscription(name, queue)); // 중복 구독 방지: 이미 Subscription이 있으면 새로 만들지 않음
    }

    boolean matches(Topic topic) { // 등록된 패턴 중 하나라도 topic과 매칭되면 true
        return patterns.stream()
                .anyMatch(pattern -> pattern.matches(topic));
    }

    void stop() { // 애플리케이션 종료 시 모든 Subscription worker thread를 즉시 중단
        subscriptions.values()
                .forEach(Subscription::shutdownNow);
    }

    @EventListener
    void onMessageArrived(
            MessageArrivedEvent event) { // MessageArrivedEvent 수신 시 해당 토픽을 구독 중인 Subscription의 worker에 drain 작업을 제출
        final TopicQueue topicQueue = event.topicQueue();
        subscriptions.computeIfPresent(topicQueue, (topic, subscription) -> { // 이 토픽을 구독하는 Subscription이 있는 경우에만 처리
            subscription.submit(() -> drain(topicQueue, subscription)); // worker thread에 비동기 실행 위임

            return subscription; // computeIfPresent는 null 반환 시 엔트리를 제거하므로 반드시 subscription 반환
        });
    }

    void drain(TopicQueue topicQueue, Subscription subscription) { // 큐에 남은 메시지를 전부 처리할 때까지 반복. peek → 전달 → commit 순서
        Message message;
        while ((message = topicQueue.peek(subscription.offset()))
                != null) { // peek은 offset을 바꾸지 않음. null이면 현재 시점에 읽을 메시지가 없음
            deliverOrDeadLetter(message); // 전달 완료(ACK 또는 DLQ 적재)가 보장된 후에만 return
            topicQueue.commit(subscription.dispatcherName(),
                    subscription.offset()); // offset++ + fsync. deliverOrDeadLetter return 후에만 실행 → at-least-once 보장
        }
    }

    private void deliverOrDeadLetter(Message message) { // Consumer에게 메시지를 전달. 통신 오류 시 지수 백오프로 무한 재시도. NACK 소진 시 DLQ 적재
        long currentBackoffDelay = INITIAL_BACKOFF_DELAY_MS;
        while (!Thread.currentThread().isInterrupted()) { // 인터럽트(shutdown) 신호가 오면 루프 탈출
            try {
                if (!sender.send(message, MAX_NACK_RETRY_COUNT)) { // NACK을 MAX_NACK_RETRY_COUNT번 연속 받으면 false 반환
                    log.warn("NACK exhausted. Sending to DLQ: {}", message);
                    deadLetterQueues.forEach(dlq -> dlq.add(new DeadLetter(message))); // 모든 DLQ에 실패 메시지 전달
                }

                return; // send 성공 또는 DLQ 적재 완료: "이 메시지의 운명 결정됨" → 호출부에서 commit 가능
            } catch (Exception exception) { // IOException 등 통신 오류: 일시적 장애로 간주하고 백오프 후 재시도
                log.warn(
                        "Communication failure. Backing off {}ms. Error: {}",
                        currentBackoffDelay,
                        exception.getMessage()
                );
                try {
                    Thread.sleep(currentBackoffDelay); // 지수 백오프 대기
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt(); // 인터럽트 상태 복원 후 루프 탈출

                    return;
                }
                currentBackoffDelay = Math.min(currentBackoffDelay * BACKOFF_MULTIPLIER,
                        MAX_BACKOFF_DELAY_MS); // 다음 대기 시간을 2배로. 최대 60초 초과하지 않도록 상한 적용
            }
        }
    }

    record Subscription( // 한 (Dispatcher, TopicQueue) 쌍의 구독 상태. 독립적인 offset과 단일 worker thread를 보유
                         String dispatcherName, // commit() 호출 시 OffsetStore를 찾기 위한 키
                         Offset offset,         // 이 구독의 현재 읽기 위치. peek 전용이며 commit 시에만 증가
                         ExecutorService worker  // drain 작업을 처리하는 단일 스레드 executor
    ) {

        Subscription(String dispatcherName, TopicQueue topicQueue) { // 신규 구독: OffsetStore에서 마지막 커밋 위치를 읽어 Offset을 초기화
            this(
                    dispatcherName,
                    topicQueue.subscribe(dispatcherName), // OffsetStore.read()로 재시작 위치 복원
                    new ThreadPoolExecutor(
                            0, 1, 60L, TimeUnit.SECONDS, // 최대 1개 스레드: 같은 토픽 메시지의 순서를 보장
                            new ArrayBlockingQueue<>(1), // 큐 크기 1: 이미 drain 중이면 추가 제출은 무시
                            new ThreadPoolExecutor.DiscardPolicy() // 큐가 가득 차면 새 task를 버림: MessageArrivedEvent는 낙관적이므로 유실 허용
                    )
            );
        }

        void submit(Runnable task) { // worker thread에 drain 작업 제출. 이미 실행 중이면 DiscardPolicy에 의해 무시됨
            worker.submit(task);
        }

        void shutdownNow() { // 진행 중인 작업을 인터럽트하고 worker thread를 즉시 종료
            worker.shutdownNow();
        }
    }
}
