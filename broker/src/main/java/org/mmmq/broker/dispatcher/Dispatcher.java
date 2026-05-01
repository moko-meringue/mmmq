package org.mmmq.broker.dispatcher;

import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
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
import org.mmmq.broker.topicqueue.TopicQueueInitializedEvent;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.core.message.TopicPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

public class Dispatcher { // 하나의 Consumer를 향해 패턴 매칭되는 토픽의 메시지를 전달하는 객체

    static final int MAX_NACK_RETRY_COUNT = 3; // Consumer가 NACK를 연속으로 이 횟수만큼 반환하면 DLQ로 이동
    static final long INITIAL_BACKOFF_DELAY_MS = 1000; // 통신 오류 시 첫 재시도 대기 시간: 1초
    static final long MAX_BACKOFF_DELAY_MS = 60000; // 지수 백오프의 상한선: 60초
    static final int BACKOFF_MULTIPLIER = 2; // 매 실패마다 대기 시간을 2배로 증가

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);
    private static final Pattern NAME_PATTERN = Pattern.compile(
            "[A-Za-z0-9._-]+"); // Dispatcher 이름은 checkpoint 파일명으로 사용되므로 파일 시스템에 안전한 문자만 허용

    final String name; // Dispatcher 식별자. checkpoint 파일명({name}.checkpoint)으로도 사용됨
    final Host host; // 메시지를 전달할 Consumer의 주소
    final List<TopicPattern> patterns; // 이 Dispatcher가 처리할 토픽 패턴 목록 (Ant-style 와일드카드)
    final List<DeadLetterQueue> deadLetterQueues; // NACK 소진 시 실패 메시지를 보낼 DLQ 목록
    final ConcurrentHashMap<TopicQueue, Offset> subscriptions = new ConcurrentHashMap<>(); // 토픽큐 → 현재 offset. 키 존재 = 구독 중, 값 = 마지막 커밋 위치
    final WorkerPool workerPool = new WorkerPool(); // 토픽별 단일 스레드 워커 풀. drain 직렬화 책임
    Sender sender; // Consumer에게 HTTP로 메시지를 전달하는 객체

    public Dispatcher(String name, Host host, List<TopicPattern> patterns) { // DLQ 없는 단순 생성자
        this(name, host, patterns, List.of());
    }

    public Dispatcher(String name, Host host, List<TopicPattern> patterns, List<DeadLetterQueue> deadLetterQueues) {
        if (!NAME_PATTERN.matcher(name).matches()) { // 파일 시스템 안전 문자 검증: offset 파일명이 될 이름에 특수문자 방지
            throw new IllegalArgumentException("Dispatcher name must match [A-Za-z0-9._-]+, but was: " + name);
        }
        this.name = name;
        this.host = host;
        this.patterns = patterns;
        this.deadLetterQueues = deadLetterQueues;
        this.sender = Sender.from(host); // Consumer 호스트 정보로 HTTP 클라이언트 생성
    }

    @EventListener
    void onTopicQueueInitialized(
            TopicQueueInitializedEvent event) { // lazy 생성 / 부팅 복원 모두 처리. 패턴 매칭이면 Checkpoint에서 초기 offset 읽어 등록. catch-up drain은 onApplicationReady로 미룸
        TopicQueue topicQueue = event.topicQueue();
        if (!matches(topicQueue.getTopic())) { // 패턴 불일치 토픽은 무시
            return;
        }
        subscriptions.computeIfAbsent(topicQueue,
                queue -> queue.subscribe(name)); // Checkpoint.read()로 재시작 위치 복원
    }

    @EventListener(ApplicationReadyEvent.class)
    void onApplicationReady() {
        subscriptions.keySet().forEach(this::triggerDrain); // 빈 큐엔 no-op이라 항상 안전
    }

    boolean matches(Topic topic) { // 등록된 패턴 중 하나라도 topic과 매칭되면 true
        return patterns.stream()
                .anyMatch(pattern -> pattern.matches(topic));
    }

    @EventListener
    void onMessageArrived(MessageArrivedEvent event) {
        triggerDrain(event.topicQueue());
    }

    private void triggerDrain(TopicQueue topicQueue) {
        if (!subscriptions.containsKey(topicQueue)) {
            return;
        }
        workerPool.submit(topicQueue, () -> drain(topicQueue));
    }

    void drain(TopicQueue topicQueue) { // 큐에 남은 메시지를 전부 처리할 때까지 반복. peek → 전달 → commit 순서. 토픽별 단일 워커 스레드에서만 호출됨
        Offset offset = subscriptions.get(topicQueue);
        Message message;
        while ((message = topicQueue.peek(offset)) != null) { // peek은 offset을 바꾸지 않음. null이면 현재 시점에 읽을 메시지가 없음
            deliverOrDeadLetter(message); // 전달 완료(ACK 또는 DLQ 적재)가 보장된 후에만 return
            offset = topicQueue.commit(name, offset); // fsync 후 진전된 새 Offset 반환
            subscriptions.put(topicQueue, offset); // 토픽별 단일 스레드라 race-free
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

    @PreDestroy
    public void destroy() { // Spring context 종료 시 호출. 모든 워커 즉시 중단
        workerPool.shutdownAll();
    }

    static final class WorkerPool { // 토픽별 단일 스레드 워커 풀. drain 직렬화 보장이 책임. 워커 정책(스레드 수, queue 크기, discard 정책)이 이 클래스에 응집

        private final Map<TopicQueue, ExecutorService> pool = new ConcurrentHashMap<>(); // 토픽 → 전용 워커. 토픽마다 메시지 순서 보장 위해 단일 스레드 격리

        void submit(TopicQueue topicQueue, Runnable task) { // 해당 토픽의 워커에 task 제출. 워커가 없으면 첫 제출 시점에 생성
            pool.computeIfAbsent(topicQueue, queue -> createWorker()).submit(task);
        }

        private ExecutorService createWorker() {
            return new ThreadPoolExecutor(
                    0, 1, 60L, TimeUnit.SECONDS, // 최대 1개 스레드: 같은 토픽 메시지의 순서를 보장
                    new ArrayBlockingQueue<>(1), // 큐 크기 1: 이미 drain 중이면 추가 제출은 무시
                    new ThreadPoolExecutor.DiscardPolicy() // 큐가 가득 차면 새 task를 버림: MessageArrivedEvent는 낙관적이므로 유실 허용
            );
        }

        void shutdownAll() { // 모든 워커를 즉시 인터럽트하여 종료. 진행 중인 drain 중단
            pool.values()
                    .forEach(ExecutorService::shutdownNow);
        }
    }
}
