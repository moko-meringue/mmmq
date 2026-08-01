package org.mmmq.broker.dispatcher;

import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.core.Host;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.core.message.TopicPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 하나의 {@code consumerId} 앞으로 메시지를 보내는 송신 단위. 1 id = 1 Dispatcher다.
 *
 * <p>{@code (consumerId, host, pattern)} 값만 들고, 그 대상으로 메시지 하나를 보내는 법(재시도·백오프)만 안다.
 * 어떤 {@link org.mmmq.broker.topicqueue.TopicQueue}를 구독하는지, 어디까지 읽었는지는 전혀 모른다 —
 * 그 상태는 {@code subscription.Subscription}이 갖는다. Dispatcher가 여러 큐에 걸쳐 재사용될 수 있는
 * 이유이기도 하다 — 워커나 오프셋을 스스로 들고 있지 않으니 공유해도 안전하다.
 *
 * <p>설정 파일이나 HTTP 본문의 형태를 모른다 — 변환은 전부 {@link DispatcherContainer}가 하고,
 * 여기로는 이미 해석된 값 타입만 들어온다.
 */
public class Dispatcher {

    private static final int MAX_NACK_RETRY_COUNT = 3;
    private static final long INITIAL_BACKOFF_DELAY_MS = 1000;
    private static final long MAX_BACKOFF_DELAY_MS = 60000;
    private static final int BACKOFF_MULTIPLIER = 2;

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);
    private final Host host;
    private final ConsumerId consumerId;
    private final TopicPattern pattern;
    private final Sender sender;

    public Dispatcher(Host host, ConsumerId consumerId, TopicPattern pattern) {
        this(host, consumerId, pattern, Sender.from(host));
    }

    public Dispatcher(Host host, ConsumerId consumerId, TopicPattern pattern, Sender sender) {
        this.host = host;
        this.consumerId = consumerId;
        this.pattern = pattern;
        this.sender = sender;
    }

    public ConsumerId consumerId() {
        return consumerId;
    }

    Host host() {
        return host;
    }

    TopicPattern pattern() {
        return pattern;
    }

    public boolean canDispatch(Topic topic) {
        return pattern.matches(topic);
    }

    /**
     * ACK을 받거나 재시도를 소진할 때까지 전송을 시도한다. NACK이 소진되면 메시지를 버리고 조용히 반환하지만,
     * 통신 실패(RuntimeException)에는 지수 백오프로 무한히 재시도한다 — 소비자가 잠깐 내려간 것과 소비자가
     * 메시지를 거부한 것은 서로 다른 문제라 복구 전략이 다르다.
     *
     * <p>호출자(구독의 워커 스레드)가 인터럽트로 종료를 요청하면 {@link InterruptedException}을 던져 즉시 멈춘다.
     */
    public void send(Message message) throws InterruptedException {
        long currentBackoffDelay = INITIAL_BACKOFF_DELAY_MS;
        while (true) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            try {
                if (!sender.send(message, consumerId, MAX_NACK_RETRY_COUNT)) {
                    log.warn("NACK exhausted. Dropping message: {}", message);
                }
                return;
            } catch (RuntimeException exception) {
                log.warn(
                        "Communication failure. Backing off {}ms. Error: {}",
                        currentBackoffDelay,
                        exception.getMessage()
                );
                Thread.sleep(currentBackoffDelay);
                currentBackoffDelay = Math.min(currentBackoffDelay * BACKOFF_MULTIPLIER, MAX_BACKOFF_DELAY_MS);
            }
        }
    }
}
