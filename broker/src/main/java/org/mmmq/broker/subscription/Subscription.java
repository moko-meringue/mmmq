package org.mmmq.broker.subscription;

import java.io.Closeable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.topicqueue.Offset;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.broker.topicqueue.storage.CheckpointFile;
import org.mmmq.broker.topicqueue.storage.CorruptionException;
import org.mmmq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ({@link TopicQueue} 하나, {@link Dispatcher} 하나) 짝의 구독 상태 전부 — 오프셋, 체크포인트 파일,
 * 그 짝을 처리하는 워커 스레드 하나까지 한 객체에 모은다.
 *
 * <p>{@link #trigger()}는 도착 통지를 워커에 큐잉만 하고 바로 반환한다. 실제 처리는
 * {@code peek → dispatcher.send → 체크포인트 갱신} 루프를 도는 {@link #drain()}이 워커 스레드에서 맡는다.
 * 그래서 느리거나 실패하는 소비자 하나가 다른 구독을 막지 못한다.
 *
 * <p>큐 하나에 워커 하나면 충분하므로 executor는 필드 하나다 — 이전에는 Dispatcher가 여러 큐를
 * {@code Map<TopicQueue, ExecutorService>}로 들고 있었지만, Subscription은 애초에 큐 하나만 알아서 맵이 필요 없다.
 *
 * <p>{@link #open}은 체크포인트가 없을 때만 {@link TopicQueue#tailOffset()}을 기록한다 — 새로 붙는 구독은
 * 로그 끝에서 시작해 기존 백로그를 재생하지 않기 위해서다. 이미 체크포인트가 있으면(재기동) 손대지 않는다.
 */
class Subscription implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Subscription.class);

    private final TopicQueue topicQueue;
    private final Dispatcher dispatcher;
    private final CheckpointFile checkpointFile;
    private final ExecutorService worker;
    private volatile Offset offset;

    private Subscription(TopicQueue topicQueue, Dispatcher dispatcher, CheckpointFile checkpointFile) {
        this.topicQueue = topicQueue;
        this.dispatcher = dispatcher;
        this.checkpointFile = checkpointFile;
        offset = new Offset(checkpointFile.read());
        worker = new ThreadPoolExecutor(
                0, 1, 60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new ThreadPoolExecutor.DiscardPolicy()
        );
    }

    static Subscription open(TopicQueue topicQueue, Dispatcher dispatcher, CheckpointDirectory checkpointDirectory) {
        CheckpointFile checkpointFile = checkpointDirectory.get(dispatcher.consumerId().value());
        if (checkpointFile == null) {
            checkpointFile = checkpointDirectory.register(dispatcher.consumerId().value());
            checkpointFile.write(topicQueue.tailOffset());
        }
        return new Subscription(topicQueue, dispatcher, checkpointFile);
    }

    Dispatcher dispatcher() {
        return dispatcher;
    }

    void trigger() {
        worker.submit(this::drain);
    }

    private void drain() {
        try {
            while (true) {
                try {
                    Message message = topicQueue.peek(offset);
                    if (message == null) {
                        return;
                    }
                    dispatcher.send(message);
                } catch (CorruptionException exception) {
                    log.error("Subscription {} skipped corrupted entry on topic {} at offset {}",
                            dispatcher.consumerId(),
                            topicQueue.getTopic(),
                            offset,
                            exception
                    );
                }
                offset = offset.next();
                checkpointFile.write(offset.value());
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.info("Subscription {} drain interrupted on topic {}", dispatcher.consumerId(), topicQueue.getTopic());
        } catch (Exception exception) {
            log.error("Subscription {} aborted drain on topic {}", dispatcher.consumerId(), topicQueue.getTopic(), exception);
        }
    }

    @Override
    public void close() {
        worker.shutdownNow();
    }
}
