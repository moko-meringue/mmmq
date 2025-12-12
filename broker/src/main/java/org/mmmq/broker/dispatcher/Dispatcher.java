package org.mmmq.broker.dispatcher;

import org.mmmq.broker.dispatcher.dlq.DeadLetter;
import org.mmmq.broker.dispatcher.dlq.DeadLetterQueue;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Dispatcher {

    static final int MAX_RETRY_COUNT = 3;
    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    final String name;
    final Host host;
    final Set<Topic> topics;
    final LinkedBlockingQueue<Message> messageQueue;
    final DeadLetterQueue deadLetterQueue;
    final Worker worker;
    final ThreadPoolExecutor threadPool;
    Sender sender;

    public Dispatcher(
            String name,
            Host host,
            Set<Topic> topics,
            ThreadPoolExecutor threadPool,
            DeadLetterQueue deadLetterQueue
    ) {
        this.name = name;
        this.host = host;
        this.topics = topics;
        this.messageQueue = new LinkedBlockingQueue<>();
        this.threadPool = threadPool;
        this.deadLetterQueue = deadLetterQueue;
        this.sender = Sender.from(host);
        this.worker = new Worker();
    }

    public void dispatch(Message message) {
        messageQueue.add(message);
    }

    public boolean isSubscribing(Topic topic) {
        return topics.contains(topic);
    }

    void start() {
        worker.start();
    }

    void stop() {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
        }
        worker.stop();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Dispatcher that)) {
            return false;
        }
        return Objects.equals(name, that.name) && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, host);
    }

    private class Worker {

        final Thread thread;

        Worker() {
            this.thread = new Thread(new Job(), "mmmq-dispatcher" + "-" + name + "-worker");
        }

        void start() {
            thread.start();
        }

        void stop() {
            thread.interrupt();
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private class Job implements Runnable {

            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted() && !threadPool.isShutdown()) {
                    try {
                        Message message = messageQueue.take();
                        threadPool.submit(() -> send(message));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.info("DispatchWorker interrupted");
                    } catch (Exception e) {
                        log.warn("Failed to dispatch message: {}", e.getMessage());
                    }
                }
            }

            void send(Message message) {
                try {
                    if (!sender.send(message, MAX_RETRY_COUNT)) {
                        handleFailure(DeadLetter.maxRetriesExceeded(message));
                    }
                } catch (Exception e) {
                    handleFailure(DeadLetter.processingFailed(message, e));
                }
            }

            void handleFailure(DeadLetter deadLetter) {
                log.warn("Failed to send message: {}", deadLetter);
                deadLetterQueue.add(deadLetter);
            }
        }
    }
}
