package org.mmmq.broker.dispatcher;

import jakarta.annotation.Nullable;
import org.mmmq.broker.dispatcher.dlq.DeadLetter;
import org.mmmq.broker.dispatcher.dlq.DeadLetterQueue;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    @Nullable
    final DeadLetterQueue deadLetterQueue;
    final ThreadPoolExecutor threadPool;
    final Worker worker;
    Sender sender;

    Dispatcher(
            String name,
            Host host,
            Set<Topic> topics,
            LinkedBlockingQueue<Message> messageQueue,
            @Nullable DeadLetterQueue deadLetterQueue,
            ThreadPoolExecutor threadPool,
            Sender sender
    ) {
        this.name = name;
        this.host = host;
        this.topics = topics;
        this.messageQueue = messageQueue;
        this.deadLetterQueue = deadLetterQueue;
        this.worker = new Worker();
        this.threadPool = threadPool;
        this.sender = sender;
    }

    public Dispatcher(
            String name,
            Host host,
            Set<Topic> topics,
            @Nullable DeadLetterQueue deadLetterQueue,
            ThreadPoolExecutor threadPool
    ) {
        this(name, host, topics, new LinkedBlockingQueue<>(), deadLetterQueue, threadPool, Sender.from(host));
    }

    public Dispatcher(String name, Host host, Set<Topic> topics, @Nullable DeadLetterQueue deadLetterQueue) {
        this(
                name,
                host,
                topics,
                new LinkedBlockingQueue<>(),
                deadLetterQueue,
                new ThreadPoolExecutor(
                        2,
                        5,
                        40L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>()
                ),
                Sender.from(host)
        );
    }

    public void dispatch(Message message) {
        messageQueue.add(message);
    }

    public boolean isSubscribing(Topic topic) {
        return topics.contains(topic);
    }

    void start() {
        worker.start();
        if (deadLetterQueue != null) {
            deadLetterQueue.start();
        }
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
        if (deadLetterQueue != null) {
            deadLetterQueue.stop();
        }
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
                if (deadLetterQueue != null) {
                    deadLetterQueue.add(deadLetter);
                }
            }
        }
    }
}
