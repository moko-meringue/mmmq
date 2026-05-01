package org.mmmq.consumer.handler;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.consumer.handler.execution.HandlerExecutions;
import org.mmmq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
public class FrontHandler {

    static final Logger log = LoggerFactory.getLogger(FrontHandler.class);

    final Worker worker;
    final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
            2,
            5,
            40L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(100)
    );
    final HandlerExecutions handlerExecutions = new HandlerExecutions();
    final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(1000);

    public FrontHandler() {
        this.worker = new Worker();
    }

    public void addHandlerExecution(HandlerExecution handlerExecution) {
        handlerExecutions.add(handlerExecution);
    }

    public void handle(Message message) {
        queue.add(message);
    }

    @PostConstruct
    void start() {
        worker.start();
    }

    @PreDestroy
    void destroy() {
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

    private class Worker {

        final Thread thread;

        Worker() {
            this.thread = new Thread(new Job(), "mmmq-frontHandler-worker");
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
                        handle(queue.take());
                    } catch (Exception e) {
                        log.warn("Failed to handle message: {}", e.getMessage());
                    }
                }
            }

            void handle(Message message) {
                handlerExecutions.getExecutions(message)
                        .forEach(handlerExecution -> threadPool.execute(() -> {
                            try {
                                handlerExecution.execute(message);
                            } catch (Exception e) {
                                log.warn("Failed to handle message: {}", message, e);
                            }
                        }));
            }
        }
    }
}
