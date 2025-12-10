package org.mmmq.consumer.handler;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.consumer.handler.execution.HandlerExecutions;
import org.mmmq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Component
public class FrontHandler {

    final Thread worker;
    final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
        2,
        5,
        40L,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>()
    );
    final Logger log = LoggerFactory.getLogger(FrontHandler.class);
    final HandlerExecutions handlerExecutions = new HandlerExecutions();
    final LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<>();

    public FrontHandler() {
        worker = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && !threadPool.isShutdown()) {
                try {
                    Message message = queue.take();
                    execute(message);
                } catch (Exception e) {
                    log.warn("Failed to handle message: {}", e.getMessage());
                }
            }
        });
    }

    @PostConstruct
    void startWorker() {
        worker.start();
    }

    public void addHandlerExecutions(HandlerExecution handlerExecution) {
        handlerExecutions.add(handlerExecution);
    }

    public void handle(Message message) {
        queue.add(message);
    }

    void execute(Message message) {
        handlerExecutions.getExecutions(message.topic())
            .forEach(handlerExecution -> threadPool.execute(() -> {
                try {
                    handlerExecution.execute(message);
                } catch (Exception e) {
                    log.warn("Failed to handle message: {}", message, e);
                }
            }));
    }

    @PreDestroy
    void destruct() {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
        }
    }
}
