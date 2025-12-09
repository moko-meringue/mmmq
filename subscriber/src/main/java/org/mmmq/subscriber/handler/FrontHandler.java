package org.mmmq.subscriber.handler;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.mmmq.core.message.Message;
import org.mmmq.subscriber.handler.execution.HandlerExecution;
import org.mmmq.subscriber.handler.execution.HandlerExecutions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

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
        this.worker = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && !threadPool.isShutdown()) {
                try {
                    Message message = queue.take();
                    handle(message);
                } catch (Exception e) {
                    log.warn("Failed to send message: {}", e.getMessage());
                }
            }
        });
    }

    public void addHandlerExecutions(HandlerExecution handlerExecution) {
        handlerExecutions.add(handlerExecution);
    }

    public void handleMessage(Message message) {
        queue.add(message);
    }

    private void handle(Message message) {
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
    public void destruct() {
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
