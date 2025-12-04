package org.mmmq.subscriber;

import jakarta.annotation.PreDestroy;
import org.mmmq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
class MessageReceivedEventHandler {

    final ThreadPoolExecutor executor = new ThreadPoolExecutor(
            2,
            5,
            40L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>()
    );
    final Logger log = LoggerFactory.getLogger(MessageReceivedEventHandler.class);
    final MessageHandlerContainer messageHandlerContainer = new MessageHandlerContainer();

    void addMessageHandler(MessageHandler handler) {
        messageHandlerContainer.add(handler);
    }

    @EventListener
    public void handle(MessageReceivedEvent event) {
        Message message = event.getMessage();
        messageHandlerContainer.getHandlers(message.topic())
                .forEach(handler -> executor.execute(() -> {
                    try {
                        handler.handle(message);
                    } catch (Exception e) {
                        log.warn("Failed to handle message: {}", message, e);
                    }
                }));
    }

    @PreDestroy
    public void destruct() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
}
