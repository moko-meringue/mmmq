package org.mmmq.subscriber;

import org.mmmq.core.message.Message;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
class MessageReceivedEventHandler implements ApplicationListener<MessageReceivedEvent> {

    final MessageHandlerContainer messageHandlerContainer = new MessageHandlerContainer();
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(
            2,
            5,
            40L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>()
    );

    void addMessageHandler(MessageHandler handler) {
        messageHandlerContainer.add(handler);
    }

    @Override
    public void onApplicationEvent(MessageReceivedEvent event) {
        Message message = event.getMessage();
        messageHandlerContainer.getHandlers(message.topic())
                .forEach(handler -> executor.execute(() -> handler.handle(message)));
    }
}
