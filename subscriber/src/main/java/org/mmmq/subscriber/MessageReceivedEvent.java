package org.mmmq.subscriber;

import org.mmmq.core.message.Message;
import org.springframework.context.ApplicationEvent;

public class MessageReceivedEvent extends ApplicationEvent {

    public Message message;

    public MessageReceivedEvent(Object source, Message message) {
        super(source);
        this.message = message;
    }

    public Message getMessage() {
        return message;
    }
}
