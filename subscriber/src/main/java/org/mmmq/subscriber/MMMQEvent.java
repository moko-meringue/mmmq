package org.mmmq.subscriber;

import org.mmmq.core.message.Message;
import org.springframework.context.ApplicationEvent;

public class MMMQEvent extends ApplicationEvent {

    public Message message;

    public MMMQEvent(Object source, Message message) {
        super(source);
        this.message = message;
    }
}
