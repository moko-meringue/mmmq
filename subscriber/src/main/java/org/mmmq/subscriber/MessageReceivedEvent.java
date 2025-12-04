package org.mmmq.subscriber;

import org.mmmq.core.message.Message;

public class MessageReceivedEvent {

    public Message message;

    public MessageReceivedEvent(Message message) {
        this.message = message;
    }

    public Message getMessage() {
        return message;
    }
}
