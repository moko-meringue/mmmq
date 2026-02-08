package org.mmmq.broker.dispatcher;

import org.mmmq.core.message.Message;

import java.util.function.Consumer;

public class MessageEnvelope {

    private final Message message;
    private final Consumer<Throwable> onFailure;

    public MessageEnvelope(Message message, Consumer<Throwable> onFailure) {
        this.message = message;
        this.onFailure = onFailure;
    }

    public void handleFailure(Throwable cause) {
        onFailure.accept(cause);
    }

    public Message getMessage() {
        return message;
    }
}
