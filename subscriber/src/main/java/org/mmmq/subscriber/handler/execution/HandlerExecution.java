package org.mmmq.subscriber.handler.execution;

import org.mmmq.core.message.Message;

public abstract class HandlerExecution {

    protected final String name;
    protected final String topic;

    protected HandlerExecution(String name, String topic) {
        this.name = name;
        this.topic = topic;
    }

    public abstract void execute(Message message);

    public String getName() {
        return name;
    }

    public String getTopic() {
        return topic;
    }
}
