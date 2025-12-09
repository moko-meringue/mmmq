package org.mmmq.consumer.handler.execution;

import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

public abstract class HandlerExecution {

    protected final String name;
    protected final Topic topic;

    protected HandlerExecution(String name, Topic topic) {
        this.name = name;
        this.topic = topic;
    }

    public abstract void execute(Message message);

    public String getName() {
        return name;
    }

    public Topic getTopic() {
        return topic;
    }
}
