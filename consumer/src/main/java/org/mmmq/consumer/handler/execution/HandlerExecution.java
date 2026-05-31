package org.mmmq.consumer.handler.execution;

import org.mmmq.core.message.Message;

public abstract class HandlerExecution {

    protected final String id;

    protected HandlerExecution(String id) {
        this.id = id;
    }

    public abstract void execute(Message message);

    public String id() {
        return id;
    }
}
