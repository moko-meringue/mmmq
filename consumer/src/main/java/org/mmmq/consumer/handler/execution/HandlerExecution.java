package org.mmmq.consumer.handler.execution;

import org.mmmq.core.message.Message;

public interface HandlerExecution {

    String id();

    void execute(Message message);
}
