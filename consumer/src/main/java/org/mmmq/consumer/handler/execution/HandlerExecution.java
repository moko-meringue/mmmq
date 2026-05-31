package org.mmmq.consumer.handler.execution;

import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.Message;

public interface HandlerExecution {

    ConsumerId id();

    void execute(Message message);
}
