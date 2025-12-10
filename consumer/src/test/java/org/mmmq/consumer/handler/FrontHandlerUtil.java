package org.mmmq.consumer.handler;

import org.mmmq.consumer.handler.execution.HandlerExecutions;

public class FrontHandlerUtil {

    public static HandlerExecutions getHandlerExecutions(FrontHandler frontHandler) {
        return frontHandler.handlerExecutions;
    }
}
