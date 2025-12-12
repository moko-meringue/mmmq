package org.mmmq.broker.dispatcher;

import org.mmmq.core.message.Message;
import org.springframework.stereotype.Component;

@Component
public class FrontDispatcher {

    final DispatcherContainer dispatcherContainer;

    public FrontDispatcher(DispatcherContainer dispatcherContainer) {
        this.dispatcherContainer = dispatcherContainer;
    }

    public void push(Message message) {
        dispatcherContainer.getDispatchers(message.topic())
                .forEach(messageDispatcher -> messageDispatcher.dispatch(message));
    }
}
