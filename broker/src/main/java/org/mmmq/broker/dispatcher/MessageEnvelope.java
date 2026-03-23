package org.mmmq.broker.dispatcher;

import org.mmmq.core.message.Message;

public record MessageEnvelope(
        Message message,
        Runnable onFailure
) {
    public void handleFailure() {
        onFailure.run();
    }
}
