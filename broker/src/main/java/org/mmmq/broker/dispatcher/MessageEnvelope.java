package org.mmmq.broker.dispatcher;

import java.util.function.Consumer;
import org.mmmq.core.message.Message;

public record MessageEnvelope(
        Message message,
        Consumer<Throwable> onFailure
) {
    public void handleFailure(Throwable cause) {
        onFailure.accept(cause);
    }
}
