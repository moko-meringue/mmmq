package org.mmmq.broker.dlq;

import org.mmmq.core.message.Message;

public record DeadLetter(
        Message message,
        Throwable cause
) {
}
