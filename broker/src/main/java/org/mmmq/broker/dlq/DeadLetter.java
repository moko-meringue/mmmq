package org.mmmq.broker.dlq;

import org.mmmq.core.message.Message;

public record DeadLetter(
        Message message,
        String reason,
        Exception exception
) {

    public static DeadLetter maxRetriesExceeded(Message message) {
        return new DeadLetter(
                message,
                "Max retries exceeded",
                null
        );
    }

    public static DeadLetter processingFailed(Message message, Exception exception) {
        return new DeadLetter(
                message,
                "Processing failed",
                exception
        );
    }
}
