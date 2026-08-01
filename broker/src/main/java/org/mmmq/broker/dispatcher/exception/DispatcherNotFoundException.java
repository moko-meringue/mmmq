package org.mmmq.broker.dispatcher.exception;

import org.mmmq.core.identifier.ConsumerId;

/**
 * 등록되지 않은 {@code consumerId}를 수정하거나 삭제하려 할 때 던진다.
 *
 * <p>{@code DispatcherController}가 404로 옮긴다.
 */
public class DispatcherNotFoundException extends RuntimeException {

    public DispatcherNotFoundException(ConsumerId consumerId) {
        super("No dispatcher for consumerId '" + consumerId + "'");
    }
}
