package org.mmmq.broker.dispatcher.exception;

import org.mmmq.core.identifier.ConsumerId;

/**
 * 이미 등록된 {@code consumerId}로 Dispatcher를 추가하려 할 때 던진다.
 *
 * <p>부팅 시 파일에 중복이 있으면 컨텍스트 기동이 실패하고, 런타임 추가에서는
 * {@code DispatcherController}가 409로 옮긴다. 1 id = 1 Dispatcher라는 규칙을 지키는 자리다.
 */
public class DuplicateConsumerIdException extends RuntimeException {

    public DuplicateConsumerIdException(ConsumerId consumerId) {
        super("Duplicate consumerId '" + consumerId + "'");
    }
}
