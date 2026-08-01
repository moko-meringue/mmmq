package org.mmmq.broker.dispatcher.api;

import java.util.List;
import org.mmmq.broker.dispatcher.DispatcherContainer;
import org.mmmq.broker.dispatcher.DispatcherSnapshot;
import org.mmmq.broker.dispatcher.exception.DispatcherNotFoundException;
import org.mmmq.broker.dispatcher.exception.DuplicateConsumerIdException;
import org.mmmq.core.Host;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 브로커가 도는 중에 Dispatcher를 조회·추가·수정·삭제하는 REST 엔드포인트.
 *
 * <p>문자열로 들어온 요청을 도메인 값 타입으로 해석해 {@link org.mmmq.broker.dispatcher.DispatcherContainer}에
 * 넘기고, 돌려받은 스냅샷을 {@link DispatcherDefinition}으로 옮겨 응답한다. 검증 실패가 여기서 400이 된다.
 *
 * <p>예외 처리를 전역 {@code @RestControllerAdvice}가 아니라 이 클래스 안에 두는 이유는
 * broker가 라이브러리이기 때문이다 — 전역으로 두면 호스트 애플리케이션의 다른 컨트롤러 예외까지 가로챈다.
 * 같은 이유로 {@code @ResponseStatus}도 쓰지 않는다. 그쪽은 응답 본문을 호스트의 오류 페이지에 넘긴다.
 */
@RestController
@RequestMapping("/mmmq/dispatchers")
public class DispatcherController {

    private static final Logger log = LoggerFactory.getLogger(DispatcherController.class);

    private final DispatcherContainer container;

    public DispatcherController(DispatcherContainer container) {
        this.container = container;
    }

    @GetMapping
    public List<DispatcherDefinition> getDispatchers() {
        return container.snapshots().stream()
                .map(DispatcherDefinition::from)
                .toList();
    }

    @PostMapping
    public ResponseEntity<DispatcherDefinition> postDispatcher(@RequestBody DispatcherDefinition definition) {
        DispatcherSnapshot registered = container.add(
                new ConsumerId(definition.consumerId()),
                Host.from(definition.host()),
                new TopicPattern(definition.pattern())
        );
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(DispatcherDefinition.from(registered));
    }

    @PutMapping("/{consumerId}")
    public DispatcherDefinition putDispatcher(
            @PathVariable String consumerId,
            @RequestBody DispatcherRoute route
    ) {
        DispatcherSnapshot modified = container.modify(
                new ConsumerId(consumerId),
                Host.from(route.host()),
                new TopicPattern(route.pattern())
        );
        return DispatcherDefinition.from(modified);
    }

    @DeleteMapping("/{consumerId}")
    public ResponseEntity<Void> deleteDispatcher(@PathVariable String consumerId) {
        container.remove(new ConsumerId(consumerId));
        return ResponseEntity.noContent().build();
    }

    @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<String> handleBadRequest(Exception exception) {
        return ResponseEntity.badRequest()
                .body(exception.getMessage());
    }

    @ExceptionHandler(DuplicateConsumerIdException.class)
    public ResponseEntity<String> handleConflict(DuplicateConsumerIdException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(exception.getMessage());
    }

    @ExceptionHandler(DispatcherNotFoundException.class)
    public ResponseEntity<String> handleNotFound(DispatcherNotFoundException exception) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(exception.getMessage());
    }

    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<String> handleUnexpected(RuntimeException exception) {
        log.error("Unexpected failure while handling dispatcher request", exception);
        return ResponseEntity.internalServerError()
                .body("Internal server error");
    }
}
