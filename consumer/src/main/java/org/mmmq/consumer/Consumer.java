package org.mmmq.consumer;

import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.consumer.handler.execution.HandlerExecutionContainer;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.ConsumerAcknowledgement;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.Message;
import org.mmmq.core.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    private final HandlerExecutionContainer handlerExecutionContainer;

    public Consumer(HandlerExecutionContainer handlerExecutionContainer) {
        this.handlerExecutionContainer = handlerExecutionContainer;
    }

    @PostMapping("/mmmq/messages")
    public ResponseEntity<ConsumerAcknowledgement> receiveMessage(
            @RequestHeader Map<String, String> headers,
            @RequestBody Message message
    ) {
        Metadata metadata = new Metadata(headers);
        ConsumerId handlerId;
        try {
            handlerId = metadata.getConsumerId();
        } catch (IllegalArgumentException e) {
            log.warn("Received message with invalid consumer id header: {}", message, e);
            return ResponseEntity.ok(new ConsumerAcknowledgement(Acknowledgement.NACK));
        }
        if (handlerId == null) {
            log.warn("Received message without consumer id header: {}", message);
            return ResponseEntity.ok(new ConsumerAcknowledgement(Acknowledgement.NACK));
        }
        HandlerExecution execution = handlerExecutionContainer.find(handlerId);
        if (execution == null) {
            log.warn("No HandlerExecution found for id '{}', message: {}", handlerId, message);
            return ResponseEntity.ok(new ConsumerAcknowledgement(Acknowledgement.NACK));
        }
        try {
            execution.execute(message);
            return ResponseEntity.ok(new ConsumerAcknowledgement(Acknowledgement.ACK));
        } catch (Exception e) {
            log.warn("Handler execution failed for id '{}', message: {}", handlerId, message, e);
            return ResponseEntity.ok(new ConsumerAcknowledgement(Acknowledgement.NACK));
        }
    }
}
