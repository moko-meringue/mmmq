package org.mmmq.consumer;

import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.consumer.handler.execution.HandlerExecutions;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.ConsumerAcknowledgement;
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

    private final HandlerExecutions handlerExecutions;

    public Consumer(HandlerExecutions handlerExecutions) {
        this.handlerExecutions = handlerExecutions;
    }

    @PostMapping("/mmmq/messages")
    public ResponseEntity<ConsumerAcknowledgement> receiveMessage(
            @RequestHeader Map<String, String> headers,
            @RequestBody Message message
    ) {
        Metadata metadata = new Metadata(headers);
        String handlerId = metadata.getHandlerId();
        if (handlerId == null) {
            log.warn("Received message without handler id header: {}", message);
            return ResponseEntity.ok(new ConsumerAcknowledgement(Acknowledgement.NACK));
        }
        HandlerExecution execution = handlerExecutions.find(handlerId);
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
