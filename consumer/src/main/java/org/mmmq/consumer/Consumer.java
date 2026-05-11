package org.mmmq.consumer;

import org.mmmq.consumer.handler.FrontHandler;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.ConsumerAcknowledgement;
import org.mmmq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    final FrontHandler frontHandler;

    public Consumer(FrontHandler frontHandler) {
        this.frontHandler = frontHandler;
    }

    @PostMapping("/mmmq/messages")
    public ResponseEntity<ConsumerAcknowledgement> receiveMessage(@RequestBody Message message) {
        ConsumerAcknowledgement acknowledgement = new ConsumerAcknowledgement(Acknowledgement.ACK);
        try {
            frontHandler.handle(message);
        } catch (Exception e) {
            log.warn("Failed to receive message: {}", message, e);
            acknowledgement = new ConsumerAcknowledgement(Acknowledgement.NACK);
        }
        return ResponseEntity.ok(acknowledgement);
    }
}
