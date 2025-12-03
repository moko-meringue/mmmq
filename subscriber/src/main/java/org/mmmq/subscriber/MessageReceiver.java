package org.mmmq.subscriber;

import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.SubscriberAcknowledgement;
import org.mmmq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageReceiver {

    private static final Logger log = LoggerFactory.getLogger(MessageReceiver.class);

    private final ApplicationEventPublisher eventPublisher;

    public MessageReceiver(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @PostMapping("/messages")
    public ResponseEntity<SubscriberAcknowledgement> receiveMessage(@RequestBody Message message) {
        SubscriberAcknowledgement acknowledgement = new SubscriberAcknowledgement(Acknowledgement.ACK);
        try {
            eventPublisher.publishEvent(new MMMQEvent(this, message));
        } catch (Exception e) {
            log.warn("Failed to receive message: {}", message, e);
            acknowledgement = new SubscriberAcknowledgement(Acknowledgement.NAK);
        }
        return ResponseEntity.ok(acknowledgement);
    }
}
