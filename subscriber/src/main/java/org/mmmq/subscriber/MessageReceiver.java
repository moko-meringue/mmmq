package org.mmmq.subscriber;

import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.SubscriberAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.subscriber.handler.FrontHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageReceiver {

    private static final Logger log = LoggerFactory.getLogger(MessageReceiver.class);

    final FrontHandler frontHandler;

    public MessageReceiver(FrontHandler frontHandler) {
        this.frontHandler = frontHandler;
    }

    @PostMapping("/messages")
    public ResponseEntity<SubscriberAcknowledgement> receiveMessage(@RequestBody Message message) {
        SubscriberAcknowledgement acknowledgement = new SubscriberAcknowledgement(Acknowledgement.ACK);
        try {
            frontHandler.handleMessage(message);
        } catch (Exception e) {
            log.warn("Failed to receive message: {}", message, e);
            acknowledgement = new SubscriberAcknowledgement(Acknowledgement.NACK);
        }
        return ResponseEntity.ok(acknowledgement);
    }
}
