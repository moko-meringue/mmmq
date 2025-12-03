package org.mmmq.core.acknowledgement;

public record SubscriberAcknowledgement(
        Acknowledgement acknowledgement
) {

    public boolean isAck() {
        return this.acknowledgement == Acknowledgement.ACK;
    }
}
