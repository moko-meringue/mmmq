package org.mmmq.core.acknowledgement;

public record ConsumerAcknowledgement(
        Acknowledgement acknowledgement
) {

    public boolean isAck() {
        return this.acknowledgement == Acknowledgement.ACK;
    }
}
