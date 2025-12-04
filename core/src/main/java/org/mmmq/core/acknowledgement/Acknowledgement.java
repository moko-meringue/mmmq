package org.mmmq.core.acknowledgement;

public enum Acknowledgement {
    ACK("ACK"),
    NACK("NACK"),
    ;

    private final String code;

    Acknowledgement(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
