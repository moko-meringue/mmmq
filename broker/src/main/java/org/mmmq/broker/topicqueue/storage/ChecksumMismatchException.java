package org.mmmq.broker.topicqueue.storage;

class ChecksumMismatchException extends CorruptionException {

    ChecksumMismatchException(String message) {
        super(message);
    }
}