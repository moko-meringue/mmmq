package org.mmmq.broker.topicqueue.storage;

class ChecksumMismatchException extends StorageException {

    ChecksumMismatchException(String message) {
        super(message);
    }
}
