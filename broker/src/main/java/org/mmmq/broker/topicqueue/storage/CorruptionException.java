package org.mmmq.broker.topicqueue.storage;

public class CorruptionException extends StorageException {

    public CorruptionException(String message) {
        super(message);
    }
}
