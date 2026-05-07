package org.mmmq.broker.topicqueue.storage;

class MessageSerializationException extends StorageException {

    MessageSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}