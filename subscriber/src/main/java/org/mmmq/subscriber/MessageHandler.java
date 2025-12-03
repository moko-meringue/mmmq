package org.mmmq.subscriber;

import org.mmmq.core.message.Message;

abstract class MessageHandler {

    final String name;
    final String topic;

    MessageHandler(String name, String topic) {
        this.name = name;
        this.topic = topic;
    }

    abstract void handle(Message message);
}
