package org.mmmq.consumer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "mmmq.consumer")
public class ConsumerProperties {

    private ConsumerMode mode = ConsumerMode.ASYNC;

    public ConsumerMode getMode() {
        return mode;
    }

    public void setMode(ConsumerMode mode) {
        this.mode = mode;
    }
}
