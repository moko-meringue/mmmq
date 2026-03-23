package org.mmmq.consumer.handler.execution;

import org.mmmq.core.message.Message;
import org.mmmq.core.message.Pattern;
import org.mmmq.core.message.Topic;

public abstract class HandlerExecution {

    protected final String name;
    protected final Pattern pattern;

    protected HandlerExecution(String name, Pattern pattern) {
        this.name = name;
        this.pattern = pattern;
    }

    public abstract void execute(Message message);

    public final boolean supports(Message message) {
        return pattern.equals(message.pattern());
    }

    public String getName() {
        return name;
    }

    public Pattern getPattern() {
        return pattern;
    }
}
