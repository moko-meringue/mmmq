package org.mmmq.core.message;

import java.util.regex.Pattern;

public record Topic(
        String name
) {

    private static final Pattern VALID_NAME = Pattern.compile("^[a-zA-Z0-9._-]+$");

    public Topic {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be blank");
        }
        if (!VALID_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("Topic name contains invalid characters: " + name);
        }
    }
}
