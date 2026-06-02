package org.mmmq.core.identifier;

import java.util.regex.Pattern;

public record ConsumerId(

        String value
) {

    private static final Pattern PATTERN = Pattern.compile("[A-Za-z0-9._-]+");

    public ConsumerId {
        if (value == null || !PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "consumerId must match [A-Za-z0-9._-]+, but was: " + value
            );
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
