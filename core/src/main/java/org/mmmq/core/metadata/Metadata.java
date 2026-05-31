package org.mmmq.core.metadata;

import java.util.HashMap;
import java.util.Map;
import org.mmmq.core.annotation.Nullable;

public class Metadata {

    static final String CONSUMER_ID = "mmmq-consumer-id";

    private final Map<String, String> headers;

    public Metadata() {
        this.headers = new HashMap<>();
    }

    public Metadata(Map<String, String> source) {
        this.headers = new HashMap<>(source);
    }

    @Nullable
    public String getConsumerId() {
        return headers.get(CONSUMER_ID);
    }

    public void setConsumerId(String consumerId) {
        headers.put(CONSUMER_ID, consumerId);
    }

    public Map<String, String> toMap() {
        return Map.copyOf(headers);
    }
}
