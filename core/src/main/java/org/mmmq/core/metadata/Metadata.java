package org.mmmq.core.metadata;

import java.util.HashMap;
import java.util.Map;
import org.mmmq.core.annotation.Nullable;
import org.mmmq.core.identifier.ConsumerId;

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
    public ConsumerId getConsumerId() {
        String raw = headers.get(CONSUMER_ID);
        return raw == null ? null : new ConsumerId(raw);
    }

    public void setConsumerId(ConsumerId consumerId) {
        headers.put(CONSUMER_ID, consumerId.value());
    }

    public Map<String, String> toMap() {
        return Map.copyOf(headers);
    }
}
