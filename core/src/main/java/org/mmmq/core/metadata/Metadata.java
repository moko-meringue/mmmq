package org.mmmq.core.metadata;

import org.springframework.lang.Nullable;

import java.util.HashMap;
import java.util.Map;

public class Metadata {

    static final String HANDLER_ID = "MMMQ-Handler-Id";

    private final Map<String, String> headers;

    public Metadata() {
        this.headers = new HashMap<>();
    }

    public Metadata(Map<String, String> source) {
        this.headers = new HashMap<>(source);
    }

    public void setHandlerId(String handlerId) {
        headers.put(HANDLER_ID, handlerId);
    }

    @Nullable
    public String getHandlerId() {
        return headers.get(HANDLER_ID);
    }

    public Map<String, String> toMap() {
        return Map.copyOf(headers);
    }
}
