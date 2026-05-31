package org.mmmq.core.metadata;

import java.util.HashMap;
import java.util.Map;
import org.mmmq.core.annotation.Nullable;

public class Metadata {

    static final String HANDLER_ID = "MMMQ-Handler-Id";

    private final Map<String, String> headers;

    public Metadata() {
        this.headers = new HashMap<>();
    }

    public Metadata(Map<String, String> source) {
        this.headers = new HashMap<>(source);
    }

    @Nullable
    public String getHandlerId() {
        return headers.get(HANDLER_ID);
    }

    public void setHandlerId(String handlerId) {
        headers.put(HANDLER_ID, handlerId);
    }

    public Map<String, String> toMap() {
        return Map.copyOf(headers);
    }
}
