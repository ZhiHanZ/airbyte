package io.airbyte.integrations.destination.databend;

import java.util.Map;

public class StageInfo {
    private String presignedURl;
    private Map<String, String> headers;
    // Constructor
    public StageInfo(String presignedURl, Map<String, String> headers) {
        this.presignedURl = presignedURl;
        this.headers = headers;
    }
    // Getter
    public String getPresignedURl() {
        return presignedURl;
    }
    public Map<String, String> getHeaders() {
        return headers;
    }
}