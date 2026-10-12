package com.floworchestrator.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class FlowRequest {
    private String flowId;
    private String executionId;

    @JsonIgnore
    private Map<String, Object> attributes = new HashMap<>();

    @JsonAnySetter
    public void setAttribute(String name, Object value) {
        if ("attributes".equals(name) && value instanceof Map) {
            attributes.putAll((Map<String, Object>) value);
        } else if (!"flowId".equals(name) && !"executionId".equals(name)) {
            attributes.put(name, value);
        }
    }
}
