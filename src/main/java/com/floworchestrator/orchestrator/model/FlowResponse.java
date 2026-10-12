package com.floworchestrator.orchestrator.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Data
@NoArgsConstructor
public class FlowResponse {
    private String id = UUID.randomUUID().toString();
    private String status = "CREATED";
    private String currentStep;
    private String errorMessage;
    private String message;
    private List<String> steps;
    private Map<String, Object> attributes = new HashMap<>();
}
