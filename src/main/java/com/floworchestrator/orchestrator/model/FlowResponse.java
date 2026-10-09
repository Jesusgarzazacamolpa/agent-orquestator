package com.floworchestrator.orchestrator.model;

import java.util.List;
import java.util.UUID;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class FlowResponse {
    private String id = UUID.randomUUID().toString();
    private String customerId;
    private String vehicleId;
    private String salesAgentId;
    private String status = "CREATED";
    private String currentStep;
    private String errorMessage;
    private String message;
    private List<String> steps;
}
