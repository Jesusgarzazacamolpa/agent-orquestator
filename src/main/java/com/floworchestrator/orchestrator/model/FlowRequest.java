package com.floworchestrator.orchestrator.model;

import lombok.Data;

@Data
public class FlowRequest {
    private String flowId = "compra-vehiculo";
    private String customerId;
    private String vehicleId;
    private String salesAgentId;
}
