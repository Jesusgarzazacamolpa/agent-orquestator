package com.floworchestrator.orchestrator.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Resultado de una ejecución o reanudación de flujo: estado, paso actual y el
 * contrato de error amigable que consumen Control Center y la UI.
 */
@Data
@NoArgsConstructor
public class FlowResponse {
    private String id = UUID.randomUUID().toString();
    private String status = "CREATED";
    private String currentStep;
    private String errorMessage;
    private String userErrorTitle;
    private String userErrorMessage;
    private String userErrorActionLabel;
    private String userErrorStatusType;
    private String flowCode;
    private String traceId;
    private String message;
    private List<String> steps;
    private Map<String, Object> attributes = new HashMap<>();
}
