package com.floworchestrator.orchestrator.model;

import java.time.OffsetDateTime;

public record FlowExecutionEvent(
        String executionId,
        String flowId,
        String eventType,
        Integer stepOrder,
        String stepName,
        String toolName,
        String server,
        String status,
        String errorMessage,
        OffsetDateTime timestamp
) {
    public static FlowExecutionEvent of(String executionId, String flowId, String eventType) {
        return new FlowExecutionEvent(executionId, flowId, eventType, null, null, null, null, null, null, OffsetDateTime.now());
    }
}
