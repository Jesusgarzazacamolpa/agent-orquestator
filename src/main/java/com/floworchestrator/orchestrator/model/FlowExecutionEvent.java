package com.floworchestrator.orchestrator.model;

import org.slf4j.MDC;

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
        String userErrorTitle,
        String userErrorMessage,
        String userErrorActionLabel,
        String userErrorStatusType,
        String flowCode,
        String traceId,
        OffsetDateTime timestamp
) {
    public static FlowExecutionEvent of(String executionId, String flowId, String eventType) {
        return new FlowExecutionEvent(executionId, flowId, eventType, null, null, null, null, null, null,
                null, null, null, null, null, MDC.get("traceId"), OffsetDateTime.now());
    }

    public static FlowExecutionEvent withError(String executionId, String flowId, String eventType,
                                               Integer stepOrder, String stepName,
                                               String toolName, String server, String status,
                                               String errorMessage) {
        return new FlowExecutionEvent(executionId, flowId, eventType, stepOrder, stepName,
                toolName, server, status, errorMessage, null, null, null, null, null,
                MDC.get("traceId"), OffsetDateTime.now());
    }

    public static FlowExecutionEvent withUserAgentError(String executionId, String flowId, String eventType,
                                                         Integer stepOrder, String stepName,
                                                         String toolName, String server, String status,
                                                         String errorMessage, String userTitle,
                                                         String userMessage, String userActionLabel,
                                                         String userErrorStatusType, String flowCode) {
        return new FlowExecutionEvent(executionId, flowId, eventType, stepOrder, stepName,
                toolName, server, status, errorMessage, userTitle, userMessage, userActionLabel,
                userErrorStatusType, flowCode, MDC.get("traceId"), OffsetDateTime.now());
    }
}
