package com.floworchestrator.orchestrator.model;

import org.slf4j.MDC;

import java.time.OffsetDateTime;

/**
 * Evento inmutable de progreso de un flujo que se publica por SSE. Los campos
 * {@code userError*} y {@code flowCode} transportan el mensaje amigable y el
 * código de error para que Control Center los muestre al operador.
 */
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
    /** Evento de ciclo de vida (inicio, reanudación o cierre) sin detalle de paso. */
    public static FlowExecutionEvent of(String executionId, String flowId, String eventType) {
        return new FlowExecutionEvent(executionId, flowId, eventType, null, null, null, null, null, null,
                null, null, null, null, null, MDC.get("traceId"), OffsetDateTime.now());
    }

    /** Evento de paso (iniciado, completado o fallido) con su herramienta y servidor MCP. */
    public static FlowExecutionEvent withError(String executionId, String flowId, String eventType,
                                               Integer stepOrder, String stepName,
                                               String toolName, String server, String status,
                                               String errorMessage) {
        return new FlowExecutionEvent(executionId, flowId, eventType, stepOrder, stepName,
                toolName, server, status, errorMessage, null, null, null, null, null,
                MDC.get("traceId"), OffsetDateTime.now());
    }

    /** Evento de error que además lleva el texto amigable, la acción sugerida y el código de flujo. */
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
