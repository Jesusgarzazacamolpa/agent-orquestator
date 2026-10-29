package com.floworchestrator.orchestrator.model;

/**
 * Respuesta inmediata del endpoint asíncrono de flujos.
 * El {@code traceId} permite correlacionar los eventos SSE mientras
 * Control Center resuelve/crea el executionId.
 */
public record FlowAsyncAccepted(
        String requestId,
        String flowId,
        String executionId,
        String traceId,
        String status
) {
    /** Crea la respuesta 202 con estado {@code ACCEPTED} para un flujo aceptado en segundo plano. */
    public static FlowAsyncAccepted accepted(String requestId, String flowId,
                                             String executionId, String traceId) {
        return new FlowAsyncAccepted(requestId, flowId, executionId, traceId, "ACCEPTED");
    }
}
