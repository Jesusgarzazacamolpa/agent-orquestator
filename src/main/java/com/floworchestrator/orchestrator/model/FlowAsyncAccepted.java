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
    public static FlowAsyncAccepted accepted(String requestId, String flowId,
                                             String executionId, String traceId) {
        return new FlowAsyncAccepted(requestId, flowId, executionId, traceId, "ACCEPTED");
    }
}
