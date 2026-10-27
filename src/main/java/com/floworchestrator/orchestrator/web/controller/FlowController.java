package com.floworchestrator.orchestrator.web.controller;

import com.floworchestrator.orchestrator.model.FlowAsyncAccepted;
import com.floworchestrator.orchestrator.model.FlowRequest;
import com.floworchestrator.orchestrator.model.FlowResponse;
import com.floworchestrator.orchestrator.service.FlowService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class FlowController {

    private static final Logger log = LoggerFactory.getLogger(FlowController.class);

    private final FlowService flowService;

    @PostMapping("/flows")
    public ResponseEntity<FlowResponse> createFlow(@Valid @RequestBody FlowRequest request) {
        return ResponseEntity.ok(flowService.createFlow(request));
    }

    /**
     * Inicia el flujo en segundo plano. Responde 202 de inmediato; el progreso
     * se sigue por {@code GET /api/flows/stream} (SSE) usando el traceId devuelto.
     */
    @PostMapping("/flows/async")
    public ResponseEntity<FlowAsyncAccepted> createFlowAsync(
            @Valid @RequestBody FlowRequest request) {
        return ResponseEntity.accepted().body(flowService.startFlowAsync(request));
    }

    @PostMapping("/flows/{executionId}/resume")
    public ResponseEntity<FlowResponse> resumeFlow(@PathVariable String executionId,
            @RequestParam(required = false) String userInput) {
        try {
            FlowResponse response = flowService.resumeFlow(executionId, userInput);
            return ResponseEntity.ok(response);
        } catch (IllegalStateException ex) {
            log.warn("[RESUME] No se encontró estado pausado para la ejecución; se devuelve 404: executionId={}", executionId);
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/flows/health")
    public String health() {
        return "Flow Orchestrator OK";
    }

    @PostMapping("/flows/{executionId}/steps/{stepOrder}/retry")
    public ResponseEntity<FlowResponse> retryStep(@PathVariable String executionId,
            @PathVariable int stepOrder) {
        try {
            FlowResponse response = flowService.retryStep(executionId, stepOrder);
            return ResponseEntity.ok(response);
        } catch (IllegalStateException ex) {
            log.warn("[RETRY-STEP] No se puede reintentar el paso: executionId={}, step={}, error={}",
                    executionId, stepOrder, ex.getMessage());
            return ResponseEntity.notFound().build();
        } catch (IllegalArgumentException ex) {
            log.warn("[RETRY-STEP] Solicitud de reintento inválida: executionId={}, step={}, error={}",
                    executionId, stepOrder, ex.getMessage());
            return ResponseEntity.badRequest().build();
        }
    }
}
