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

/**
 * API REST de ejecución de flujos: crear (síncrono o asíncrono), reanudar,
 * cancelar y reintentar pasos, más un healthcheck propio.
 */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class FlowController {

    private static final Logger log = LoggerFactory.getLogger(FlowController.class);

    private final FlowService flowService;

    /**
     * POST /api/flows: ejecuta el flujo de forma síncrona y espera el resultado final.
     */
    @PostMapping("/flows")
    public ResponseEntity<FlowResponse> createFlow(@Valid @RequestBody FlowRequest request) {
        return ResponseEntity.ok(flowService.createFlow(request));
    }

    /**
     * POST /api/flows/async: inicia el flujo en segundo plano. Responde 202 de inmediato;
     * el progreso se sigue por {@code GET /api/flows/stream} (SSE) usando el traceId devuelto.
     */
    @PostMapping("/flows/async")
    public ResponseEntity<FlowAsyncAccepted> createFlowAsync(
            @Valid @RequestBody FlowRequest request) {
        return ResponseEntity.accepted().body(flowService.startFlowAsync(request));
    }

    /**
     * POST /api/flows/{executionId}/resume: reanuda el flujo en segundo plano. Responde
     * 202 en cuanto valida que hay estado pausado (404 si no lo hay); el cliente sigue
     * el resultado por SSE.
     */
    @PostMapping("/flows/{executionId}/resume")
    public ResponseEntity<FlowAsyncAccepted> resumeFlow(@PathVariable String executionId,
            @RequestParam(required = false) String userInput) {
        try {
            FlowAsyncAccepted response = flowService.resumeFlowAsync(executionId, userInput);
            return ResponseEntity.accepted().body(response);
        } catch (IllegalStateException ex) {
            log.warn("[RESUME] No se encontró estado pausado para la ejecución; se devuelve 404: executionId={}", executionId);
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * POST /api/flows/{executionId}/cancel: cancela un flujo pausado o fallido y
     * descarta su estado para que no pueda reanudarse. El Control Center lo invoca
     * cuando un operador rechaza la intervención humana.
     */
    @PostMapping("/flows/{executionId}/cancel")
    public ResponseEntity<FlowResponse> cancelFlow(@PathVariable String executionId) {
        try {
            return ResponseEntity.ok(flowService.cancelFlow(executionId));
        } catch (IllegalStateException ex) {
            log.warn("[CANCEL] No se encontró un flujo cancelable para la ejecución: executionId={}", executionId);
            return ResponseEntity.notFound().build();
        }
    }

    /** GET /api/flows/health: healthcheck simple del controlador; no consulta dependencias. */
    @GetMapping("/flows/health")
    public String health() {
        return "Flow Orchestrator OK";
    }

    /**
     * POST /api/flows/{executionId}/steps/{stepOrder}/retry: marca un paso con
     * ejecución previa para reintentarlo. Devuelve 404 si no hay estado pausado y
     * 400 si el paso no existe o no puede reintentarse.
     */
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
