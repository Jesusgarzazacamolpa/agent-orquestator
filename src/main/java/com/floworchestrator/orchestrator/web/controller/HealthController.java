package com.floworchestrator.orchestrator.web.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Healthcheck básico del servicio, separado del expuesto por FlowController.
 */
@RestController
@RequestMapping("/api")
public class HealthController {

    /** GET /api/health: responde 200 con un texto fijo para liveness/readiness. */
    @GetMapping("/health")
    public String health() {
        return "Orchestrator OK";
    }
}
