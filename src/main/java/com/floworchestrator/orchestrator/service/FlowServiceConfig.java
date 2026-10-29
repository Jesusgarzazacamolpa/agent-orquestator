package com.floworchestrator.orchestrator.service;

import java.time.Duration;
import java.util.UUID;

/**
 * Límites y timeouts del orquestador: proveedor y modelo activo del LLM,
 * turnos del agente, ventana de contexto, reintentos y circuit breaker de LLM
 * y MCP, y timeout hacia Control Center. Los valores reales los construye el
 * bean de {@code OrchestratorConfiguration}.
 */
public record FlowServiceConfig(
        String controlCenterBaseUrl,
        String llmProvider,
        String llmModel,
        int maxAgentTurns,
        int maxMcpRetries,
        Duration stepTimeout,
        int mcpCircuitBreakerThreshold,
        Duration mcpCircuitBreakerResetTimeout,
        int maxContextMessages,
        Duration llmTimeout,
        int llmMaxRetries,
        Duration llmRetryWait,
        int llmCircuitBreakerThreshold,
        Duration llmCircuitBreakerResetTimeout,
        Duration controlCenterTimeout
) {
    /** Configuración por defecto para pruebas o arranque sin propiedades externas (Ollama con {@code llama3.1}). */
    public static FlowServiceConfig defaults() {
        return new FlowServiceConfig(
                "http://localhost:8081",
                "ollama",
                "llama3.1",
                8,
                2,
                Duration.ofSeconds(30),
                5,
                Duration.ofSeconds(60),
                20,
                Duration.ofSeconds(60),
                3,
                Duration.ofSeconds(2),
                50,
                Duration.ofSeconds(60),
                Duration.ofSeconds(10)
        );
    }
}
