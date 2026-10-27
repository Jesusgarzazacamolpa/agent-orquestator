package com.floworchestrator.orchestrator.service;

import java.time.Duration;
import java.util.UUID;

public record FlowServiceConfig(
        String controlCenterBaseUrl,
        String ollamaBaseUrl,
        String ollamaModel,
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
    public static FlowServiceConfig defaults() {
        return new FlowServiceConfig(
                "http://localhost:8081",
                "http://localhost:11434",
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
