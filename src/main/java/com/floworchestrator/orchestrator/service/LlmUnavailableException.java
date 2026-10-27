package com.floworchestrator.orchestrator.service;

/**
 * Señala que el LLM no pudo completar la llamada tras agotar reintentos o con
 * el circuit breaker abierto. Transporta el código de flujo y el mensaje
 * amigable que debe mostrarse al usuario final.
 */
public class LlmUnavailableException extends RuntimeException {

    private final String flowCode;
    private final String userMessage;
    private final String userAction;

    public LlmUnavailableException(String technicalMessage, String flowCode,
                                   String userMessage, String userAction) {
        super(technicalMessage);
        this.flowCode = flowCode;
        this.userMessage = userMessage;
        this.userAction = userAction;
    }

    public String getFlowCode() {
        return flowCode;
    }

    public String getUserMessage() {
        return userMessage;
    }

    public String getUserAction() {
        return userAction;
    }
}
