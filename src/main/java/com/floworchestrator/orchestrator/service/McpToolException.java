package com.floworchestrator.orchestrator.service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Error de negocio reportado por una herramienta MCP. La sesión sigue siendo
 * válida: solo se reintenta cuando el propio error se declara retryable.
 */
public class McpToolException extends RuntimeException {

    private final String errorCode;
    private final boolean retryable;
    private final Map<String, Object> payload;

    public McpToolException(String message, String errorCode, boolean retryable) {
        this(message, errorCode, retryable, Map.of());
    }

    public McpToolException(String message, String errorCode, boolean retryable,
                            Map<String, Object> payload) {
        super(message);
        this.errorCode = errorCode;
        this.retryable = retryable;
        this.payload = payload == null ? Map.of() : new LinkedHashMap<>(payload);
    }

    public String getErrorCode() {
        return errorCode;
    }

    public boolean isRetryable() {
        return retryable;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }
}
