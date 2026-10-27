package com.floworchestrator.orchestrator.service;

/**
 * Lanzada cuando una herramienta MCP indica que la ejecución del flujo
 * requiere intervención humana antes de poder continuar.
 * <p>
 * A diferencia de {@link IllegalStateException}, esta excepción no marca
 * el flujo como fallido; en su lugar pausa la ejecución y crea un registro
 * de intervención que puede ser revisado y resuelto por un usuario.
 */
public class RequiresHumanInterventionException extends RuntimeException {

    public RequiresHumanInterventionException(String message) {
        super(message);
    }

    public RequiresHumanInterventionException(String message, Throwable cause) {
        super(message, cause);
    }
}
