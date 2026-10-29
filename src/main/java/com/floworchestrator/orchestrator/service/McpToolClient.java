package com.floworchestrator.orchestrator.service;

import java.util.List;
import java.util.Map;

/**
 * Puerto de acceso a los servidores MCP: descubrir herramientas y ejecutarlas.
 * Permite sustituir la implementación síncrona HTTP por una de prueba.
 */
public interface McpToolClient {
    /** Devuelve las herramientas publicadas por el servidor, con su esquema de entrada. */
    List<DiscoveredTool> listTools(String serverName);

    /**
     * Ejecuta una herramienta MCP.
     *
     * @return Resultado ya parseado como mapa JSON.
     * @throws McpToolException si la herramienta reporta un error de negocio.
     */
    Map<String, Object> callTool(String serverName, String toolName, Map<String, Object> arguments);

    /** Herramienta descubierta en un servidor MCP. */
    record DiscoveredTool(String name, String description, Map<String, Object> inputSchema) {
    }
}