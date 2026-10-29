package com.floworchestrator.orchestrator.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Catálogo de flujos declarado en {@code flow-definitions.yml}: asocia cada flowId
 * con sus prompts, sus pasos MCP y la URL de cada servidor MCP disponible.
 */
@Data
@Component
@ConfigurationProperties(prefix = "orchestrator")
public class FlowDefinitions {
    /** Flujos indexados por flowId lógico. */
    private Map<String, FlowDefinition> flows = new LinkedHashMap<>();
    /** Nombre lógico del servidor MCP a su URL base. */
    private Map<String, String> mcpServers = new LinkedHashMap<>();

    /**
     * Contrato de un flujo: plantilla de proceso del Control Center, prompts y
     * la secuencia de herramientas MCP que el agente debe ejecutar.
     */
    @Data
    public static class FlowDefinition {
        private String processTemplateName;
        private String systemPromptTemplate;
        private String userPromptTemplate;
        private List<ToolDefinition> tools = new ArrayList<>();
    }

    /**
     * Paso del flujo respaldado por una herramienta MCP; {@code required} decide
     * si el flujo puede cerrarse sin haberlo completado y {@code dependsOn} el
     * orden en que se ofrecen las herramientas.
     */
    @Data
    public static class ToolDefinition {
        private String server;
        private String name;
        private int stepOrder;
        private String stepName;
        private boolean required = true;
        private List<String> dependsOn = new ArrayList<>();
    }
}