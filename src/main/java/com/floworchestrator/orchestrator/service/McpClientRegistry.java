package com.floworchestrator.orchestrator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.floworchestrator.orchestrator.config.FlowDefinitions;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Component
public class McpClientRegistry implements McpToolClient {
    private static final Logger log = LoggerFactory.getLogger(McpClientRegistry.class);

    private final FlowDefinitions flowDefinitions;
    private final ObjectMapper objectMapper;
    private final Map<String, McpSyncClient> clients = new ConcurrentHashMap<>();

    public McpClientRegistry(FlowDefinitions flowDefinitions, ObjectMapper objectMapper) {
        this.flowDefinitions = flowDefinitions;
        this.objectMapper = objectMapper;
    }

    @Override
    public List<DiscoveredTool> listTools(String serverName) {
        return withRetry(serverName, client -> client.listTools().tools().stream()
                .map(tool -> new DiscoveredTool(tool.name(), tool.description(), tool.inputSchema()))
                .toList());
    }

    @Override
    public Map<String, Object> callTool(String serverName, String toolName, Map<String, Object> arguments) {
        return withRetry(serverName, client -> {
            McpSchema.CallToolResult result = client.callTool(
                    McpSchema.CallToolRequest.builder(toolName)
                            .arguments(arguments)
                            .build());
            String content = textContent(result);
            if (Boolean.TRUE.equals(result.isError())) {
                throw toToolException(toolName, content);
            }
            return parseResult(content);
        });
    }

    private McpToolException toToolException(String toolName, String content) {
        Map<String, Object> payload = parseResult(content);
        String errorCode = payload.get("error_code") instanceof String code ? code : "TOOL_ERROR";
        boolean retryable = Boolean.TRUE.equals(payload.get("retryable"));
        return new McpToolException("Falló la herramienta MCP " + toolName + ": " + content,
                errorCode, retryable, payload);
    }

    private <T> T withRetry(String serverName, Function<McpSyncClient, T> operation) {
        try {
            return operation.apply(client(serverName));
        } catch (McpToolException ex) {
            // Un error de negocio no invalida la sesión ni debe repetirse a ciegas.
            throw ex;
        } catch (RuntimeException ex) {
            log.warn("[MCP] Sesión invalidada para '{}'; se recrea el cliente y se reintenta: {}", serverName, ex.getMessage());
            invalidateClient(serverName);
            return operation.apply(client(serverName));
        }
    }

    private void invalidateClient(String serverName) {
        McpSyncClient staleClient = clients.remove(serverName);
        if (staleClient != null) {
            try {
                staleClient.closeGracefully();
            } catch (Exception e) {
                log.warn("[MCP] No se pudo cerrar el cliente obsoleto de '{}': {}", serverName, e.getMessage());
            }
        }
    }

    private McpSyncClient client(String serverName) {
        return clients.computeIfAbsent(serverName, this::createClient);
    }

    private McpSyncClient createClient(String serverName) {
        String baseUrl = flowDefinitions.getMcpServers().get(serverName);
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("La URL del servidor MCP no está configurada: " + serverName);
        }
        log.info("[MCP] Creando cliente para '{}': baseUrl={}", serverName, baseUrl);
        HttpClientStreamableHttpTransport transport = HttpClientStreamableHttpTransport
                .builder(baseUrl)
                .endpoint("/mcp")
                .build();
        McpSyncClient client = McpClient.sync(transport).build();
        client.initialize();
        log.info("[MCP] Cliente inicializado para '{}'", serverName);
        return client;
    }

    private String textContent(McpSchema.CallToolResult result) {
        return result.content().stream()
                .filter(McpSchema.TextContent.class::isInstance)
                .map(McpSchema.TextContent.class::cast)
                .map(McpSchema.TextContent::text)
                .reduce((first, second) -> first + "\n" + second)
                .orElse("");
    }

    private Map<String, Object> parseResult(String content) {
        try {
            return objectMapper.readValue(content, Map.class);
        } catch (JsonProcessingException ex) {
            Map<String, Object> result = new HashMap<>();
            result.put("content", content);
            return result;
        }
    }

    @PreDestroy
    public void closeClients() {
        clients.values().forEach(client -> {
            try {
                client.closeGracefully();
            } catch (Exception e) {
                log.warn("[MCP] Error al cerrar el cliente durante el apagado: {}", e.getMessage());
            }
        });
        clients.clear();
    }
}
