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
            if (Boolean.TRUE.equals(result.isError())) {
                throw new IllegalStateException("MCP tool failed: " + textContent(result));
            }
            return parseResult(textContent(result));
        });
    }

    private <T> T withRetry(String serverName, Function<McpSyncClient, T> operation) {
        try {
            return operation.apply(client(serverName));
        } catch (RuntimeException ex) {
            log.warn("[MCP] Session invalidated for '{}', recreating client and retrying: {}", serverName, ex.getMessage());
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
                log.warn("[MCP] Failed to close stale client for '{}': {}", serverName, e.getMessage());
            }
        }
    }

    private McpSyncClient client(String serverName) {
        return clients.computeIfAbsent(serverName, this::createClient);
    }

    private McpSyncClient createClient(String serverName) {
        String baseUrl = flowDefinitions.getMcpServers().get(serverName);
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("MCP server URL is not configured: " + serverName);
        }
        log.info("[MCP] Creating client for '{}': baseUrl={}", serverName, baseUrl);
        HttpClientStreamableHttpTransport transport = HttpClientStreamableHttpTransport
                .builder(baseUrl)
                .endpoint("/mcp")
                .build();
        McpSyncClient client = McpClient.sync(transport).build();
        client.initialize();
        log.info("[MCP] Client initialized for '{}'", serverName);
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
                log.warn("[MCP] Error closing client during shutdown: {}", e.getMessage());
            }
        });
        clients.clear();
    }
}
