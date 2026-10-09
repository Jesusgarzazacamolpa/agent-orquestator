package com.floworchestrator.orchestrator.service;

import java.util.List;
import java.util.Map;

public interface McpToolClient {
    List<DiscoveredTool> listTools(String serverName);

    Map<String, Object> callTool(String serverName, String toolName, Map<String, Object> arguments);

    record DiscoveredTool(String name, String description, Map<String, Object> inputSchema) {
    }
}