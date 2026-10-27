package com.floworchestrator.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.floworchestrator.orchestrator.config.FlowDefinitions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class McpClientRegistryIntegrationTest {

    @Test
    void javaClientDiscoversAndCallsNodeMcpTool() throws Exception {
        FlowDefinitions flowDefinitions = new FlowDefinitions();
        ObjectMapper objectMapper = new ObjectMapper();
        int port = availablePort();
        Path notificationsDirectory = Path.of("..", "mcp-notifications").toAbsolutePath().normalize();
        ProcessBuilder processBuilder = new ProcessBuilder("node", "server.js")
                .directory(notificationsDirectory.toFile())
            .redirectErrorStream(true);
        processBuilder.environment().put("PORT", String.valueOf(port));
        Process serverProcess = processBuilder.start();
        flowDefinitions.getMcpServers().put("mcp-notifications", "http://127.0.0.1:" + port);

        McpClientRegistry registry = new McpClientRegistry(flowDefinitions, objectMapper);
        try {
            waitForHealth(port, serverProcess);
            var tools = registry.listTools("mcp-notifications");
            assertEquals("send_purchase_status_email", tools.getFirst().name());

            Map<String, Object> result = registry.callTool(
                    "mcp-notifications",
                    "send_purchase_status_email",
                    Map.of(
                            "recipientEmail", "customer@example.com",
                            "templateId", "ORDER_CREATED",
                            "data", Map.of("order", "purchase-123")));

            assertEquals("SENT", result.get("status"));
            assertEquals("purchase-123", ((Map<?, ?>) result.get("payload")).get("order"));
        } finally {
            registry.closeClients();
            serverProcess.destroy();
            if (!serverProcess.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                serverProcess.destroyForcibly();
            }
        }
    }

    private int availablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void waitForHealth(int port, Process serverProcess) throws Exception {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(1))
                .build();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/health"))
                .timeout(Duration.ofSeconds(1))
                .GET()
                .build();
        IOException lastError = null;
        for (int attempt = 0; attempt < 40; attempt++) {
            if (!serverProcess.isAlive()) {
                throw new IllegalStateException("Node MCP process exited: " +
                        new String(serverProcess.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
            }
            try {
                HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200) {
                    return;
                }
            } catch (IOException ex) {
                lastError = ex;
                Thread.sleep(100);
            }
        }
        throw new IllegalStateException("Node MCP server did not become healthy", lastError);
    }
}
