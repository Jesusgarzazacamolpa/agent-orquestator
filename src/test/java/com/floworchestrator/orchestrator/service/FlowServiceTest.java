package com.floworchestrator.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.floworchestrator.orchestrator.config.FlowDefinitions;
import com.floworchestrator.orchestrator.model.FlowRequest;
import com.floworchestrator.orchestrator.service.McpToolClient;
import com.floworchestrator.orchestrator.service.McpToolClient.DiscoveredTool;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class FlowServiceTest {

    @Autowired
    private FlowDefinitions flowDefinitions;

    @Test
    void shouldCompleteFlowAndReportEachStatus() throws IOException {
        List<String> statusUpdates = new CopyOnWriteArrayList<>();
        List<List<String>> offeredToolsPerTurn = new CopyOnWriteArrayList<>();
        HttpServer server = startServer(statusUpdates, offeredToolsPerTurn, false);
        McpToolClient registry = fakeMcpToolClient(false);
        FlowService service = new FlowService(
                WebClient.builder(),
                baseUrl(server),
                baseUrl(server),
                "llama3.1",
                8,
                new ObjectMapper(),
            flowDefinitions,
            registry
        );

        FlowRequest request = new FlowRequest();
        request.setCustomerId("customer-001");
        request.setVehicleId("vehicle-123");
        request.setSalesAgentId("agent-77");
        request.setFlowId("Compra de vehiculo");

        try {
            var flow = service.createFlow(request);

            assertEquals("execution-123", flow.getId());
            assertEquals("COMPLETED", flow.getStatus());
            assertEquals("customer-001", flow.getCustomerId());
            assertEquals("vehicle-123", flow.getVehicleId());
            assertFalse(flow.getSteps().isEmpty());
            assertTrue(statusUpdates.get(0).contains("Ejecuta el flujo compra-vehiculo"));
            assertTrue(statusUpdates.get(0).contains("Cliente: customer-001"));
            assertFalse(statusUpdates.get(0).contains("<customerId>"));
            assertEquals(10, statusUpdates.size());
            assertEquals(List.of("get_vehicle_information"), offeredToolsPerTurn.get(0));
            assertEquals(List.of("get_vehicle_information"), offeredToolsPerTurn.get(1));
            assertEquals(List.of("register_vehicle_purchase"), offeredToolsPerTurn.get(2));
            assertEquals(List.of("set_payment_method"), offeredToolsPerTurn.get(3));
            assertEquals(List.of("send_purchase_status_email"), offeredToolsPerTurn.get(4));
            assertTrue(offeredToolsPerTurn.get(5).isEmpty());
            assertTrue(statusUpdates.get(1).contains("/steps/1 true false false"));
            assertTrue(statusUpdates.get(2).contains("/steps/1 false true false"));
                assertTrue(statusUpdates.get(8).contains("/steps/4 false true false"), statusUpdates.toString());
                assertTrue(statusUpdates.get(9).contains("/status") && statusUpdates.get(9).contains("COMPLETED"),
                    statusUpdates.toString());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void shouldReportFailedStepWhenDomainServiceFails() throws IOException {
        List<String> statusUpdates = new CopyOnWriteArrayList<>();
        List<List<String>> offeredToolsPerTurn = new CopyOnWriteArrayList<>();
        HttpServer server = startServer(statusUpdates, offeredToolsPerTurn, true);
        McpToolClient registry = fakeMcpToolClient(true);
        FlowService service = new FlowService(
                WebClient.builder(),
                baseUrl(server),
                baseUrl(server),
                "llama3.1",
                8,
                new ObjectMapper(),
            flowDefinitions,
            registry
        );
        FlowRequest request = new FlowRequest();
        request.setCustomerId("customer-001");
        request.setVehicleId("vehicle-123");
        request.setSalesAgentId("agent-77");

        try {
            var flow = service.createFlow(request);

            assertEquals("FAILED", flow.getStatus());
            assertEquals("registrar_compra", flow.getCurrentStep());
            assertNotNull(flow.getErrorMessage());
            assertTrue(statusUpdates.get(statusUpdates.size() - 1).contains("/steps/2 false false true"));
        } finally {
            server.stop(0);
        }
    }

        private HttpServer startServer(
            List<String> statusUpdates,
            List<List<String>> offeredToolsPerTurn,
            boolean failPurchase) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger chatCalls = new AtomicInteger();
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            if ("/api/chat".equals(path) && "POST".equals(method)) {
                int call = chatCalls.getAndIncrement();
                String chatRequest = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                offeredToolsPerTurn.add(extractToolNames(chatRequest));
                if (call == 0) {
                    statusUpdates.add(chatRequest);
                }
                List<String> tools = List.of(
                        "get_vehicle_information",
                        "register_vehicle_purchase",
                        "set_payment_method",
                        "send_purchase_status_email"
                );
                if (call == 0) {
                    respond(exchange, 200, malformedToolCallResponse());
                } else if (call - 1 < (failPurchase ? 2 : tools.size())) {
                    respond(exchange, 200, toolCallResponse(tools.get(call - 1)));
                } else {
                    respond(exchange, 200, "{\"message\":{\"role\":\"assistant\",\"content\":\"Compra completada\"}}");
                }
            } else if ("/api/process-templates/by-name".equals(path) && "GET".equals(method)) {
                respond(exchange, 200, "{\"id\":\"template-123\"}");
            } else if ("/api/processes".equals(path) && "POST".equals(method)) {
                respond(exchange, 201, "{\"id\":\"execution-123\",\"status\":\"PENDING\"}");
            } else if (path.contains("/steps/") && "PUT".equals(method)) {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                statusUpdates.add(path + " " + body.contains("RUNNING") + " " + body.contains("COMPLETED")
                        + " " + body.contains("FAILED"));
                respond(exchange, 200, "{}");
            } else if (path.endsWith("/status") && "PATCH".equals(method)) {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                statusUpdates.add(path + " " + body.replaceAll("[\\r\\n]", ""));
                respond(exchange, 204, "");
            } else if (path.endsWith("/get_vehicle_information")) {
                respond(exchange, 200, "{\"available\":true}");
            } else if (path.endsWith("/register_vehicle_purchase") && failPurchase) {
                respond(exchange, 500, "purchase service failed");
            } else if (path.endsWith("/register_vehicle_purchase")) {
                respond(exchange, 200, "{\"purchase_order_id\":\"purchase-123\"}");
            } else if (path.endsWith("/set_payment_method")) {
                respond(exchange, 200, "{\"status\":\"PAYMENT_CONFIGURED\"}");
            } else if (path.endsWith("/purchase-status")) {
                respond(exchange, 200, "{\"status\":\"EMAIL_QUEUED\"}");
            } else {
                respond(exchange, 404, "not found");
            }
        });
        server.start();
        return server;
    }

    private List<String> extractToolNames(String chatRequest) throws IOException {
        JsonNode tools = new ObjectMapper().readTree(chatRequest).path("tools");
        List<String> names = new java.util.ArrayList<>();
        tools.forEach(tool -> names.add(tool.path("function").path("name").asText()));
        return names;
    }

    private String toolCallResponse(String name) {
        String arguments = switch (name) {
            case "get_vehicle_information" -> "{\"model_id\":\"vehicle-123\",\"agency_id\":\"agency-01\"}";
            case "register_vehicle_purchase" -> "{\"customer_id\":\"customer-001\",\"vehicle_id\":\"vehicle-123\",\"sales_agent_id\":\"agent-77\"}";
            case "set_payment_method" -> "{\"purchase_order_id\":\"purchase-123\",\"payment_type\":\"FINANCING\",\"financial_institution\":\"BBVA\",\"down_payment_amount\":15000}";
            case "send_purchase_status_email" -> "{\"recipientEmail\":\"customer@example.com\",\"templateId\":\"ORDER_CREATED\",\"data\":{\"customer\":\"customer-001\",\"vehicle\":\"vehicle-123\",\"order\":\"purchase-123\"}}";
            default -> "{}";
        };
        return "{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{" +
                "\"function\":{\"name\":\"" + name + "\",\"arguments\":" + arguments + "}}]}}";
    }

    private String malformedToolCallResponse() {
        return "{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{" +
                "\"function\":{\"name\":\"get_vehicle_information\",\"arguments\":{" +
                "\"type\":\"object\",\"properties\":{\"model_id\":{\"type\":\"string\"}}}}}]}}";
    }

    private String baseUrl(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

            private McpToolClient fakeMcpToolClient(boolean failPurchase) {
            return new McpToolClient() {
                @Override
                public List<DiscoveredTool> listTools(String serverName) {
                if ("mcp-domain".equals(serverName)) {
                    return List.of(
                        discoveredTool("get_vehicle_information", Map.of(
                            "model_id", stringSchema(), "agency_id", stringSchema()), List.of("model_id", "agency_id")),
                        discoveredTool("register_vehicle_purchase", Map.of(
                            "customer_id", stringSchema(), "vehicle_id", stringSchema(),
                            "sales_agent_id", stringSchema()), List.of("customer_id", "vehicle_id", "sales_agent_id")),
                        discoveredTool("set_payment_method", Map.of(
                            "purchase_order_id", stringSchema(), "payment_type", stringSchema(),
                            "financial_institution", stringSchema(), "down_payment_amount", Map.of("type", "number")),
                            List.of("purchase_order_id", "payment_type", "financial_institution", "down_payment_amount")));
                }
                return List.of(discoveredTool("send_purchase_status_email", Map.of(
                    "recipientEmail", stringSchema(), "templateId", stringSchema(),
                    "data", Map.of("type", "object")), List.of("recipientEmail", "templateId", "data")));
                }

                @Override
                public Map<String, Object> callTool(String serverName, String toolName, Map<String, Object> arguments) {
                if (failPurchase && "register_vehicle_purchase".equals(toolName)) {
                    throw new IllegalStateException("purchase service failed");
                }
                return switch (toolName) {
                    case "get_vehicle_information" -> Map.of("available", true);
                    case "register_vehicle_purchase" -> Map.of("purchase_order_id", "purchase-123");
                    case "set_payment_method" -> Map.of("status", "PAYMENT_CONFIGURED");
                    case "send_purchase_status_email" -> Map.of("status", "SENT");
                    default -> throw new IllegalArgumentException("Unexpected tool: " + toolName);
                };
                }
            };
        }

        private DiscoveredTool discoveredTool(String name, Map<String, Object> properties, List<String> required) {
        return new DiscoveredTool(name, name, Map.of(
            "type", "object",
            "properties", properties,
            "required", required));
        }

        private Map<String, Object> stringSchema() {
        return Map.of("type", "string");
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] response = body.getBytes(StandardCharsets.UTF_8);
        if (status != 204) {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
        }
        exchange.sendResponseHeaders(status, status == 204 ? -1 : response.length);
        if (status != 204) {
            exchange.getResponseBody().write(response);
        }
        exchange.close();
    }
}
