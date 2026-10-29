package com.floworchestrator.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.floworchestrator.orchestrator.config.FlowDefinitions;
import com.floworchestrator.orchestrator.model.FlowAsyncAccepted;
import com.floworchestrator.orchestrator.model.FlowExecutionEvent;
import com.floworchestrator.orchestrator.model.FlowRequest;
import com.floworchestrator.orchestrator.model.FlowResponse;
import com.floworchestrator.orchestrator.service.McpToolClient.DiscoveredTool;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.ToolResponseMessage;
import org.springframework.ai.chat.metadata.ChatResponseMetadata;
import org.springframework.ai.chat.metadata.DefaultUsage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.model.tool.ToolCallingChatOptions;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlowServiceTest {

    private FlowDefinitions flowDefinitions;
    private ChatModel chatModel;
    private FlowEventPublisher publisher;
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private RetryRegistry retryRegistry;
    private HttpServer controlCenter;

    @BeforeEach
    void setUp() throws IOException {
        flowDefinitions = buildFlowDefinitions();
        chatModel = mock(ChatModel.class);
        publisher = new FlowEventPublisher();
        circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
        retryRegistry = RetryRegistry.of(RetryConfig.custom()
                .maxAttempts(1)
                .waitDuration(Duration.ZERO)
                .build());
        controlCenter = startControlCenter();
    }

    @AfterEach
    void tearDown() {
        if (controlCenter != null) {
            controlCenter.stop(0);
        }
    }

    @Test
    @DisplayName("debe completar el flujo ejecutando las tools MCP sin un turno final del LLM")
    void shouldCompleteFlowAndPublishEvents() throws InterruptedException {
        when(chatModel.call(any(Prompt.class))).thenReturn(
                toolCallResponse("call-1", "get_vehicle_information"),
                toolCallResponse("call-2", "register_vehicle_purchase"),
                toolCallResponse("call-3", "set_payment_method"),
                toolCallResponse("call-4", "send_purchase_status_email"));

        List<FlowExecutionEvent> events = new CopyOnWriteArrayList<>();
        CountDownLatch completed = new CountDownLatch(1);
        publisher.stream().subscribe(event -> {
            events.add(event);
            if ("FLOW_COMPLETED".equals(event.eventType())) {
                completed.countDown();
            }
        });

        FlowService service = buildService(fakeMcpToolClient(false), 8);
        FlowResponse response = service.createFlow(request());

        assertTrue(completed.await(5, TimeUnit.SECONDS), "Debe publicarse FLOW_COMPLETED");
        assertEquals("COMPLETED", response.getStatus());
        assertEquals("execution-123", response.getId());
        assertNotNull(response.getTraceId());
        assertTrue(response.getMessage().contains("se completó correctamente"),
                "El cierre debe usar el resumen determinista");
        assertEquals("customer-001", response.getAttributes().get("customerId"));

        List<String> eventTypes = events.stream().map(FlowExecutionEvent::eventType).toList();
        assertEquals(4, eventTypes.stream().filter("STEP_STARTED"::equals).count());
        assertEquals(4, eventTypes.stream().filter("STEP_COMPLETED"::equals).count());

        ArgumentCaptor<Prompt> promptCaptor = ArgumentCaptor.forClass(Prompt.class);
        verify(chatModel, times(4)).call(promptCaptor.capture());

        ToolCallingChatOptions firstOptions = assertInstanceOf(
                ToolCallingChatOptions.class, promptCaptor.getAllValues().getFirst().getOptions());
        assertEquals(1, firstOptions.getToolCallbacks().size());
        assertEquals("get_vehicle_information",
                firstOptions.getToolCallbacks().getFirst().getToolDefinition().name());

        List<ToolResponseMessage> toolResponses = promptCaptor.getAllValues().getLast().getInstructions()
                .stream()
                .filter(ToolResponseMessage.class::isInstance)
                .map(ToolResponseMessage.class::cast)
                .toList();
        assertEquals(3, toolResponses.size(),
                "El último turno recibe los resultados previos; el cuarto paso ya no se reenvía al LLM");
        assertEquals("call-1", toolResponses.getFirst().getResponses().getFirst().id());
    }

    @Test
    @DisplayName("no solicita un turno adicional al LLM cuando ya están todos los pasos obligatorios")
    void shouldNotRequestExtraLlmTurnWhenAllToolsCompleted() throws InterruptedException {
        when(chatModel.call(any(Prompt.class))).thenReturn(
                toolCallResponse("call-1", "get_vehicle_information"),
                toolCallResponse("call-2", "register_vehicle_purchase"),
                toolCallResponse("call-3", "set_payment_method"),
                toolCallResponse("call-4", "send_purchase_status_email"));

        List<FlowExecutionEvent> events = new CopyOnWriteArrayList<>();
        CountDownLatch completed = new CountDownLatch(1);
        publisher.stream().subscribe(event -> {
            events.add(event);
            if ("FLOW_COMPLETED".equals(event.eventType())) {
                completed.countDown();
            }
        });

        FlowService service = buildService(fakeMcpToolClient(false), 8);
        FlowResponse response = service.createFlow(request());

        assertTrue(completed.await(5, TimeUnit.SECONDS),
                "El flujo debe completarse en cuanto terminan los pasos obligatorios");
        assertEquals("COMPLETED", response.getStatus());
        assertTrue(response.getMessage().contains("se completó correctamente"));
        verify(chatModel, times(4)).call(any(Prompt.class));
        assertTrue(events.stream().noneMatch(event -> "FLOW_FAILED".equals(event.eventType())),
                "No debe publicarse FLOW_FAILED si los pasos de negocio ya terminaron");
    }

    @Test
    @DisplayName("debe aceptar el flujo en segundo plano y publicar eventos con el mismo traceId")
    void shouldStartFlowAsynchronously() throws InterruptedException {
        when(chatModel.call(any(Prompt.class))).thenReturn(
                toolCallResponse("call-1", "get_vehicle_information"),
                toolCallResponse("call-2", "register_vehicle_purchase"),
                toolCallResponse("call-3", "set_payment_method"),
                toolCallResponse("call-4", "send_purchase_status_email"));

        List<FlowExecutionEvent> events = new CopyOnWriteArrayList<>();
        CountDownLatch completed = new CountDownLatch(1);
        publisher.stream().subscribe(event -> {
            events.add(event);
            if ("FLOW_COMPLETED".equals(event.eventType())) {
                completed.countDown();
            }
        });

        FlowService service = buildService(fakeMcpToolClient(false), 8);
        FlowRequest request = request();
        request.setExecutionId("execution-async");

        FlowAsyncAccepted accepted = service.startFlowAsync(request);

        assertEquals("ACCEPTED", accepted.status());
        assertNotNull(accepted.requestId());
        assertNotNull(accepted.traceId());
        assertEquals("execution-async", accepted.executionId());

        assertTrue(completed.await(5, TimeUnit.SECONDS), "El flujo debe completarse en segundo plano");
        FlowExecutionEvent completedEvent = events.stream()
                .filter(event -> "FLOW_COMPLETED".equals(event.eventType()))
                .findFirst()
                .orElseThrow();
        assertEquals(accepted.traceId(), completedEvent.traceId(),
                "Los eventos SSE deben correlacionarse con el traceId devuelto");
    }

    @Test
    @DisplayName("debe publicar STEP_FAILED y terminar en FAILED cuando una tool MCP falla")
    void shouldPublishStepFailedWhenDomainServiceFails() throws InterruptedException {
        when(chatModel.call(any(Prompt.class)))
                .thenReturn(toolCallResponse("call-1", "get_vehicle_information"));

        List<FlowExecutionEvent> events = new CopyOnWriteArrayList<>();
        CountDownLatch failed = new CountDownLatch(1);
        publisher.stream().subscribe(event -> {
            events.add(event);
            if ("FLOW_FAILED".equals(event.eventType())) {
                failed.countDown();
            }
        });

        FlowService service = buildService(fakeMcpToolClient(true), 3);
        FlowResponse response = service.createFlow(request());

        assertTrue(failed.await(5, TimeUnit.SECONDS), "Debe publicarse FLOW_FAILED");
        assertEquals("FAILED", response.getStatus());
        assertEquals("ERR_FLOW_01", response.getFlowCode());
        assertNotNull(response.getUserErrorTitle());

        FlowExecutionEvent stepFailed = events.stream()
                .filter(event -> "STEP_FAILED".equals(event.eventType()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Debe publicarse STEP_FAILED"));
        assertEquals("get_vehicle_information", stepFailed.toolName());
        assertNotNull(stepFailed.errorMessage());
        assertTrue(stepFailed.errorMessage().contains("MCP service unavailable"),
                "STEP_FAILED debe conservar el error de la tool MCP");
    }

    @Test
    @DisplayName("debe degradar de forma elegante cuando el LLM no responde")
    void shouldDegradeGracefullyWhenLlmIsUnavailable() throws InterruptedException {
        when(chatModel.call(any(Prompt.class)))
                .thenThrow(new IllegalStateException("connection refused"));

        List<FlowExecutionEvent> events = new CopyOnWriteArrayList<>();
        CountDownLatch failed = new CountDownLatch(1);
        publisher.stream().subscribe(event -> {
            events.add(event);
            if ("FLOW_FAILED".equals(event.eventType())) {
                failed.countDown();
            }
        });

        FlowService service = buildService(fakeMcpToolClient(false), 3);
        FlowResponse response = service.createFlow(request());

        assertTrue(failed.await(5, TimeUnit.SECONDS), "Debe publicarse FLOW_FAILED");
        assertEquals("FAILED", response.getStatus());
        assertEquals(AiService.FLOW_CODE_LLM_ERROR, response.getFlowCode());
        assertTrue(response.getUserErrorMessage().contains("IA"));
        assertEquals("warning", response.getUserErrorStatusType());

        FlowExecutionEvent failedEvent = events.stream()
                .filter(event -> "FLOW_FAILED".equals(event.eventType()))
                .findFirst()
                .orElseThrow();
        assertEquals(AiService.FLOW_CODE_LLM_ERROR, failedEvent.flowCode());
        assertFalse(failedEvent.userErrorMessage().isBlank());
    }

    private FlowRequest request() {
        FlowRequest request = new FlowRequest();
        request.setFlowId("compra-vehiculo");
        request.getAttributes().put("customerId", "customer-001");
        request.getAttributes().put("vehicleId", "vehicle-123");
        request.getAttributes().put("salesAgentId", "agent-77");
        return request;
    }

    private FlowService buildService(McpToolClient registry, int maxAgentTurns) {
        FlowServiceConfig config = new FlowServiceConfig(
                baseUrl(controlCenter),
                "ollama",
                "qwen2.5:7b",
                maxAgentTurns,
                0,
                Duration.ofSeconds(5),
                5,
                Duration.ofSeconds(5),
                20,
                Duration.ofSeconds(2),
                1,
                Duration.ZERO,
                50,
                Duration.ofSeconds(30),
                Duration.ofSeconds(5)
        );
        AiService aiService = new AiService(chatModel, config, new ObjectMapper(),
                new PiiMasker(), circuitBreakerRegistry, retryRegistry);
        FlowStateManager stateManager = new FlowStateManager();
        return new FlowService(
                WebClient.builder().build(),
                aiService,
                config,
                new ObjectMapper(),
                flowDefinitions,
                registry,
                publisher,
                stateManager,
                new FlowInterruptionHandler(publisher, stateManager, new UserFacingErrorMapper()),
                new UserFacingErrorMapper(),
                new PiiMasker(),
                circuitBreakerRegistry,
                retryRegistry
        );
    }

    private FlowDefinitions buildFlowDefinitions() {
        FlowDefinitions definitions = new FlowDefinitions();
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("compra-vehiculo");
        flow.setSystemPromptTemplate(
                "inline:Eres un asistente especializado en <flowName>. Flujo: <flowId>.");
        flow.setUserPromptTemplate(
                "inline:Procesa el flujo <flowName> para el cliente <customerId>. "
                        + "Vehículo: <vehicleId>");

        FlowDefinitions.ToolDefinition t1 = tool("get_vehicle_information", 1,
                "Consultar vehículo", List.of());
        FlowDefinitions.ToolDefinition t2 = tool("register_vehicle_purchase", 2,
                "Registrar compra", List.of("get_vehicle_information"));
        FlowDefinitions.ToolDefinition t3 = tool("set_payment_method", 3,
                "Configurar pago", List.of("register_vehicle_purchase"));
        FlowDefinitions.ToolDefinition t4 = tool("send_purchase_status_email", 4,
                "Enviar email", List.of("set_payment_method"));

        flow.setTools(List.of(t1, t2, t3, t4));
        definitions.getFlows().put("compra-vehiculo", flow);
        definitions.getMcpServers().put("mcp-domain", "http://localhost:9001");
        return definitions;
    }

    private FlowDefinitions.ToolDefinition tool(String name, int order, String stepName,
                                                List<String> dependsOn) {
        FlowDefinitions.ToolDefinition tool = new FlowDefinitions.ToolDefinition();
        tool.setName(name);
        tool.setServer("mcp-domain");
        tool.setStepOrder(order);
        tool.setStepName(stepName);
        tool.setRequired(true);
        tool.setDependsOn(dependsOn);
        return tool;
    }

    private HttpServer startControlCenter() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/process-templates/by-name", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            String name = query != null && query.contains("name=") ? query.split("name=")[1] : "";
            sendResponse(exchange, 200, "{\"id\": \"" + name + "-template-id\"}");
        });
        server.createContext("/api/processes", exchange ->
                sendResponse(exchange, 200, "{\"id\": \"execution-123\"}"));
        server.start();
        return server;
    }

    private void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] response = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }

    private McpToolClient fakeMcpToolClient(boolean failTools) {
        return new McpToolClient() {
            @Override
            public List<DiscoveredTool> listTools(String serverName) {
                return List.of(
                        discovered("get_vehicle_information"),
                        discovered("register_vehicle_purchase"),
                        discovered("set_payment_method"),
                        discovered("send_purchase_status_email"));
            }

            @Override
            public Map<String, Object> callTool(String serverName, String toolName,
                                                Map<String, Object> arguments) {
                if (failTools) {
                    throw new RuntimeException("MCP service unavailable");
                }
                return Map.of("result", "ok", "tool", toolName);
            }
        };
    }

    private DiscoveredTool discovered(String name) {
        return new DiscoveredTool(name, "desc",
                Map.of("type", "object", "properties", Map.of(), "required", List.of()));
    }

    private ChatResponse toolCallResponse(String callId, String toolName) {
        return chatResponse("", List.of(
                new AssistantMessage.ToolCall(callId, "function", toolName, "{}")));
    }

    private ChatResponse chatResponse(String content, List<AssistantMessage.ToolCall> toolCalls) {
        AssistantMessage message = new AssistantMessage(content, Map.of(), toolCalls);
        ChatResponseMetadata metadata = ChatResponseMetadata.builder()
                .model("qwen2.5:7b")
                .usage(new DefaultUsage(10, 5))
                .build();
        return new ChatResponse(List.of(new Generation(message)), metadata);
    }

    private String baseUrl(HttpServer server) {
        InetSocketAddress address = server.getAddress();
        return "http://" + address.getHostName() + ":" + address.getPort();
    }
}
