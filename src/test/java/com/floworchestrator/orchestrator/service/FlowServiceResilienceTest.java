package com.floworchestrator.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.floworchestrator.orchestrator.config.FlowDefinitions;
import com.floworchestrator.orchestrator.model.FlowResponse;
import com.floworchestrator.orchestrator.service.McpToolClient.DiscoveredTool;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.web.reactive.function.client.WebClient;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

/**
 * Pruebas de resiliencia para FlowService:
 * - Reintentos de herramientas MCP ante fallos transitorios
 * - Circuit breaker por servidor MCP
 * - Truncamiento de ventana de contexto
 * - Detección de solicitud de entrada de usuario
 * - Inyección de input de usuario al reanudar
 * - Reintento de paso individual
 * - Manejo unificado de excepciones del agente
 */
class FlowServiceResilienceTest {

    private FlowService service;
    private FlowEventPublisher publisher;
    private AtomicInteger mcpCallCount;
    private boolean mcpShouldFail;
    private RuntimeException mcpFailure;
    private FlowDefinitions flowDefinitions;
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private RetryRegistry retryRegistry;
    private ChatModel chatModel;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PiiMasker piiMasker = new PiiMasker();

    @BeforeEach
    void setUp() {
        publisher = new FlowEventPublisher();
        mcpCallCount = new AtomicInteger(0);
        mcpShouldFail = false;
        mcpFailure = null;
        chatModel = mock(ChatModel.class);
        circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
        retryRegistry = RetryRegistry.of(RetryConfig.custom()
                .maxAttempts(1)
                .waitDuration(Duration.ZERO)
                .build());

        flowDefinitions = new FlowDefinitions();
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("compra-vehiculo");
        flow.setSystemPromptTemplate("classpath:prompts/system.txt");
        flow.setUserPromptTemplate("classpath:prompts/user.txt");

        FlowDefinitions.ToolDefinition t1 = tool("get_vehicle_information", 1,
                "Consultar vehículo", List.of());
        FlowDefinitions.ToolDefinition t2 = tool("register_vehicle_purchase", 2,
                "Registrar compra", List.of("get_vehicle_information"));
        FlowDefinitions.ToolDefinition t3 = tool("set_payment_method", 3,
                "Configurar pago", List.of("register_vehicle_purchase"));
        FlowDefinitions.ToolDefinition t4 = tool("send_purchase_status_email", 4,
                "Enviar email", List.of("set_payment_method"));

        flow.setTools(List.of(t1, t2, t3, t4));
        flowDefinitions.getFlows().put("compra-vehiculo", flow);
        flowDefinitions.getMcpServers().put("mcp-domain", "http://localhost:9001");

        service = buildService(config(2));
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

    private FlowServiceConfig config(int maxMcpRetries) {
        return new FlowServiceConfig(
                "http://localhost:8081",
                "http://localhost:11434",
                "qwen2.5:7b",
                3,
                maxMcpRetries,
                Duration.ofSeconds(5),
                5,
                Duration.ofSeconds(10),
                20,
                Duration.ofSeconds(1),
                1,
                Duration.ZERO,
                50,
                Duration.ofSeconds(30),
                Duration.ofSeconds(5)
        );
    }

    private FlowService buildService(FlowServiceConfig config) {
        McpToolClient registry = new McpToolClient() {
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
                int callNum = mcpCallCount.incrementAndGet();
                if (mcpFailure != null) {
                    throw mcpFailure;
                }
                if (mcpShouldFail) {
                    throw new RuntimeException("MCP service unavailable");
                }
                return Map.of("result", "ok", "call", callNum, "tool", toolName);
            }
        };
        AiService aiService = new AiService(chatModel, config, objectMapper,
                piiMasker, circuitBreakerRegistry, retryRegistry);
        return new FlowService(
                WebClient.builder().build(),
                aiService,
                config,
                objectMapper,
                flowDefinitions,
                registry,
                publisher,
                new FlowStateManager(),
                new UserFacingErrorMapper(),
                piiMasker,
                circuitBreakerRegistry,
                retryRegistry
        );
    }

    private DiscoveredTool discovered(String name) {
        return new DiscoveredTool(name, "desc",
                Map.of("type", "object", "properties", Map.of(), "required", List.of()));
    }

    @Test
    @DisplayName("debe reintentar herramienta MCP ante fallos transitorios")
    @Timeout(15)
    void shouldRetryMcpToolOnTransientFailure() throws Exception {
        mcpShouldFail = true;

        Method method = FlowService.class.getDeclaredMethod("executeToolWithRetry",
                FlowDefinitions.ToolDefinition.class, Map.class);
        method.setAccessible(true);

        FlowDefinitions.ToolDefinition tool = tool("test_tool", 1, "Test", List.of());

        try {
            method.invoke(service, tool, Map.of());
            fail("Debe lanzar excepción tras agotar reintentos");
        } catch (InvocationTargetException e) {
            assertInstanceOf(RuntimeException.class, e.getCause());
        }

        assertEquals(3, mcpCallCount.get(), "Debe llamar 3 veces (inicial + 2 reintentos)");
    }

    @Test
    @DisplayName("no debe repetir un error de herramienta no reintentable")
    @Timeout(15)
    void shouldNotRetryNonRetryableToolError() throws Exception {
        mcpFailure = new McpToolException("Falló la herramienta MCP: ALREADY_EXISTS",
                "ALREADY_EXISTS", false);

        Method method = FlowService.class.getDeclaredMethod("executeToolWithRetry",
                FlowDefinitions.ToolDefinition.class, Map.class);
        method.setAccessible(true);

        FlowDefinitions.ToolDefinition tool = tool("register_vehicle_purchase", 2, "Registrar compra", List.of());

        try {
            method.invoke(service, tool, Map.of());
            fail("Debe propagar el error de negocio");
        } catch (InvocationTargetException e) {
            assertInstanceOf(McpToolException.class, e.getCause());
            assertFalse(((McpToolException) e.getCause()).isRetryable());
        }

        assertEquals(1, mcpCallCount.get(),
                "Un error de negocio no reintentable debe llamar una sola vez");
    }

    @Test
    @DisplayName("debe reintentar un error de herramienta marcado como reintentable")
    @Timeout(20)
    void shouldRetryRetryableToolError() throws Exception {
        mcpFailure = new McpToolException("Falló la herramienta MCP: DEPENDENCY_UNAVAILABLE",
                "DEPENDENCY_UNAVAILABLE", true);

        Method method = FlowService.class.getDeclaredMethod("executeToolWithRetry",
                FlowDefinitions.ToolDefinition.class, Map.class);
        method.setAccessible(true);

        FlowDefinitions.ToolDefinition tool = tool("register_vehicle_purchase", 2, "Registrar compra", List.of());

        try {
            method.invoke(service, tool, Map.of());
            fail("Debe lanzar excepción tras agotar reintentos");
        } catch (InvocationTargetException e) {
            assertInstanceOf(McpToolException.class, e.getCause());
        }

        assertEquals(3, mcpCallCount.get(), "Debe reintentar hasta maxMcpRetries + 1");
    }

    @Test
    @DisplayName("un ALREADY_EXISTS marca el paso cumplido y desbloquea los dependientes")
    void alreadyExistsMarksStepCompletedAndUnlocksDependents() throws Exception {
        mcpFailure = new McpToolException("Falló la herramienta MCP: ALREADY_EXISTS",
                "ALREADY_EXISTS", false,
                Map.of("error_code", "ALREADY_EXISTS", "purchase_order_id", "po-1"));

        FlowService.AgentLoopState state = new FlowService.AgentLoopState();
        setCurrentStepOrder(state, 2);
        setCurrentStep(state, "registrar_compra");
        FlowDefinitions.FlowDefinition flow = flowDefinitions.getFlows().get("compra-vehiculo");
        FlowDefinitions.ToolDefinition register = flow.getTools().stream()
                .filter(candidate -> "register_vehicle_purchase".equals(candidate.getName()))
                .findFirst().orElseThrow();

        Method execute = FlowService.class.getDeclaredMethod("executeToolStep",
                String.class, String.class, List.class, FlowService.AgentLoopState.class,
                FlowDefinitions.ToolDefinition.class, AiService.AiToolCall.class);
        execute.setAccessible(true);

        boolean success = (boolean) execute.invoke(service, "compra-vehiculo", "exec-1",
                new ArrayList<Map<String, Object>>(), state, register,
                new AiService.AiToolCall("call-1", "register_vehicle_purchase",
                        Map.of("customer_id", "c-1", "vehicle_id", "v-1", "sales_agent_id", "a-1")));

        assertTrue(success, "El resultado ya existente se trata como paso cumplido");
        assertTrue(getCompletedTools(state).contains("register_vehicle_purchase"));
        assertFalse(getFailedTools(state).contains("register_vehicle_purchase"));
        assertEquals("po-1", getToolResults(state).get("register_vehicle_purchase").get("purchase_order_id"));

        Method specs = FlowService.class.getDeclaredMethod("getToolSpecs",
                FlowDefinitions.FlowDefinition.class, FlowService.AgentLoopState.class);
        specs.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<AiService.ToolSpec> offered = (List<AiService.ToolSpec>) specs.invoke(service, flow, state);

        assertTrue(offered.stream().anyMatch(spec -> "set_payment_method".equals(spec.name())),
                "El siguiente paso debe ofrecerse al modelo tras cumplirse su dependencia");
    }

    @Test
    @DisplayName("si el agente termina con pasos pendientes, el flujo se pausa")
    void earlyStopWithPendingStepsPausesForHumanInput() throws Exception {
        FlowService.AgentLoopState state = new FlowService.AgentLoopState();
        setCompletedTools(state, List.of("get_vehicle_information"));
        FlowDefinitions.FlowDefinition flow = flowDefinitions.getFlows().get("compra-vehiculo");

        Method complete = FlowService.class.getDeclaredMethod("completeFlow",
                FlowDefinitions.FlowDefinition.class, String.class, String.class,
                FlowResponse.class, String.class, Set.class);
        complete.setAccessible(true);

        try {
            complete.invoke(service, flow, "compra-vehiculo", "exec-1",
                    new FlowResponse(), "listo", new java.util.HashSet<>(getCompletedTools(state)));
            fail("Debe pausar el flujo en lugar de fallarlo");
        } catch (InvocationTargetException e) {
            assertInstanceOf(RequiresHumanInterventionException.class, e.getCause());
            assertTrue(e.getCause().getMessage().contains("set_payment_method"),
                    "El mensaje debe indicar qué pasos quedaron pendientes");
        }
    }

    @Test
    @DisplayName("el error de argumento inválido debe listar los argumentos permitidos")
    void invalidArgumentErrorShouldListAllowedArguments() throws Exception {
        Method method = FlowService.class.getDeclaredMethod("validateArguments",
                DiscoveredTool.class, Map.class);
        method.setAccessible(true);

        DiscoveredTool tool = new DiscoveredTool("send_purchase_status_email", "desc",
                Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "recipientEmail", Map.of("type", "string"),
                                "templateId", Map.of("type", "string")),
                        "required", List.of("recipientEmail")));

        try {
            method.invoke(service, tool, Map.of("purchase_order_id", "po-1"));
            fail("Debe rechazar argumentos fuera del esquema");
        } catch (InvocationTargetException e) {
            assertInstanceOf(IllegalArgumentException.class, e.getCause());
            String message = e.getCause().getMessage();
            assertTrue(message.contains("purchase_order_id"));
            assertTrue(message.contains("Allowed arguments:"));
            assertTrue(message.contains("recipientEmail"));
            assertTrue(message.contains("templateId"),
                    "El modelo necesita saber qué argumentos sí puede enviar");
        }
    }

    @Test
    @DisplayName("debe abrir circuit breaker tras umbral de fallos")
    void shouldOpenCircuitBreakerAfterThreshold() throws Exception {
        mcpShouldFail = true;
        FlowService circuitBreakerService = buildService(config(0));

        Method method = FlowService.class.getDeclaredMethod("executeToolWithRetry",
                FlowDefinitions.ToolDefinition.class, Map.class);
        method.setAccessible(true);

        FlowDefinitions.ToolDefinition tool = new FlowDefinitions.ToolDefinition();
        tool.setName("test_tool");
        tool.setServer("mcp-failing-server");
        tool.setStepOrder(1);
        tool.setStepName("Test");

        for (int i = 0; i < 5; i++) {
            try {
                method.invoke(circuitBreakerService, tool, Map.of());
                fail("Fallo " + (i + 1) + " debe lanzar excepción");
            } catch (InvocationTargetException e) {
                assertInstanceOf(RuntimeException.class, e.getCause());
            }
        }

        try {
            method.invoke(circuitBreakerService, tool, Map.of());
            fail("Circuito abierto debe lanzar excepción inmediatamente");
        } catch (InvocationTargetException e) {
            assertTrue(e.getCause().getMessage().contains("OPEN"),
                    "Debe indicar que el circuito está abierto");
        }
    }

    @Test
    @DisplayName("debe truncar ventana de contexto manteniendo mensaje del sistema")
    void shouldTrimContextWindowKeepingSystemMessage() throws Exception {
        Method trimMethod = FlowService.class.getDeclaredMethod("trimContextIfNeeded", List.class);
        trimMethod.setAccessible(true);

        List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", "Eres un asistente"));
        for (int i = 0; i < 25; i++) {
            messages.add(Map.of("role", "user", "content", "mensaje " + i));
            messages.add(Map.of("role", "assistant", "content", "respuesta " + i));
        }

        trimMethod.invoke(service, messages);

        assertEquals(20, messages.size(), "Debe mantener exactamente 20 mensajes");
        assertEquals("system", messages.get(0).get("role"),
                "El primer mensaje debe ser el del sistema");
        assertEquals("Eres un asistente", messages.get(0).get("content"));
    }

    @Test
    @DisplayName("no debe dejar respuestas de tool huérfanas al truncar contexto")
    void shouldNotKeepOrphanToolMessagesWhenTrimming() throws Exception {
        Method trimMethod = FlowService.class.getDeclaredMethod("trimContextIfNeeded", List.class);
        trimMethod.setAccessible(true);

        List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", "system"));
        for (int i = 0; i < 12; i++) {
            messages.add(Map.of("role", "user", "content", "mensaje " + i));
            messages.add(Map.of("role", "assistant", "content", "respuesta " + i));
        }
        messages.add(Map.of("role", "assistant", "content", ""));
        messages.add(Map.of("role", "tool", "tool_name", "tool", "tool_call_id", "1", "content", "{}"));
        messages.add(Map.of("role", "tool", "tool_name", "tool", "tool_call_id", "2", "content", "{}"));

        trimMethod.invoke(service, messages);

        assertEquals("system", messages.get(0).get("role"));
        assertFalse("tool".equals(messages.get(1).get("role")),
                "No debe iniciar el contexto truncado con mensajes de herramienta huérfanos");
    }

    @Test
    @DisplayName("no debe truncar si hay menos mensajes que el límite")
    void shouldNotTrimWhenUnderLimit() throws Exception {
        Method trimMethod = FlowService.class.getDeclaredMethod("trimContextIfNeeded", List.class);
        trimMethod.setAccessible(true);

        List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", "system"));
        messages.add(Map.of("role", "user", "content", "hola"));

        int originalSize = messages.size();
        trimMethod.invoke(service, messages);

        assertEquals(originalSize, messages.size(),
                "No debe truncar si está por debajo del límite");
    }

    @Test
    @DisplayName("debe detectar solicitud de entrada de usuario en español")
    void shouldDetectUserInputInSpanish() throws Exception {
        Method method = FlowService.class.getDeclaredMethod("isRequestingUserInput", String.class);
        method.setAccessible(true);

        assertTrue((Boolean) method.invoke(service,
                "Para continuar con el flujo de compra de vehículo, necesitamos conocer el modelo"));
        assertTrue((Boolean) method.invoke(service,
                "Por favor, proporcione el modelo del vehículo (model_id)"));
        assertTrue((Boolean) method.invoke(service,
                "Necesitamos saber el número de serie del vehículo"));
    }

    @Test
    @DisplayName("debe detectar solicitud de entrada de usuario en inglés")
    void shouldDetectUserInputInEnglish() throws Exception {
        Method method = FlowService.class.getDeclaredMethod("isRequestingUserInput", String.class);
        method.setAccessible(true);

        assertTrue((Boolean) method.invoke(service,
                "Please provide the vehicle model to continue"));
        assertTrue((Boolean) method.invoke(service,
                "I need to know the customer's email address"));
        assertTrue((Boolean) method.invoke(service, "Input needed for the payment method"));
    }

    @Test
    @DisplayName("no debe detectar texto normal como solicitud de usuario")
    void shouldNotDetectNormalTextAsUserInput() throws Exception {
        Method method = FlowService.class.getDeclaredMethod("isRequestingUserInput", String.class);
        method.setAccessible(true);

        assertFalse((Boolean) method.invoke(service, "Compra completada exitosamente"));
        assertFalse((Boolean) method.invoke(service, "El vehículo está disponible"));
        assertFalse((Boolean) method.invoke(service, ""));
        assertFalse((Boolean) method.invoke(service, (String) null));
    }

    @Test
    @DisplayName("debe detectar solicitudes de usuario en contenido multilínea")
    void shouldDetectUserInputInMultilineContent() throws Exception {
        Method method = FlowService.class.getDeclaredMethod("isRequestingUserInput", String.class);
        method.setAccessible(true);

        assertTrue((Boolean) method.invoke(service,
                "Para continuar necesito lo siguiente:\n- Por favor proporcione el RFC del cliente"));
    }

    @Test
    @DisplayName("debe inyectar input de usuario al reanudar flujo pausado")
    void shouldInjectUserInputOnResume() throws Exception {
        FlowStateManager.PausedFlowState pausedState = createPausedState();

        FlowStateManager stateManager = new FlowStateManager();
        stateManager.store("exec-resume-test", pausedState);

        Field smField = FlowService.class.getDeclaredField("flowStateManager");
        smField.setAccessible(true);
        smField.set(service, stateManager);

        FlowResponse response = service.resumeFlow("exec-resume-test",
                "El modelo del vehículo es VW-Golf-2024");

        assertNotNull(response);
        assertEquals("exec-resume-test", response.getId());
        assertNotNull(response.getTraceId());
    }

    @Test
    @DisplayName("debe lanzar excepción al reanudar ejecución no pausada")
    void shouldThrowWhenResumingNonPausedExecution() {
        assertThrows(IllegalStateException.class,
                () -> service.resumeFlow("nonexistent-exec", "algún input"),
                "Debe lanzar IllegalStateException para ejecución no encontrada");
    }

    @Test
    @DisplayName("debe rechazar la reanudación asíncrona sin estado pausado")
    void shouldRejectAsyncResumeWithoutPausedState() {
        assertThrows(IllegalStateException.class,
                () -> service.resumeFlowAsync("nonexistent-exec", null),
                "Debe responder 404 antes de aceptar la reanudación");
    }

    @Test
    @DisplayName("debe marcar paso para reintento eliminándolo de completados")
    void shouldMarkStepForRetry() throws Exception {
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("compra-vehiculo");

        FlowDefinitions.ToolDefinition tool = tool("register_vehicle_purchase", 2,
                "Registrar compra", List.of());

        flow.setTools(List.of(tool));

        FlowService.AgentLoopState state = new FlowService.AgentLoopState();
        setCompletedTools(state, List.of("register_vehicle_purchase"));

        FlowStateManager stateManager = new FlowStateManager();
        stateManager.store("exec-retry-step",
                new FlowStateManager.PausedFlowState("compra-vehiculo", flow, List.of(), state));

        Field smField = FlowService.class.getDeclaredField("flowStateManager");
        smField.setAccessible(true);
        smField.set(service, stateManager);

        FlowResponse response = service.retryStep("exec-retry-step", 2);

        assertNotNull(response);
        assertEquals("RETRYING", response.getStatus());
        assertEquals("Registrar compra", response.getCurrentStep());
        assertNotNull(response.getTraceId());

        FlowStateManager.PausedFlowState retrieved = stateManager.retrieve("exec-retry-step");
        assertNotNull(retrieved);
        assertFalse(getCompletedTools(retrieved.agentState()).contains("register_vehicle_purchase"),
                "La herramienta debe eliminarse de completados para reintentarla");
    }

    @Test
    @DisplayName("debe permitir reintentar un paso que falló")
    void shouldAllowRetryingFailedStep() throws Exception {
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("compra-vehiculo");

        FlowDefinitions.ToolDefinition tool = tool("register_vehicle_purchase", 2,
                "Registrar compra", List.of());
        flow.setTools(List.of(tool));

        FlowService.AgentLoopState state = new FlowService.AgentLoopState();
        setFailedTools(state, List.of("register_vehicle_purchase"));

        FlowStateManager stateManager = new FlowStateManager();
        stateManager.store("exec-retry-failed",
                new FlowStateManager.PausedFlowState("compra-vehiculo", flow, List.of(), state));

        Field smField = FlowService.class.getDeclaredField("flowStateManager");
        smField.setAccessible(true);
        smField.set(service, stateManager);

        FlowResponse response = service.retryStep("exec-retry-failed", 2);

        assertEquals("RETRYING", response.getStatus());
        FlowStateManager.PausedFlowState retrieved = stateManager.retrieve("exec-retry-failed");
        assertNotNull(retrieved);
        assertFalse(getFailedTools(retrieved.agentState()).contains("register_vehicle_purchase"));
        assertTrue(retrieved.messages().getLast().get("content").toString().contains("reintentar"));
    }

    @Test
    @DisplayName("debe rechazar el reintento de un paso que nunca se ejecutó")
    void shouldRejectRetryOfStepNeverExecuted() throws Exception {
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("compra-vehiculo");
        flow.setTools(List.of(tool("set_payment_method", 3, "Configurar pago", List.of())));

        FlowStateManager stateManager = new FlowStateManager();
        stateManager.store("exec-retry-unknown",
                new FlowStateManager.PausedFlowState("compra-vehiculo", flow, List.of(),
                        new FlowService.AgentLoopState()));

        Field smField = FlowService.class.getDeclaredField("flowStateManager");
        smField.setAccessible(true);
        smField.set(service, stateManager);

        assertThrows(IllegalArgumentException.class,
                () -> service.retryStep("exec-retry-unknown", 3));
    }

    @Test
    @DisplayName("debe inyectar guía de reintento al reanudar tras un paso fallido")
    void shouldInjectRetryGuidanceOnResume() throws Exception {
        FlowDefinitions.FlowDefinition flow = flowDefinitions.getFlows().get("compra-vehiculo");

        FlowService.AgentLoopState state = new FlowService.AgentLoopState();
        setCompletedTools(state, List.of("get_vehicle_information"));
        setFailedTools(state, List.of("register_vehicle_purchase", "set_payment_method"));

        List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", "prompt"));

        Method method = FlowService.class.getDeclaredMethod("injectPendingRetryGuidance",
                FlowDefinitions.FlowDefinition.class, List.class,
                FlowService.AgentLoopState.class, String.class);
        method.setAccessible(true);
        method.invoke(service, flow, messages, state, "exec-guidance");

        assertEquals(2, messages.size());
        String guidance = messages.getLast().get("content").toString();
        assertTrue(guidance.contains("register_vehicle_purchase"));
        assertTrue(guidance.contains("set_payment_method"));
        assertFalse(guidance.contains("get_vehicle_information"),
                "Los pasos ya completados no deben pedirse de nuevo");
    }

    @Test
    @DisplayName("no debe inyectar guía si no hay pasos fallidos pendientes")
    void shouldNotInjectGuidanceWithoutPendingFailures() throws Exception {
        FlowDefinitions.FlowDefinition flow = flowDefinitions.getFlows().get("compra-vehiculo");

        FlowService.AgentLoopState state = new FlowService.AgentLoopState();
        setCompletedTools(state, List.of("get_vehicle_information"));
        setFailedTools(state, List.of("get_vehicle_information"));

        List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", "prompt"));

        Method method = FlowService.class.getDeclaredMethod("injectPendingRetryGuidance",
                FlowDefinitions.FlowDefinition.class, List.class,
                FlowService.AgentLoopState.class, String.class);
        method.setAccessible(true);
        method.invoke(service, flow, messages, state, "exec-guidance-none");

        assertEquals(1, messages.size());
    }

    @Test
    @DisplayName("debe manejar excepción de intervención humana en handleAgentLoopException")
    void shouldHandleInterventionException() throws Exception {
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("test");

        FlowService.AgentLoopState state = new FlowService.AgentLoopState();
        setCurrentStep(state, "paso_1");
        setCurrentStepOrder(state, 1);

        RequiresHumanInterventionException ex =
                new RequiresHumanInterventionException("Necesitamos el modelo del vehículo");

        FlowResponse response = invokeHandleAgentLoopException(
                "exec-intervention", "test-flow", flow,
                new ArrayList<>(), state, ex, false, Map.of(), "trace-1");

        assertEquals("PAUSED_FOR_HUMAN", response.getStatus());
        assertEquals("PAUSED_FOR_HUMAN", response.getFlowCode());
        assertEquals("paso_1", response.getCurrentStep());
        assertNotNull(response.getErrorMessage());
        assertTrue(response.getErrorMessage().contains("Necesitamos el modelo"));
        assertEquals("trace-1", response.getTraceId());
        assertNotNull(response.getUserErrorMessage());
    }

    @Test
    @DisplayName("debe manejar excepción de fallo en handleAgentLoopException")
    void shouldHandleFailureException() throws Exception {
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("test");
        flow.setTools(List.of(tool("paso_2", 2, "paso_2", List.of())));

        FlowService.AgentLoopState state = new FlowService.AgentLoopState();
        setCurrentStep(state, "paso_2");
        setCurrentStepOrder(state, 2);

        RuntimeException ex = new RuntimeException("MCP connection refused");

        FlowResponse response = invokeHandleAgentLoopException(
                "exec-failure", "test-flow", flow,
                new ArrayList<>(), state, ex, false, Map.of(), "trace-1");

        assertEquals("FAILED", response.getStatus());
        assertEquals("ERR_FLOW_01", response.getFlowCode());
        assertEquals("paso_2", response.getCurrentStep());
        assertEquals("MCP connection refused", response.getErrorMessage());
        assertNotNull(response.getUserErrorTitle());
    }

    @Test
    @DisplayName("debe degradar con flow_code cuando el LLM no está disponible")
    void shouldHandleLlmUnavailableException() throws Exception {
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("test");
        flow.setTools(List.of(tool("paso_3", 3, "paso_3", List.of())));

        FlowService.AgentLoopState state = new FlowService.AgentLoopState();
        setCurrentStep(state, "paso_3");
        setCurrentStepOrder(state, 3);

        LlmUnavailableException ex = new LlmUnavailableException("LLM timeout", "ERR_LLM_01",
                "La IA está procesando demasiadas solicitudes", "Reintentar");

        FlowResponse response = invokeHandleAgentLoopException(
                "exec-llm", "test-flow", flow, new ArrayList<>(), state, ex,
                false, Map.of("customerId", "c-1"), "trace-1");

        assertEquals("FAILED", response.getStatus());
        assertEquals("ERR_LLM_01", response.getFlowCode());
        assertEquals("warning", response.getUserErrorStatusType());
        assertEquals("c-1", response.getAttributes().get("customerId"));
        assertTrue(getFlowStateManager(service).hasState("exec-llm"));
    }

    @Test
    @DisplayName("debe persistir estado pausado en handleAgentLoopException para resume")
    void shouldPersistPausedStateOnResume() throws Exception {
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("test");

        FlowService.AgentLoopState state = new FlowService.AgentLoopState();
        setCurrentStep(state, "paso_3");

        RequiresHumanInterventionException ex =
                new RequiresHumanInterventionException("Input requerido");

        FlowResponse response = invokeHandleAgentLoopException(
                "exec-persist", "test-flow", flow,
                List.of(Map.of("role", "user", "content", "hola")),
                state, ex, true, Map.of(), "trace-1");

        assertEquals("PAUSED_FOR_HUMAN", response.getStatus());

        FlowStateManager stateManager = getFlowStateManager(service);
        assertTrue(stateManager.hasState("exec-persist"),
                "El estado debe estar almacenado para poder reanudarse");

        FlowStateManager.PausedFlowState retrieved = stateManager.retrieve("exec-persist");
        assertNotNull(retrieved);
        assertEquals(1, retrieved.messages().size());
    }

    @Test
    @DisplayName("debe mapear error de conexión a mensaje de usuario")
    void shouldMapConnectionErrorToUserMessage() {
        UserFacingErrorMapper.ErrorMapping mapping =
                new UserFacingErrorMapper().mapError("get_vehicle_information",
                        "mcp-domain", "Connection refused");

        assertEquals("connection_error", mapping.errorCode());
        assertEquals("warning", mapping.statusType());
        assertEquals("No se pudo conectar al servicio", mapping.title());
        assertEquals("Reintentar", mapping.actionLabel());
    }

    // ========== Métodos auxiliares ==========

    private FlowResponse invokeHandleAgentLoopException(String executionId, String flowId,
                                                        FlowDefinitions.FlowDefinition flow,
                                                        List<Map<String, Object>> messages,
                                                        FlowService.AgentLoopState state,
                                                        RuntimeException ex, boolean isResume,
                                                        Map<String, Object> attributes,
                                                        String traceId) throws Exception {
        Method method = FlowService.class.getDeclaredMethod(
                "handleAgentLoopException",
                String.class, String.class,
                FlowDefinitions.FlowDefinition.class,
                List.class, FlowService.AgentLoopState.class,
                RuntimeException.class, boolean.class, Map.class, String.class);
        method.setAccessible(true);
        return (FlowResponse) method.invoke(service, executionId, flowId, flow,
                messages, state, ex, isResume, attributes, traceId);
    }

    private FlowStateManager getFlowStateManager(FlowService svc) throws Exception {
        Field f = FlowService.class.getDeclaredField("flowStateManager");
        f.setAccessible(true);
        return (FlowStateManager) f.get(svc);
    }

    private FlowStateManager.PausedFlowState createPausedState() throws Exception {
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("compra-vehiculo");

        FlowDefinitions.ToolDefinition tool = tool("get_vehicle_information", 1,
                "Consultar vehículo", List.of());
        flow.setTools(List.of(tool));

        FlowService.AgentLoopState state = new FlowService.AgentLoopState();
        setCompletedTools(state, List.of("get_vehicle_information"));

        return new FlowStateManager.PausedFlowState(
                "compra-vehiculo", flow,
                List.of(Map.of("role", "system", "content", "system prompt")),
                state);
    }

    // ========== Helpers de reflexión para AgentLoopState ==========

    private void setCompletedTools(FlowService.AgentLoopState state, List<String> tools) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("completedTools");
        f.setAccessible(true);
        f.set(state, new java.util.HashSet<>(tools));
    }

    private List<String> getCompletedTools(FlowService.AgentLoopState state) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("completedTools");
        f.setAccessible(true);
        Object val = f.get(state);
        return val != null ? new ArrayList<>((java.util.Set<String>) val) : List.of();
    }

    private void setFailedTools(FlowService.AgentLoopState state, List<String> tools) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("failedTools");
        f.setAccessible(true);
        f.set(state, new java.util.HashSet<>(tools));
    }

    private List<String> getFailedTools(FlowService.AgentLoopState state) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("failedTools");
        f.setAccessible(true);
        Object val = f.get(state);
        return val != null ? new ArrayList<>((java.util.Set<String>) val) : List.of();
    }

    private void setCurrentStep(FlowService.AgentLoopState state, String step) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("currentStep");
        f.setAccessible(true);
        f.set(state, step);
    }

    private void setCurrentStepOrder(FlowService.AgentLoopState state, int stepOrder) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("currentStepOrder");
        f.setAccessible(true);
        f.setInt(state, stepOrder);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Object>> getToolResults(FlowService.AgentLoopState state) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("toolResults");
        f.setAccessible(true);
        return (Map<String, Map<String, Object>>) f.get(state);
    }
}
