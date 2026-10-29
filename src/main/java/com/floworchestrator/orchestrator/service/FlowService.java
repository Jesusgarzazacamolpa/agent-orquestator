package com.floworchestrator.orchestrator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.floworchestrator.orchestrator.config.FlowDefinitions;
import com.floworchestrator.orchestrator.config.FlowDefinitions.FlowDefinition;
import com.floworchestrator.orchestrator.config.FlowDefinitions.ToolDefinition;
import com.floworchestrator.orchestrator.model.FlowAsyncAccepted;
import com.floworchestrator.orchestrator.model.FlowExecutionEvent;
import com.floworchestrator.orchestrator.model.FlowRequest;
import com.floworchestrator.orchestrator.model.FlowResponse;
import com.floworchestrator.orchestrator.service.AiService.AiResponse;
import com.floworchestrator.orchestrator.service.AiService.AiToolCall;
import com.floworchestrator.orchestrator.service.AiService.ToolSpec;
import com.floworchestrator.orchestrator.service.McpToolClient.DiscoveredTool;
import com.floworchestrator.orchestrator.service.FlowStateManager.AgentStateSnapshot;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.vavr.control.Try;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.stringtemplate.v4.ST;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Núcleo del orquestador: ejecuta el loop de agente sobre el LLM, valida y lanza
 * las herramientas MCP en orden, pausa/reanuda flujos que requieren intervención
 * humana y publica cada transición por SSE.
 */
@Service
public class FlowService {
    private static final Logger log = LoggerFactory.getLogger(FlowService.class);

    private static final String ROLE_SYSTEM = "system";
    private static final String ROLE_USER = "user";
    private static final String ROLE_ASSISTANT = "assistant";
    private static final String ROLE_TOOL = "tool";
    private static final String FLOW_CODE_START_ERROR = "ERR_FLOW_START";
    private static final String FLOW_CODE_CANCELLED = "FLOW_CANCELLED";
    private static final String ERROR_CODE_ALREADY_EXISTS = "ALREADY_EXISTS";

    private final WebClient webClient;
    private final AiService aiService;
    private final FlowServiceConfig config;
    private final ObjectMapper objectMapper;
    private final FlowDefinitions flowDefinitions;
    private final McpToolClient mcpClientRegistry;
    private final FlowEventPublisher flowEventPublisher;
    private final FlowStateManager flowStateManager;
    private final FlowInterruptionHandler interruptionHandler;
    private final UserFacingErrorMapper errorMapper;
    private final PiiMasker piiMasker;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final ExecutorService toolExecutor;
    private final ExecutorService flowExecutor;

    private final Map<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();
    private final Map<String, Retry> retries = new ConcurrentHashMap<>();

    public FlowService(
            WebClient webClient,
            AiService aiService,
            FlowServiceConfig config,
            ObjectMapper objectMapper,
            FlowDefinitions flowDefinitions,
            McpToolClient mcpClientRegistry,
            FlowEventPublisher flowEventPublisher,
            FlowStateManager flowStateManager,
            FlowInterruptionHandler interruptionHandler,
            UserFacingErrorMapper errorMapper,
            PiiMasker piiMasker,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry
    ) {
        this.webClient = webClient;
        this.aiService = aiService;
        this.config = config;
        this.objectMapper = objectMapper;
        this.flowDefinitions = flowDefinitions;
        this.mcpClientRegistry = mcpClientRegistry;
        this.flowEventPublisher = flowEventPublisher;
        this.flowStateManager = flowStateManager;
        this.interruptionHandler = interruptionHandler;
        this.errorMapper = errorMapper;
        this.piiMasker = piiMasker;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.retryRegistry = retryRegistry;
        this.toolExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.flowExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /** Ejecuta el flujo de forma síncrona y devuelve el resultado final; se usa en el endpoint no asíncrono. */
    public FlowResponse createFlow(FlowRequest request) {
        return withTraceId(traceId -> doCreateFlow(request, traceId));
    }

    /**
     * Inicia el flujo en segundo plano y responde de inmediato. Los eventos se
     * consumen por SSE en {@code /api/flows/stream}; el {@code traceId} devuelto
     * permite correlacionarlos cuando aún no se conoce el executionId.
     */
    public FlowAsyncAccepted startFlowAsync(FlowRequest request) {
        selectFlow(request.getFlowId());

        String traceId = newTraceId();
        String requestId = UUID.randomUUID().toString();
        flowExecutor.submit(() -> {
            try {
                withTraceId(traceId, id -> doCreateFlow(request, id));
            } catch (RuntimeException ex) {
                log.error("[ASYNC] No se pudo iniciar el flujo: requestId={}, flowId={}, traceId={}, error={}",
                        requestId, request.getFlowId(), traceId, messageOf(ex), ex);
                withTraceId(traceId, id -> {
                    flowEventPublisher.publish(FlowExecutionEvent.withUserAgentError(
                            request.getExecutionId(), request.getFlowId(), "FLOW_FAILED",
                            null, null, null, null, "FAILED", messageOf(ex),
                            "No se pudo iniciar el flujo",
                            "No fue posible iniciar el flujo. Intenta nuevamente.",
                            "Reintentar", "error", FLOW_CODE_START_ERROR));
                    return null;
                });
            }
        });
        log.info("[ASYNC] Flujo aceptado: requestId={}, flowId={}, executionId={}, traceId={}",
                requestId, request.getFlowId(), request.getExecutionId(), traceId);
        return FlowAsyncAccepted.accepted(requestId, request.getFlowId(),
                request.getExecutionId(), traceId);
    }

    /** Valida el flowId, resuelve el executionId y arranca el loop publicando el evento de inicio. */
    private FlowResponse doCreateFlow(FlowRequest request, String traceId) {
        Map.Entry<String, FlowDefinition> selectedFlow = selectFlow(request.getFlowId());
        String flowId = selectedFlow.getKey();
        FlowDefinition flow = selectedFlow.getValue();
        log.info("[START] Iniciando flujo: flowId={}, traceId={}", flowId, traceId);

        String executionId = resolveExecutionId(flow, request);
        log.info("[ID] Ejecución del proceso creada: flowId={}, executionId={}", flowId, executionId);
        flowEventPublisher.publish(FlowExecutionEvent.of(executionId, flowId, "FLOW_STARTED"));

        FlowResponse response = new FlowResponse();
        response.setId(executionId);
        response.setTraceId(traceId);
        response.setAttributes(request.getAttributes());
        response.setSteps(flow.getTools().stream().map(ToolDefinition::getStepName).toList());

        List<Map<String, Object>> messages = buildPromptMessages(flow, request, flowId);
        AgentLoopState state = new AgentLoopState();
        try {
            log.info("[AGENT] Entrando al bucle del agente: flowId={}, executionId={}, maxAgentTurns={}",
                    flowId, executionId, config.maxAgentTurns());
            boolean completed = runAgentLoop(flow, flowId, executionId, response, messages, state, traceId);
            if (!completed) {
                throw new IllegalStateException("El agente superó el número máximo de turnos");
            }
        } catch (RuntimeException ex) {
            response = interruptionHandler.handle(executionId, flowId, flow, messages, state, ex,
                    false, request.getAttributes(), traceId);
        }
        return response;
    }

    /** Reanuda un flujo pausado de forma síncrona; se usa cuando el llamador puede esperar al LLM. */
    public FlowResponse resumeFlow(String executionId, String userInput) {
        return withTraceId(traceId ->
                doResumeFlow(executionId, userInput, retrievePausedState(executionId), traceId));
    }

    /**
     * Consume el estado pausado y reanuda en segundo plano. El cliente recibe
     * 202 de inmediato; el resultado se sigue por SSE. Evita que el llamador
     * agote su timeout HTTP mientras el LLM trabaja y que dos reanudaciones
     * simultáneas ejecuten el flujo dos veces.
     */
    public FlowAsyncAccepted resumeFlowAsync(String executionId, String userInput) {
        FlowStateManager.PausedFlowState pausedState = retrievePausedState(executionId);

        String traceId = newTraceId();
        flowExecutor.submit(() -> {
            try {
                withTraceId(traceId, id -> doResumeFlow(executionId, userInput, pausedState, id));
            } catch (RuntimeException ex) {
                log.error("[ASYNC] No se pudo reanudar el flujo: executionId={}, traceId={}, error={}",
                        executionId, traceId, messageOf(ex), ex);
            }
        });
        log.info("[ASYNC] Reanudación aceptada: executionId={}, flowId={}, traceId={}",
                executionId, pausedState.flowId(), traceId);
        return FlowAsyncAccepted.accepted(UUID.randomUUID().toString(), pausedState.flowId(),
                executionId, traceId);
    }

    /** Recupera y consume el estado persistido; falla con {@link IllegalStateException} si no hay flujo pausado. */
    private FlowStateManager.PausedFlowState retrievePausedState(String executionId) {
        FlowStateManager.PausedFlowState pausedState = flowStateManager.retrieve(executionId);
        if (pausedState == null) {
            throw new IllegalStateException("No se encontró un flujo pausado para executionId: " + executionId);
        }
        return pausedState;
    }

    /**
     * Rehidrata el estado pausado, inyecta la entrada del operador y, si la
     * respuesta es una aprobación, reintenta la herramienta pendiente antes de
     * volver al loop del agente.
     */
    private FlowResponse doResumeFlow(String executionId, String userInput,
                                      FlowStateManager.PausedFlowState pausedState, String traceId) {
        String flowId = pausedState.flowId();
        FlowDefinition flow = pausedState.flowDefinition();
        List<Map<String, Object>> messages = new ArrayList<>(pausedState.messages());
        AgentLoopState state = pausedState.agentState();

        if (userInput != null && !userInput.isBlank()) {
            Map<String, Object> userMessage = new HashMap<>();
            userMessage.put("role", ROLE_USER);
            userMessage.put("content", userInput);
            messages.add(userMessage);
            log.info("[RESUME] Entrada del usuario inyectada: executionId={}, inputLength={}",
                    executionId, userInput.length());
        }

        if (isApprovalInput(userInput)) {
            approvePendingIntervention(flow, flowId, executionId, messages, state);
        }

        injectPendingRetryGuidance(flow, messages, state, executionId);

        log.info("[RESUME] Reanudando flujo: executionId={}, flowId={}, traceId={}",
                executionId, flowId, traceId);
        flowEventPublisher.publish(FlowExecutionEvent.of(executionId, flowId, "FLOW_RESUMED"));

        FlowResponse response = new FlowResponse();
        response.setId(executionId);
        response.setTraceId(traceId);
        response.setSteps(flow.getTools().stream().map(ToolDefinition::getStepName).toList());
        response.setAttributes(new HashMap<>());

        try {
            boolean completed = runAgentLoop(flow, flowId, executionId, response, messages, state, traceId);
            if (!completed) {
                throw new IllegalStateException("El agente superó el número máximo de turnos");
            }
        } catch (RuntimeException ex) {
            response = interruptionHandler.handle(executionId, flowId, flow, messages, state, ex,
                    true, Map.of(), traceId);
        }
        return response;
    }

    /**
     * Cancela un flujo detenido (pausado o fallido) descartando su estado para
     * que no pueda reanudarse. Los flujos en ejecución no se interrumpen a mitad
     * de una herramienta: este endpoint aplica sobre el estado persistido.
     */
    public FlowResponse cancelFlow(String executionId) {
        return withTraceId(traceId -> doCancelFlow(executionId, traceId));
    }

    /** Descarta el estado persistido y emite {@code FLOW_CANCELLED}; falla si la ejecución no tiene estado cancelable. */
    private FlowResponse doCancelFlow(String executionId, String traceId) {
        FlowStateManager.PausedFlowState pausedState = flowStateManager.retrieve(executionId);
        if (pausedState == null) {
            throw new IllegalStateException(
                    "No se encontró un flujo cancelable para executionId: " + executionId);
        }

        String flowId = pausedState.flowId();
        AgentLoopState state = pausedState.agentState();
        String reason = "Cancelación solicitada por el operador.";
        log.info("[CANCEL] Flujo cancelado; se descarta el estado: executionId={}, flowId={}, step={}, traceId={}",
                executionId, flowId, state.currentStep, traceId);

        flowEventPublisher.publish(FlowExecutionEvent.withUserAgentError(
                executionId, flowId, "FLOW_CANCELLED", state.currentStepOrder, state.currentStep,
                null, null, "CANCELLED", reason,
                "Proceso cancelado", "El flujo se canceló y no continuará ejecutándose.",
                "Iniciar un nuevo proceso", "warning", FLOW_CODE_CANCELLED));

        FlowResponse response = new FlowResponse();
        response.setId(executionId);
        response.setTraceId(traceId);
        response.setStatus("CANCELLED");
        response.setFlowCode(FLOW_CODE_CANCELLED);
        response.setCurrentStep(state.currentStep);
        response.setMessage("El flujo fue cancelado y su estado se descartó.");
        response.setErrorMessage(reason);
        response.setUserErrorTitle("Proceso cancelado");
        response.setUserErrorMessage("El flujo se canceló y no continuará ejecutándose.");
        response.setUserErrorActionLabel("Iniciar un nuevo proceso");
        response.setUserErrorStatusType("warning");
        response.setSteps(pausedState.flowDefinition().getTools().stream()
                .map(ToolDefinition::getStepName).toList());
        return response;
    }

    /** Marca un paso fallido o completado como pendiente de reintento y guarda el estado; la ejecución ocurre en la siguiente reanudación. */
    public FlowResponse retryStep(String executionId, int stepOrder) {
        return withTraceId(traceId -> doRetryStep(executionId, stepOrder, traceId));
    }

    /**
     * Retrocede el estado del paso indicado para que el agente lo vuelva a invocar;
     * solo acepta pasos con ejecución previa registrada. El estado se consulta con
     * {@code peek} y solo se sobrescribe tras validar, de modo que una solicitud
     * inválida no destruye el flujo pausado.
     */
    private FlowResponse doRetryStep(String executionId, int stepOrder, String traceId) {
        FlowStateManager.PausedFlowState pausedState = flowStateManager.peek(executionId);
        if (pausedState == null) {
            throw new IllegalStateException("No se encontró un flujo pausado para executionId: " + executionId);
        }

        FlowDefinition flow = pausedState.flowDefinition();
        ToolDefinition tool = flow.getTools().stream()
                .filter(candidate -> candidate.getStepOrder() == stepOrder)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "No se encontró el paso con orden " + stepOrder
                                + " en el flujo " + flow.getProcessTemplateName()));

        AgentLoopState state = pausedState.agentState();
        boolean executedBefore = state.completedTools.contains(tool.getName())
                || state.failedTools.contains(tool.getName());
        if (!executedBefore) {
            throw new IllegalArgumentException(
                    "El paso " + tool.getName() + " no registra una ejecución previa que pueda reintentarse");
        }

        state.completedTools.remove(tool.getName());
        state.failedTools.remove(tool.getName());
        state.currentStepOrder = stepOrder;
        state.currentStep = tool.getStepName();

        List<Map<String, Object>> messages = new ArrayList<>(pausedState.messages());
        messages.add(retryStepInstruction(tool));

        log.info("[RETRY-STEP] Paso marcado para reintento: executionId={}, stepOrder={}, tool={}, traceId={}",
                executionId, stepOrder, tool.getName(), traceId);

        flowStateManager.store(executionId,
                new FlowStateManager.PausedFlowState(pausedState.flowId(), flow,
                        List.copyOf(messages), state));

        flowEventPublisher.publish(FlowExecutionEvent.withError(
                executionId, pausedState.flowId(), "STEP_RETRY", stepOrder, state.currentStep,
                tool.getName(), tool.getServer(), "RETRYING", null));

        FlowResponse response = new FlowResponse();
        response.setId(executionId);
        response.setTraceId(traceId);
        response.setStatus("RETRYING");
        response.setCurrentStep(state.currentStep);
        response.setSteps(flow.getTools().stream().map(ToolDefinition::getStepName).toList());
        return response;
    }

    /** Mensaje de usuario que ordena al agente reejecutar la herramienta del paso reintentado. */
    private Map<String, Object> retryStepInstruction(ToolDefinition tool) {
        return Map.of(
                "role", ROLE_USER,
                "content", "El operador solicitó reintentar el paso '" + tool.getStepName()
                        + "' (herramienta " + tool.getName() + "). Vuelve a invocarla ahora con los "
                        + "argumentos ya recopilados, corrigiendo la causa del error anterior si la hubo, "
                        + "y continúa con el resto del flujo hasta completarlo.");
    }

    /**
     * Añade una instrucción al historial cuando quedan pasos obligatorios que
     * fallaron en el intento anterior, para que el agente los reintente en lugar
     * de cerrar el flujo con éxito.
     */
    private void injectPendingRetryGuidance(FlowDefinition flow, List<Map<String, Object>> messages,
                                            AgentLoopState state, String executionId) {
        Set<String> pendingFailures = new java.util.TreeSet<>(getRequiredTools(flow));
        pendingFailures.removeAll(state.completedTools);
        pendingFailures.retainAll(state.failedTools);
        if (pendingFailures.isEmpty()) {
            return;
        }
        messages.add(Map.of(
                "role", ROLE_USER,
                "content", "El operador solicitó reanudar el flujo. Los pasos pendientes " + pendingFailures
                        + " fallaron en el intento anterior, pero el fallo no cierra el proceso: "
                        + "revisa el error devuelto por cada herramienta y vuelve a invocarla. "
                        + "Si el error indica datos ya registrados, reutiliza el recurso existente; "
                        + "si indica argumentos inválidos, corrígelos. No finalices el flujo sin "
                        + "completar los pasos obligatorios."));
        log.info("[RESUME] Guía de reintento inyectada: executionId={}, tools={}",
                executionId, pendingFailures);
    }

    private String messageOf(Exception ex) {
        return ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
    }

    /**
     * Resuelve el executionId: usa el del request o crea la ejecución en Control
     * Center a partir de la plantilla de proceso del flujo.
     */
    private String resolveExecutionId(FlowDefinition flow, FlowRequest request) {
        String executionId = request.getExecutionId();
        if (executionId != null && !executionId.isBlank()) {
            return executionId;
        }
        Map<String, Object> templateResponse = webClient.get()
                .uri(UriComponentsBuilder.fromUriString(config.controlCenterBaseUrl()
                                + "/api/process-templates/by-name")
                        .queryParam("name", flow.getProcessTemplateName())
                        .build()
                        .encode()
                        .toUri())
                .retrieve()
                .bodyToMono(Map.class)
                .block(config.controlCenterTimeout());
        if (templateResponse == null || templateResponse.get("id") == null) {
            throw new IllegalStateException("El Control Center no devolvió un ID de plantilla de proceso activa");
        }

        Map<String, Object> controlCenterRequest = new HashMap<>();
        controlCenterRequest.put("processTemplateId", templateResponse.get("id"));
        controlCenterRequest.put("customerId", request.getAttributes().get("customerId"));

        Map<String, Object> controlCenterResponse = webClient.post()
                .uri(config.controlCenterBaseUrl() + "/api/processes")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(controlCenterRequest)
                .retrieve()
                .bodyToMono(Map.class)
                .block(config.controlCenterTimeout());
        if (controlCenterResponse == null || controlCenterResponse.get("id") == null) {
            throw new IllegalStateException("El Control Center no devolvió un ID de ejecución del proceso");
        }
        return controlCenterResponse.get("id").toString();
    }

    /** Construye los mensajes iniciales system+user renderizando las plantillas con los atributos del request. */
    private List<Map<String, Object>> buildPromptMessages(FlowDefinition flow, FlowRequest request,
                                                          String flowId) {
        Map<String, Object> promptAttributes = new HashMap<>();
        promptAttributes.put("flowId", flowId);
        promptAttributes.put("flowName", flow.getProcessTemplateName());
        promptAttributes.putAll(request.getAttributes());
        promptAttributes.put("toolNames",
                flow.getTools().stream().map(ToolDefinition::getName).toList());

        List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(Map.of(
                "role", ROLE_SYSTEM,
                "content", renderPrompt(flow.getSystemPromptTemplate(), promptAttributes)
        ));
        messages.add(Map.of(
                "role", ROLE_USER,
                "content", renderPrompt(flow.getUserPromptTemplate(), promptAttributes)
        ));
        return messages;
    }

    /**
     * Recorta el historial conservando siempre el mensaje system y los últimos
     * mensajes; si el corte dejaría respuestas de herramienta sin su llamada,
     * avanza el inicio para no enviar un historial inválido al LLM.
     */
    private void trimContextIfNeeded(List<Map<String, Object>> messages) {
        if (messages.isEmpty() || messages.size() <= config.maxContextMessages()) {
            return;
        }
        Map<String, Object> systemMessage = messages.get(0);
        int keepFromEnd = Math.max(1, config.maxContextMessages() - 1);
        int start = messages.size() - keepFromEnd;
        // No dejar respuestas de herramienta huérfanas sin su llamada asociada.
        while (start < messages.size() && ROLE_TOOL.equals(messages.get(start).get("role"))) {
            start++;
        }
        if (start >= messages.size()) {
            start = messages.size() - keepFromEnd;
        }
        List<Map<String, Object>> trimmed = new ArrayList<>(keepFromEnd + 1);
        trimmed.add(systemMessage);
        trimmed.addAll(messages.subList(start, messages.size()));
        messages.clear();
        messages.addAll(trimmed);
        log.info("[CONTEXT] Ventana de contexto recortada: se conservaron {} mensajes (máx. {})",
                messages.size(), config.maxContextMessages());
    }

    /**
     * Ejecuta turnos de agente hasta completar el flujo, pausar o agotar
     * {@code maxAgentTurns}. Antes de cada turno cierra el flujo sin llamar al LLM
     * si ya están todos los pasos obligatorios, y solo ofrece las herramientas
     * cuyas dependencias están satisfechas. Devuelve {@code false} al agotar turnos.
     */
    private boolean runAgentLoop(FlowDefinition flow, String flowId, String executionId,
                                 FlowResponse response, List<Map<String, Object>> messages,
                                 AgentLoopState state, String traceId) {
        for (int turn = 0; turn < config.maxAgentTurns(); turn++) {
            Set<String> requiredTools = getRequiredTools(flow);
            if (!requiredTools.isEmpty() && state.completedTools.containsAll(requiredTools)) {
                // Pedir otro turno solo para redactar el cierre añade 30-60 s y expone
                // el flujo a un timeout cuando los pasos de negocio ya terminaron.
                log.info("[DONE] Pasos obligatorios completos; se cierra el flujo sin un turno "
                                + "adicional del LLM: flowId={}, executionId={}",
                        flowId, executionId);
                completeFlow(flow, flowId, executionId, response,
                        completionFallbackMessage(flow, state.completedTools), state.completedTools);
                return true;
            }
            log.info("[AGENT] Iniciando turno {}/{}: flowId={}, executionId={}",
                    turn + 1, config.maxAgentTurns(), flowId, executionId);
            trimContextIfNeeded(messages);

            List<ToolSpec> toolSpecs = getToolSpecs(flow, state);
            log.info("[TOOLS] Ofreciendo {} herramientas al LLM: flowId={}, executionId={}, tools={}",
                    toolSpecs.size(), flowId, executionId,
                    toolSpecs.stream().map(ToolSpec::name).toList());

            AiResponse aiResponse = aiService.generateResponse(messages, toolSpecs, traceId, flowId);
            if (aiResponse.degraded()) {
                // Defensivo: si no quedan pasos pendientes, el fallo del LLM no debe
                // marcar el flujo como fallido.
                if (state.completedTools.containsAll(requiredTools)) {
                    String fallbackMessage = completionFallbackMessage(flow, state.completedTools);
                    log.warn("[AI-DEGRADED] LLM no disponible después de completar todos los pasos "
                                    + "obligatorios; se cierra el flujo con un resumen de respaldo: "
                                    + "flowId={}, executionId={}",
                            flowId, executionId);
                    completeFlow(flow, flowId, executionId, response, fallbackMessage,
                            state.completedTools);
                    return true;
                }
                throw new LlmUnavailableException(
                        "LLM no disponible para el flujo " + flowId,
                        aiResponse.flowCode(), aiResponse.userMessage(), aiResponse.userAction());
            }

            String content = aiResponse.content();
            log.info("[AI] Respuesta de Ollama recibida: flowId={}, executionId={}, content={}",
                    flowId, executionId, piiMasker.mask(content));

            List<AiToolCall> toolCalls = aiResponse.toolCalls();
            if (toolCalls.isEmpty()) {
                toolCalls = parseToolCallsFromContent(content, flow, flowId, executionId);
            }
            messages.add(assistantMessage(content, toolCalls));

            if (toolCalls.isEmpty()) {
                if (isRequestingUserInput(content)) {
                    log.info("[USER_INPUT] El LLM solicita información al usuario: flowId={}, executionId={}",
                            flowId, executionId);
                    throw new RequiresHumanInterventionException(
                            "El agente necesita información adicional del usuario para continuar: " + content);
                }
                completeFlow(flow, flowId, executionId, response, content, state.completedTools);
                return true;
            }

            processToolCalls(flow, flowId, executionId, messages, state, toolCalls);
        }
        return false;
    }

    /** Serializa la respuesta del modelo (incluidas sus tool calls) al formato del historial propio. */
    private Map<String, Object> assistantMessage(String content, List<AiToolCall> toolCalls) {
        Map<String, Object> assistantMessage = new HashMap<>();
        assistantMessage.put("role", ROLE_ASSISTANT);
        assistantMessage.put("content", content);
        if (!toolCalls.isEmpty()) {
            assistantMessage.put("tool_calls", toolCalls.stream()
                    .map(this::toToolCallMap)
                    .toList());
        }
        return assistantMessage;
    }

    private Map<String, Object> toToolCallMap(AiToolCall toolCall) {
        Map<String, Object> function = new HashMap<>();
        function.put("name", toolCall.name());
        function.put("arguments", toolCall.arguments());
        Map<String, Object> call = new HashMap<>();
        call.put("id", toolCall.id());
        call.put("function", function);
        return call;
    }

    /** Resumen con el que se cierra el flujo cuando no se pidió un turno final al LLM. */
    private String completionFallbackMessage(FlowDefinition flow, Set<String> completedTools) {
        return "El flujo " + flow.getProcessTemplateName() + " se completó correctamente ("
                + completedTools.size() + " pasos ejecutados).";
    }

    /**
     * Cierra el flujo como COMPLETED si están todos los pasos obligatorios; si
     * faltan, lanza {@link RequiresHumanInterventionException} para pausarlo en
     * lugar de darlo por fallido.
     *
     * @throws RequiresHumanInterventionException si el agente dejó pasos obligatorios sin ejecutar.
     */
    private void completeFlow(FlowDefinition flow, String flowId, String executionId,
                              FlowResponse response, String content, Set<String> completedTools) {
        Set<String> requiredTools = getRequiredTools(flow);
        if (!completedTools.containsAll(requiredTools)) {
            // El agente dejó de llamar herramientas con pasos pendientes: se pausa
            // para que el usuario aporte lo que falta en lugar de fallar el flujo.
            throw new RequiresHumanInterventionException(
                    "El agente terminó el turno sin completar los pasos obligatorios: "
                            + difference(requiredTools, completedTools)
                            + ". Proporciona la información pendiente y reanuda el flujo.");
        }
        log.info("[DONE] Flujo completado: flowId={}, executionId={}", flowId, executionId);
        flowEventPublisher.publish(FlowExecutionEvent.withError(
                executionId, flowId, "FLOW_COMPLETED", null, null, null, null,
                "COMPLETED", null));
        response.setStatus("COMPLETED");
        response.setMessage(content);
    }

    /**
     * Procesa las tool calls del turno: descarta herramientas no permitidas, valida
     * dependencias y argumentos, ejecuta cada paso y devuelve el resultado al modelo.
     * Los errores se agregan al historial para que el agente pueda corregir en el
     * siguiente turno en vez de abortar el flujo.
     */
    private void processToolCalls(FlowDefinition flow, String flowId, String executionId,
                                  List<Map<String, Object>> messages, AgentLoopState state,
                                  List<AiToolCall> toolCalls) {
        for (AiToolCall toolCall : toolCalls) {
            String toolName = toolCall.name();
            log.info("[TOOLS] Procesando llamada a herramienta: flowId={}, executionId={}, toolName={}, toolCallId={}",
                    flowId, executionId, toolName, toolCall.id());
            ToolDefinition tool;
            try {
                tool = findTool(flow, toolName);
            } catch (IllegalArgumentException ex) {
                log.warn("[BLOCKED] El agente solicitó una herramienta no permitida por el flujo: "
                                + "flowId={}, executionId={}, tool={}",
                        flowId, executionId, toolName);
                addToolErrorMessage(messages, toolName, toolCall.id(), ex.getMessage());
                continue;
            }

            state.currentStepOrder = tool.getStepOrder();
            state.currentStep = tool.getStepName();
            flowEventPublisher.publish(FlowExecutionEvent.withError(
                    executionId, flowId, "STEP_STARTED", state.currentStepOrder, state.currentStep,
                    toolName, tool.getServer(), "RUNNING", null));
            try {
                validateToolCall(tool, toolCall.arguments(), state);
            } catch (IllegalArgumentException ex) {
                log.warn("[RETRY] Llamada a herramienta del agente rechazada: flowId={}, executionId={}, tool={}, reason={}",
                        flowId, executionId, toolName, ex.getMessage());
                state.failedTools.add(toolName);
                addToolErrorMessage(messages, toolName, toolCall.id(), ex.getMessage());
                UserFacingErrorMapper.ErrorMapping userError =
                        errorMapper.mapError(toolName, tool.getServer(), ex.getMessage());
                flowEventPublisher.publish(FlowExecutionEvent.withUserAgentError(
                        executionId, flowId, "STEP_FAILED", state.currentStepOrder, state.currentStep,
                        toolName, tool.getServer(), "FAILED", ex.getMessage(),
                        userError.title(), userError.message(), userError.actionLabel(),
                        userError.statusType(), userError.errorCode()));
                resetCurrentStep(state);
                continue;
            }

            if (!state.completedTools.contains(toolName)) {
                boolean success = executeToolStep(flowId, executionId, messages, state,
                        tool, toolCall);
                if (success) {
                    addToolMessage(messages, toolName, toolCall.id(),
                            writeJson(state.toolResults.get(toolName)));
                }
            } else {
                addToolMessage(messages, toolName, toolCall.id(),
                        writeJson(state.toolResults.get(toolName)));
            }
            resetCurrentStep(state);
        }
    }

    /** Restablece el paso en curso para que no quede marcado tras terminar una tool call. */
    private void resetCurrentStep(AgentLoopState state) {
        state.currentStepOrder = 0;
        state.currentStep = null;
    }

    /** Considera aprobación la entrada que empieza por {@code APPROVED}, sin distinguir mayúsculas. */
    private boolean isApprovalInput(String userInput) {
        return userInput != null && userInput.strip().toUpperCase(Locale.ROOT).startsWith("APPROVED");
    }

    /** Guarda la herramienta, el toolCallId y los argumentos que quedaron pendientes de aprobación. */
    private void rememberPendingIntervention(AgentLoopState state, ToolDefinition tool,
                                             AiToolCall toolCall, Map<String, Object> result) {
        state.pendingInterventionTool = tool.getName();
        state.pendingInterventionToolCallId = toolCall.id();
        state.pendingInterventionArguments = new HashMap<>(toolCall.arguments());
        log.info("[PAUSE] Herramienta pendiente de autorización registrada: tool={}, step={}, status={}",
                tool.getName(), tool.getStepName(), result.get("status"));
    }

    /**
     * Al aprobar una intervención se reintenta la herramienta pendiente con
     * {@code decision=APPROVED} sin depender de que el LLM decida invocarla: con
     * modelos pequeños la respuesta suele venir sin tool calls y el flujo volvía
     * a pausarse. El historial conserva la forma assistant(tool_call) → tool(result).
     */
    private void approvePendingIntervention(FlowDefinition flow, String flowId, String executionId,
                                            List<Map<String, Object>> messages, AgentLoopState state) {
        String toolName = state.pendingInterventionTool;
        Map<String, Object> storedArguments = state.pendingInterventionArguments;
        state.pendingInterventionTool = null;
        state.pendingInterventionToolCallId = null;
        state.pendingInterventionArguments = null;
        if (toolName == null) {
            return;
        }

        ToolDefinition tool = findTool(flow, toolName);
        Map<String, Object> arguments = storedArguments == null
                ? new HashMap<>()
                : new HashMap<>(storedArguments);
        DiscoveredTool discovered = discoverTool(tool.getServer(), toolName, state);
        if (asMap(discovered.inputSchema().get("properties")).containsKey("decision")) {
            arguments.put("decision", "APPROVED");
        }

        AiToolCall approvalCall = new AiToolCall(UUID.randomUUID().toString(), toolName, arguments);
        log.info("[RESUME] Aprobación recibida; se reintenta la herramienta pendiente: "
                + "executionId={}, tool={}", executionId, toolName);

        state.currentStepOrder = tool.getStepOrder();
        state.currentStep = tool.getStepName();
        flowEventPublisher.publish(FlowExecutionEvent.withError(
                executionId, flowId, "STEP_STARTED", state.currentStepOrder, state.currentStep,
                toolName, tool.getServer(), "RUNNING", null));

        messages.add(assistantMessage("", List.of(approvalCall)));
        boolean success = executeToolStep(flowId, executionId, messages, state, tool, approvalCall);
        if (success) {
            addToolMessage(messages, toolName, approvalCall.id(), writeJson(state.toolResults.get(toolName)));
        }
        resetCurrentStep(state);
    }

    /**
     * Ejecuta un paso MCP con reintentos y traduce el resultado a eventos y estado:
     * éxito, intervención requerida ({@code requiresIntervention}), error
     * {@code ALREADY_EXISTS} tratado como ya aplicado, o fallo devuelto al agente.
     */
    private boolean executeToolStep(String flowId, String executionId,
                                    List<Map<String, Object>> messages, AgentLoopState state,
                                    ToolDefinition tool, AiToolCall toolCall) {
        String toolName = tool.getName();
        log.info("[CALL] El agente solicitó la herramienta MCP: flowId={}, executionId={}, server={}, tool={}, step={}",
                flowId, executionId, tool.getServer(), toolName, state.currentStep);
        long toolStartedAt = System.nanoTime();
        try {
            Map<String, Object> result = executeToolWithRetry(tool, toolCall.arguments());
            if (isInterventionRequired(result)) {
                rememberPendingIntervention(state, tool, toolCall, result);
                // La conversación no puede quedar con un tool_call sin respuesta:
                // el LLM recibiría un historial inválido al reanudar y respondería vacío.
                addToolMessage(messages, toolName, toolCall.id(), writeJson(result));
                throw new RequiresHumanInterventionException(extractInterventionReason(result));
            }
            state.toolResults.put(toolName, result);
            state.completedTools.add(toolName);
            state.failedTools.remove(toolName);
            flowEventPublisher.publish(FlowExecutionEvent.withError(
                    executionId, flowId, "STEP_COMPLETED", state.currentStepOrder, state.currentStep,
                    toolName, tool.getServer(), "COMPLETED", null));
            log.info("[OK] Herramienta MCP completada: flowId={}, executionId={}, tool={}, durationMs={}",
                    flowId, executionId, toolName,
                    (System.nanoTime() - toolStartedAt) / 1_000_000);
            return true;
        } catch (RequiresHumanInterventionException ex) {
            UserFacingErrorMapper.ErrorMapping userError =
                    errorMapper.mapError(toolName, tool.getServer(), ex.getMessage());
            flowEventPublisher.publish(FlowExecutionEvent.withUserAgentError(
                    executionId, flowId, "STEP_REQUIRES_INTERVENTION",
                    state.currentStepOrder, state.currentStep,
                    toolName, tool.getServer(), "REQUIRES_INTERVENTION", ex.getMessage(),
                    userError.title(), userError.message(), userError.actionLabel(),
                    userError.statusType(), userError.errorCode()));
            throw ex;
        } catch (RuntimeException ex) {
            if (ex instanceof McpToolException toolException
                    && ERROR_CODE_ALREADY_EXISTS.equals(toolException.getErrorCode())) {
                return reuseExistingToolResult(executionId, flowId, tool, state, toolException);
            }
            String toolError = messageOf(ex);
            state.failedTools.add(toolName);
            log.warn("[FAIL] Falló la herramienta MCP; se agrega el error al contexto para que el agente reintente: "
                            + "flowId={}, executionId={}, tool={}, retryable={}, error={}",
                    flowId, executionId, toolName,
                    ex instanceof McpToolException toolException && toolException.isRetryable(), toolError);
            addToolErrorMessage(messages, toolName, toolCall.id(), toolError);
            UserFacingErrorMapper.ErrorMapping userError =
                    errorMapper.mapError(toolName, tool.getServer(), toolError);
            flowEventPublisher.publish(FlowExecutionEvent.withUserAgentError(
                    executionId, flowId, "STEP_FAILED", state.currentStepOrder, state.currentStep,
                    toolName, tool.getServer(), "FAILED", toolError,
                    userError.title(), userError.message(), userError.actionLabel(),
                    userError.statusType(), userError.errorCode()));
            return false;
        }
    }

    /**
     * Un error {@code ALREADY_EXISTS} significa que la operación ya se aplicó con
     * anterioridad: el paso queda cumplido y sus dependencias se desbloquean. Si no
     * se tratara así, el agente quedaría atrapado reintentando el mismo paso porque
     * las herramientas siguientes nunca se ofrecerían.
     */
    private boolean reuseExistingToolResult(String executionId, String flowId,
                                            FlowDefinitions.ToolDefinition tool,
                                            AgentLoopState state, McpToolException ex) {
        String toolName = tool.getName();
        Map<String, Object> result = new HashMap<>(ex.getPayload());
        result.putIfAbsent("status", ERROR_CODE_ALREADY_EXISTS);
        result.putIfAbsent("reused_existing_resource", true);
        state.toolResults.put(toolName, result);
        state.completedTools.add(toolName);
        state.failedTools.remove(toolName);
        flowEventPublisher.publish(FlowExecutionEvent.withError(
                executionId, flowId, "STEP_COMPLETED", state.currentStepOrder, state.currentStep,
                toolName, tool.getServer(), "COMPLETED", null));
        log.info("[OK] La herramienta ya estaba aplicada; se reutiliza el recurso existente: "
                        + "flowId={}, executionId={}, tool={}",
                flowId, executionId, toolName);
        return true;
    }

    /** Estado mutable de una ejecución (pasos completados/fallidos, resultados y tool pendiente) que se persiste al pausar. */
    static class AgentLoopState {
        private final Set<String> completedTools = new HashSet<>();
        private final Set<String> failedTools = new HashSet<>();
        private final Map<String, Map<String, Object>> toolResults = new HashMap<>();
        private final Map<String, List<DiscoveredTool>> discoveredByServer = new HashMap<>();
        private int currentStepOrder = 0;
        private String currentStep = null;
        private String pendingInterventionTool = null;
        private String pendingInterventionToolCallId = null;
        private Map<String, Object> pendingInterventionArguments = null;

        String currentStepName() {
            return currentStep;
        }

        int currentStepNumber() {
            return currentStepOrder;
        }

        AgentStateSnapshot snapshot() {
            // discoveredByServer se omite a propósito: al reanudar se redescubren
            // las herramientas para no reutilizar esquemas obsoletos.
            return new AgentStateSnapshot(
                    new HashSet<>(completedTools),
                    new HashSet<>(failedTools),
                    new HashMap<>(toolResults),
                    currentStepOrder,
                    currentStep,
                    pendingInterventionTool,
                    pendingInterventionToolCallId,
                    pendingInterventionArguments == null
                            ? null
                            : new HashMap<>(pendingInterventionArguments));
        }

        static AgentLoopState fromSnapshot(AgentStateSnapshot snapshot) {
            AgentLoopState state = new AgentLoopState();
            if (snapshot.completedTools() != null) {
                state.completedTools.addAll(snapshot.completedTools());
            }
            if (snapshot.failedTools() != null) {
                state.failedTools.addAll(snapshot.failedTools());
            }
            if (snapshot.toolResults() != null) {
                state.toolResults.putAll(snapshot.toolResults());
            }
            state.currentStepOrder = snapshot.currentStepOrder();
            state.currentStep = snapshot.currentStep();
            state.pendingInterventionTool = snapshot.pendingInterventionTool();
            state.pendingInterventionToolCallId = snapshot.pendingInterventionToolCallId();
            state.pendingInterventionArguments = snapshot.pendingInterventionArguments() == null
                    ? null
                    : new HashMap<>(snapshot.pendingInterventionArguments());
            return state;
        }
    }

    /**
     * Construye las herramientas que se ofrecen al LLM: excluye las ya completadas
     * y las que aún tienen dependencias pendientes, y toma descripción y esquema
     * del descubrimiento MCP.
     */
    private List<ToolSpec> getToolSpecs(FlowDefinition flow, AgentLoopState state) {
        return flow.getTools().stream()
                .filter(tool -> !state.completedTools.contains(tool.getName()))
                .filter(tool -> state.completedTools.containsAll(tool.getDependsOn()))
                .map(tool -> {
                    DiscoveredTool discovered = discoverTool(tool.getServer(), tool.getName(), state);
                    return new ToolSpec(
                            tool.getName(),
                            discovered.description() == null ? "" : discovered.description(),
                            discovered.inputSchema());
                })
                .toList();
    }

    /** Descubre las tools del servidor MCP una sola vez por ejecución y devuelve la solicitada. */
    private DiscoveredTool discoverTool(String server, String toolName, AgentLoopState state) {
        List<DiscoveredTool> discoveredTools = state.discoveredByServer.computeIfAbsent(
                server, serverName -> {
                    List<DiscoveredTool> tools = mcpClientRegistry.listTools(serverName);
                    log.info("[DISCOVER] Herramientas MCP descubiertas: server={}, tools={}", serverName,
                            tools.stream().map(DiscoveredTool::name).toList());
                    return tools;
                });
        return discoveredTools.stream()
                .filter(candidate -> candidate.name().equals(toolName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("La herramienta MCP no está disponible: " + toolName));
    }

    private Set<String> getRequiredTools(FlowDefinition flow) {
        return flow.getTools().stream()
                .filter(ToolDefinition::isRequired)
                .map(ToolDefinition::getName)
                .collect(Collectors.toSet());
    }

    /**
     * Rechaza la llamada si alguna dependencia no está completada o si los
     * argumentos no cumplen el JSON Schema descubierto de la herramienta.
     */
    private void validateToolCall(ToolDefinition tool,
                                  Map<String, Object> modelArguments,
                                  AgentLoopState state) {
        for (String dependency : tool.getDependsOn()) {
            if (!state.completedTools.contains(dependency)) {
                throw new IllegalArgumentException("La herramienta obligatoria no se ha completado: " + dependency);
            }
        }
        DiscoveredTool discoveredTool = discoverTool(tool.getServer(), tool.getName(), state);
        validateArguments(discoveredTool, modelArguments);
    }

    private Map<String, Object> executeTool(ToolDefinition tool, Map<String, Object> arguments) {
        return mcpClientRegistry.callTool(tool.getServer(), tool.getName(), arguments);
    }

    /**
     * Reintenta la herramienta solo cuando el error se declara retryable, hasta
     * {@code maxMcpRetries + 1} intentos, con espera lineal creciente.
     */
    private Map<String, Object> executeToolWithRetry(ToolDefinition tool, Map<String, Object> arguments) {
        int maxAttempts = config.maxMcpRetries() + 1;
        for (int attempt = 1; ; attempt++) {
            try {
                return executeToolAttempt(tool, arguments);
            } catch (McpToolException ex) {
                if (!ex.isRetryable() || attempt >= maxAttempts) {
                    throw ex;
                }
                log.warn("[RETRY] La herramienta MCP reportó un fallo transitorio; se reintenta: "
                                + "tool={}, errorCode={}, attempt={}/{}",
                        tool.getName(), ex.getErrorCode(), attempt, maxAttempts);
                backoffBeforeToolRetry(attempt);
            }
        }
    }

    /**
     * Ejecuta la llamada MCP con circuit breaker y retry propios del servidor;
     * los errores de negocio ({@link McpToolException}) no se reintentan.
     */
    private Map<String, Object> executeToolAttempt(ToolDefinition tool, Map<String, Object> arguments) {
        CircuitBreaker circuitBreaker = circuitBreakerFor(tool.getServer());
        Retry retry = retryFor(tool.getServer());

        Supplier<Map<String, Object>> supplier = () -> {
            try {
                return callToolWithTimeout(tool, arguments);
            } catch (TimeoutException e) {
                throw new IllegalStateException("Tiempo de espera agotado en la herramienta MCP: " + tool.getName(), e);
            }
        };

        Try<Map<String, Object>> result = Try.ofSupplier(
                CircuitBreaker.decorateSupplier(circuitBreaker,
                        Retry.decorateSupplier(retry, supplier)));

        return result.get();
    }

    private void backoffBeforeToolRetry(int attempt) {
        try {
            Thread.sleep(Duration.ofSeconds(1).multipliedBy(attempt).toMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrumpido durante la espera entre reintentos de la herramienta", ex);
        }
    }

    /**
     * Devuelve el circuit breaker del servidor MCP (se crea por servidor e ignora
     * {@link McpToolException}, porque los errores de negocio no indican caída).
     */
    private CircuitBreaker circuitBreakerFor(String server) {
        return circuitBreakers.computeIfAbsent(server, name ->
                circuitBreakerRegistry.circuitBreaker("mcp-" + name, CircuitBreakerConfig.custom()
                        .failureRateThreshold(config.mcpCircuitBreakerThreshold())
                        .slidingWindowSize(10)
                        .minimumNumberOfCalls(5)
                        .waitDurationInOpenState(config.mcpCircuitBreakerResetTimeout())
                        .ignoreExceptions(McpToolException.class)
                        .build())
        );
    }

    /** Retry por servidor MCP con backoff exponencial; no reintenta errores de negocio. */
    private Retry retryFor(String server) {
        return retries.computeIfAbsent(server, name ->
                retryRegistry.retry("mcp-" + name, RetryConfig.custom()
                        .maxAttempts(config.maxMcpRetries() + 1)
                        .intervalFunction(IntervalFunction.ofExponentialBackoff(
                                Duration.ofSeconds(1), 2.0))
                        .retryExceptions(RuntimeException.class, TimeoutException.class)
                        .ignoreExceptions(McpToolException.class)
                        .build())
        );
    }

    /** Ejecuta la herramienta en un hilo virtual y corta la espera con {@code stepTimeout}. */
    private Map<String, Object> callToolWithTimeout(ToolDefinition tool, Map<String, Object> arguments)
            throws TimeoutException {
        CompletableFuture<Map<String, Object>> future =
                CompletableFuture.supplyAsync(() -> executeTool(tool, arguments), toolExecutor);
        try {
            return future.get(config.stepTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            future.cancel(true);
            throw ex;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrumpido durante la llamada a la herramienta MCP: " + tool.getName(), ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException("Falló la herramienta MCP: " + tool.getName(), cause);
        }
    }

    /** Contrato con las tools MCP: {@code requiresIntervention=true} significa que el flujo debe pausarse. */
    private boolean isInterventionRequired(Map<String, Object> result) {
        Object value = result.get("requiresIntervention");
        return value instanceof Boolean bool && bool;
    }

    /** Extrae el motivo de la pausa del resultado de la tool ({@code reason} o {@code errorMessage}). */
    private String extractInterventionReason(Map<String, Object> result) {
        Object reason = result.get("reason");
        if (reason != null && !reason.toString().isBlank()) {
            return reason.toString();
        }
        Object message = result.get("errorMessage");
        if (message != null && !message.toString().isBlank()) {
            return message.toString();
        }
        return "Intervención humana requerida";
    }

    private static final Pattern USER_INPUT_PATTERN = Pattern.compile(
            ".*(¿|necesitamos conocer|proporcione|por favor proporcione|necesitamos saber|requerimos|"
                    + "especificar el modelo|model_id|please provide|need to know|require|input needed|"
                    + "information about|provide.*details).*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /**
     * Heurística para detectar que el texto del LLM pide datos en lugar de haber
     * terminado; solo aplica cuando el modelo no emitió tool calls.
     */
    private boolean isRequestingUserInput(String content) {
        if (content == null || content.isBlank()) {
            return false;
        }
        return USER_INPUT_PATTERN.matcher(content).matches();
    }

    private void addToolMessage(List<Map<String, Object>> messages, String toolName,
                                String toolCallId, String content) {
        Map<String, Object> toolMessage = new HashMap<>();
        toolMessage.put("role", ROLE_TOOL);
        toolMessage.put("tool_name", toolName);
        toolMessage.put("tool_call_id", toolCallId);
        toolMessage.put("content", content);
        messages.add(toolMessage);
    }

    private void addToolErrorMessage(List<Map<String, Object>> messages, String toolName,
                                     String toolCallId, String errorMessage) {
        addToolMessage(messages, toolName, toolCallId,
                writeJson(Map.of("error", errorMessage, "retryable", true)));
    }

    private void validateArguments(DiscoveredTool tool, Map<String, Object> arguments) {
        Map<String, Object> properties = asMap(tool.inputSchema().get("properties"));
        String allowedArguments = properties.isEmpty()
                ? "(ninguno)"
                : String.join(", ", properties.keySet());
        for (String argument : arguments.keySet()) {
            if (!properties.containsKey(argument)) {
                throw new IllegalArgumentException(
                        "El argumento no está permitido para la herramienta MCP " + tool.name() + ": " + argument
                                + ". Allowed arguments: " + allowedArguments);
            }
        }
        Object requiredValue = tool.inputSchema().get("required");
        if (requiredValue instanceof List<?> required) {
            for (Object argument : required) {
                if (arguments.get(String.valueOf(argument)) == null) {
                    throw new IllegalArgumentException(
                            "Falta el argumento para la herramienta MCP " + tool.name() + ": " + argument
                                    + ". Allowed arguments: " + allowedArguments);
                }
            }
        }
    }

    /**
     * Busca el flujo por flowId o por nombre de plantilla de proceso (sin distinguir
     * mayúsculas) y valida su configuración antes de usarlo.
     */
    private Map.Entry<String, FlowDefinition> selectFlow(String requestedFlowId) {
        if (requestedFlowId == null || requestedFlowId.isBlank()) {
            throw new IllegalArgumentException("El flowId es obligatorio");
        }
        String normalizedFlowId = requestedFlowId.trim();
        Map.Entry<String, FlowDefinition> selectedFlow = flowDefinitions.getFlows().entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(normalizedFlowId)
                        || entry.getValue().getProcessTemplateName().equalsIgnoreCase(normalizedFlowId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "flowId o nombre de plantilla de proceso desconocido: " + requestedFlowId
                                + ". Available flowIds: " + flowDefinitions.getFlows().keySet()));
        validateFlow(selectedFlow.getValue());
        return selectedFlow;
    }

    /**
     * Verifica que el flujo tenga prompts y tools, y que cada tool tenga nombres
     * únicos, servidor MCP declarado, orden positivo y nombre de paso.
     */
    private void validateFlow(FlowDefinition flow) {
        if (flow.getProcessTemplateName() == null || flow.getProcessTemplateName().isBlank()
                || flow.getSystemPromptTemplate() == null || flow.getSystemPromptTemplate().isBlank()
                || flow.getUserPromptTemplate() == null || flow.getUserPromptTemplate().isBlank()
                || flow.getTools().isEmpty()) {
            throw new IllegalStateException("La configuración del flujo está incompleta");
        }
        Set<String> ids = new HashSet<>();
        Set<Integer> stepOrders = new HashSet<>();
        for (ToolDefinition tool : flow.getTools()) {
            if (tool.getName() == null || !ids.add(tool.getName())
                    || tool.getServer() == null || !flowDefinitions.getMcpServers().containsKey(tool.getServer())
                    || tool.getStepOrder() < 1 || !stepOrders.add(tool.getStepOrder())
                    || tool.getStepName() == null || tool.getStepName().isBlank()) {
                throw new IllegalStateException("Configuración de herramienta MCP inválida o duplicada en el flujo");
            }
        }
    }

    private ToolDefinition findTool(FlowDefinition flow, String id) {
        return flow.getTools().stream()
                .filter(tool -> tool.getName().equals(id))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("La herramienta no está permitida en este flujo: " + id));
    }

    private Set<String> difference(Set<String> required, Set<String> completed) {
        Set<String> missing = new HashSet<>(required);
        missing.removeAll(completed);
        return missing;
    }

    /**
     * Respaldo para modelos que escriben la tool call como JSON dentro del texto:
     * extrae los fragmentos con {@code name} y {@code arguments} y descarta los que
     * no corresponden a herramientas del flujo.
     */
    private List<AiToolCall> parseToolCallsFromContent(String content, FlowDefinition flow,
                                                       String flowId, String executionId) {
        List<AiToolCall> result = new ArrayList<>();
        if (content == null || content.isBlank()) {
            return result;
        }

        log.debug("[PARSE] Intentando extraer llamadas a herramientas del contenido: flowId={}, content={}",
                flowId, content);

        Pattern pattern = Pattern.compile("\\{[^{}]*\"name\"\\s*:\\s*\"[^\"]+\"[^{}]*\\}");
        Matcher matcher = pattern.matcher(content);
        while (matcher.find()) {
            String jsonFragment = matcher.group();
            try {
                Map<String, Object> parsed = objectMapper.readValue(jsonFragment, Map.class);
                if (!parsed.containsKey("name")
                        || (!parsed.containsKey("parameters") && !parsed.containsKey("arguments"))) {
                    continue;
                }
                String toolName = String.valueOf(parsed.get("name"));
                boolean allowed = flow.getTools().stream()
                        .anyMatch(tool -> tool.getName().equals(toolName));
                if (!allowed) {
                    log.warn("[PARSE] Llamada a herramienta detectada pero no permitida en el flujo: tool={}, allowed={}",
                            toolName, flow.getTools().stream().map(ToolDefinition::getName).toList());
                    continue;
                }
                Map<String, Object> arguments = asMap(parsed.containsKey("parameters")
                        ? parsed.get("parameters")
                        : parsed.get("arguments"));
                result.add(new AiToolCall("call_" + UUID.randomUUID(), toolName, arguments));
                log.info("[PARSE] Llamada a herramienta extraída del contenido: flowId={}, executionId={}, tool={}, arguments={}",
                        flowId, executionId, toolName, arguments);
            } catch (JsonProcessingException ex) {
                log.warn("[PARSE] No se pudo analizar el fragmento JSON: {}, error={}",
                        jsonFragment, ex.getOriginalMessage());
            }
        }
        log.info("[PARSE] Análisis completado: se encontraron {} llamadas a herramientas válidas", result.size());
        return result;
    }

    /**
     * Renderiza un prompt con StringTemplate desde un path {@code classpath:},
     * un texto {@code inline:} o, si está vacío, un volcado de atributos.
     */
    private String renderPrompt(String templatePath, Map<String, Object> attributes) {
        Map<String, Object> model = new HashMap<>();
        model.put("customerId", "");
        model.put("vehicleId", "");
        model.put("salesAgentId", "");
        if (attributes != null) {
            attributes.forEach((key, value) -> model.put(key, value == null ? "" : value));
        }

        if (templatePath == null || templatePath.isBlank()) {
            return model.entrySet().stream()
                    .map(entry -> entry.getKey() + ": " + entry.getValue())
                    .collect(Collectors.joining("\n"));
        }
        if (templatePath.startsWith("inline:")) {
            ST template = new ST(templatePath.substring("inline:".length()));
            model.forEach(template::add);
            return template.render();
        }

        String resourcePath = templatePath.startsWith("classpath:")
                ? templatePath.substring("classpath:".length())
                : templatePath;
        ClassPathResource resource = new ClassPathResource(resourcePath);
        try (var input = resource.getInputStream()) {
            String templateText = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            ST template = new ST(templateText);
            model.forEach(template::add);
            return template.render();
        } catch (IOException ex) {
            throw new IllegalStateException("No se pudo cargar la plantilla de prompt: " + templatePath, ex);
        }
    }

    private Map<String, Object> asMap(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return Map.of();
        }
        Map<String, Object> result = new HashMap<>();
        map.forEach((key, item) -> result.put(String.valueOf(key), item));
        return result;
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("No se pudo serializar el resultado de la herramienta", ex);
        }
    }

    private <T> T withTraceId(Function<String, T> action) {
        String existingTraceId = MDC.get("traceId");
        if (existingTraceId != null && !existingTraceId.isBlank()) {
            return action.apply(existingTraceId);
        }
        return withTraceId(newTraceId(), action);
    }

    private <T> T withTraceId(String traceId, Function<String, T> action) {
        String previousTraceId = MDC.get("traceId");
        MDC.put("traceId", traceId);
        try {
            return action.apply(traceId);
        } finally {
            if (previousTraceId == null) {
                MDC.remove("traceId");
            } else {
                MDC.put("traceId", previousTraceId);
            }
        }
    }

    private String newTraceId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    /** Libera los ejecutores de hilos virtuales de tools y de flujos asíncronos. */
    @PreDestroy
    public void shutdown() {
        toolExecutor.shutdownNow();
        flowExecutor.shutdownNow();
    }
}
