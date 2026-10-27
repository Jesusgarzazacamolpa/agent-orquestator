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

@Service
public class FlowService {
    private static final Logger log = LoggerFactory.getLogger(FlowService.class);

    private static final String ROLE_SYSTEM = "system";
    private static final String ROLE_USER = "user";
    private static final String ROLE_ASSISTANT = "assistant";
    private static final String ROLE_TOOL = "tool";
    private static final String FLOW_CODE_PAUSED = "PAUSED_FOR_HUMAN";
    private static final String FLOW_CODE_FLOW_ERROR = "ERR_FLOW_01";
    private static final String FLOW_CODE_START_ERROR = "ERR_FLOW_START";

    private final WebClient webClient;
    private final AiService aiService;
    private final FlowServiceConfig config;
    private final ObjectMapper objectMapper;
    private final FlowDefinitions flowDefinitions;
    private final McpToolClient mcpClientRegistry;
    private final FlowEventPublisher flowEventPublisher;
    private final FlowStateManager flowStateManager;
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
        this.errorMapper = errorMapper;
        this.piiMasker = piiMasker;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.retryRegistry = retryRegistry;
        this.toolExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.flowExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

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
                log.error("[ASYNC] Flow could not be started: requestId={}, flowId={}, traceId={}, error={}",
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
        log.info("[ASYNC] Flow accepted: requestId={}, flowId={}, executionId={}, traceId={}",
                requestId, request.getFlowId(), request.getExecutionId(), traceId);
        return FlowAsyncAccepted.accepted(requestId, request.getFlowId(),
                request.getExecutionId(), traceId);
    }

    private FlowResponse doCreateFlow(FlowRequest request, String traceId) {
        Map.Entry<String, FlowDefinition> selectedFlow = selectFlow(request.getFlowId());
        String flowId = selectedFlow.getKey();
        FlowDefinition flow = selectedFlow.getValue();
        log.info("[START] Starting flow: flowId={}, traceId={}", flowId, traceId);

        String executionId = resolveExecutionId(flow, request);
        log.info("[ID] Process execution created: flowId={}, executionId={}", flowId, executionId);
        flowEventPublisher.publish(FlowExecutionEvent.of(executionId, flowId, "FLOW_STARTED"));

        FlowResponse response = new FlowResponse();
        response.setId(executionId);
        response.setTraceId(traceId);
        response.setAttributes(request.getAttributes());
        response.setSteps(flow.getTools().stream().map(ToolDefinition::getStepName).toList());

        List<Map<String, Object>> messages = buildPromptMessages(flow, request, flowId);
        AgentLoopState state = new AgentLoopState();
        try {
            log.info("[AGENT] Entering agent loop: flowId={}, executionId={}, maxAgentTurns={}",
                    flowId, executionId, config.maxAgentTurns());
            boolean completed = runAgentLoop(flow, flowId, executionId, response, messages, state, traceId);
            if (!completed) {
                throw new IllegalStateException("Agent exceeded the maximum number of turns");
            }
        } catch (RuntimeException ex) {
            response = handleAgentLoopException(executionId, flowId, flow, messages, state, ex,
                    false, request.getAttributes(), traceId);
        }
        return response;
    }

    public FlowResponse resumeFlow(String executionId, String userInput) {
        return withTraceId(traceId -> doResumeFlow(executionId, userInput, traceId));
    }

    private FlowResponse doResumeFlow(String executionId, String userInput, String traceId) {
        FlowStateManager.PausedFlowState pausedState = flowStateManager.retrieve(executionId);
        if (pausedState == null) {
            throw new IllegalStateException("No se encontró un flujo pausado para executionId: " + executionId);
        }

        String flowId = pausedState.flowId();
        FlowDefinition flow = pausedState.flowDefinition();
        List<Map<String, Object>> messages = new ArrayList<>(pausedState.messages());
        AgentLoopState state = pausedState.agentState();

        if (userInput != null && !userInput.isBlank()) {
            Map<String, Object> userMessage = new HashMap<>();
            userMessage.put("role", ROLE_USER);
            userMessage.put("content", userInput);
            messages.add(userMessage);
            log.info("[RESUME] User input injected: executionId={}, inputLength={}",
                    executionId, userInput.length());
        }

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
                throw new IllegalStateException("Agent exceeded the maximum number of turns");
            }
        } catch (RuntimeException ex) {
            response = handleAgentLoopException(executionId, flowId, flow, messages, state, ex,
                    true, Map.of(), traceId);
        }
        return response;
    }

    public FlowResponse retryStep(String executionId, int stepOrder) {
        return withTraceId(traceId -> doRetryStep(executionId, stepOrder, traceId));
    }

    private FlowResponse doRetryStep(String executionId, int stepOrder, String traceId) {
        FlowStateManager.PausedFlowState pausedState = flowStateManager.retrieve(executionId);
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
        if (!state.completedTools.contains(tool.getName())) {
            throw new IllegalArgumentException(
                    "El paso " + tool.getName() + " no está marcado como completado/fallido");
        }

        state.completedTools.remove(tool.getName());
        state.currentStepOrder = stepOrder;
        state.currentStep = tool.getStepName();

        log.info("[RETRY-STEP] Paso marcado para reintento: executionId={}, stepOrder={}, tool={}, traceId={}",
                executionId, stepOrder, tool.getName(), traceId);

        flowStateManager.store(executionId,
                new FlowStateManager.PausedFlowState(pausedState.flowId(), flow,
                        List.copyOf(pausedState.messages()), state));

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

    private FlowResponse handleAgentLoopException(
            String executionId, String flowId, FlowDefinition flow,
            List<Map<String, Object>> messages, AgentLoopState state,
            RuntimeException ex, boolean isResume,
            Map<String, Object> attributes, String traceId) {
        FlowResponse response = new FlowResponse();
        response.setId(executionId);
        response.setTraceId(traceId);
        response.setSteps(flow.getTools().stream().map(ToolDefinition::getStepName).toList());
        response.setAttributes(attributes == null ? new HashMap<>() : new HashMap<>(attributes));

        if (ex instanceof RequiresHumanInterventionException intervention) {
            String errorMessage = messageOf(intervention);
            String userTitle = "Se requiere información adicional";
            String userMessage = "El agente necesita información adicional para continuar. "
                    + "Proporciona los datos solicitados y reanuda el proceso.";
            String userActionLabel = "Reanudar proceso";
            String userStatusType = "info";
            String logPrefix = isResume ? "[PAUSE] Flujo pausado nuevamente" : "[PAUSE] Flow paused";
            log.warn("{}: executionId={}, step={}, reason={}",
                    logPrefix, executionId, state.currentStep, errorMessage);
            flowEventPublisher.publish(FlowExecutionEvent.withUserAgentError(
                    executionId, flowId, "FLOW_PAUSED", state.currentStepOrder, state.currentStep,
                    null, null, FLOW_CODE_PAUSED, errorMessage,
                    userTitle, userMessage, userActionLabel, userStatusType, FLOW_CODE_PAUSED));
            flowStateManager.store(executionId,
                    new FlowStateManager.PausedFlowState(flowId, flow, List.copyOf(messages), state));
            response.setStatus(FLOW_CODE_PAUSED);
            response.setFlowCode(FLOW_CODE_PAUSED);
            response.setCurrentStep(state.currentStep);
            response.setErrorMessage(errorMessage);
            response.setUserErrorTitle(userTitle);
            response.setUserErrorMessage(userMessage);
            response.setUserErrorActionLabel(userActionLabel);
            response.setUserErrorStatusType(userStatusType);
        } else if (ex instanceof LlmUnavailableException llmUnavailable) {
            String userTitle = "Servicio de IA no disponible";
            log.error("[AI-DEGRADED] Flujo detenido por indisponibilidad del LLM: executionId={}, "
                            + "step={}, flowCode={}, error={}",
                    executionId, state.currentStep, llmUnavailable.getFlowCode(), messageOf(ex));
            flowEventPublisher.publish(FlowExecutionEvent.withUserAgentError(
                    executionId, flowId, "FLOW_FAILED", state.currentStepOrder, state.currentStep,
                    null, null, "FAILED", messageOf(ex),
                    userTitle, llmUnavailable.getUserMessage(), llmUnavailable.getUserAction(),
                    "warning", llmUnavailable.getFlowCode()));
            flowStateManager.store(executionId,
                    new FlowStateManager.PausedFlowState(flowId, flow, List.copyOf(messages), state));
            response.setStatus("FAILED");
            response.setFlowCode(llmUnavailable.getFlowCode());
            response.setCurrentStep(state.currentStep);
            response.setErrorMessage(messageOf(ex));
            response.setUserErrorTitle(userTitle);
            response.setUserErrorMessage(llmUnavailable.getUserMessage());
            response.setUserErrorActionLabel(llmUnavailable.getUserAction());
            response.setUserErrorStatusType("warning");
        } else {
            String errorMessage = messageOf(ex);
            String logPrefix = isResume ? "[FAIL] Flujo falló al reanudar" : "[FAIL] Flow failed";
            log.error("{}: executionId={}, step={}, errorType={}, message={}",
                    logPrefix, executionId, state.currentStep, ex.getClass().getSimpleName(), errorMessage);
            UserFacingErrorMapper.ErrorMapping userError =
                    errorMapper.mapError(state.currentStep, null, errorMessage);
            flowEventPublisher.publish(FlowExecutionEvent.withUserAgentError(
                    executionId, flowId, "FLOW_FAILED", state.currentStepOrder, state.currentStep,
                    null, null, "FAILED", errorMessage,
                    userError.title(), userError.message(), userError.actionLabel(),
                    userError.statusType(), userError.errorCode()));
            flowStateManager.store(executionId,
                    new FlowStateManager.PausedFlowState(flowId, flow, List.copyOf(messages), state));
            response.setStatus("FAILED");
            response.setFlowCode(FLOW_CODE_FLOW_ERROR);
            response.setCurrentStep(state.currentStep);
            response.setErrorMessage(errorMessage);
            response.setUserErrorTitle(userError.title());
            response.setUserErrorMessage(userError.message());
            response.setUserErrorActionLabel(userError.actionLabel());
            response.setUserErrorStatusType(userError.statusType());
        }
        return response;
    }

    private String messageOf(Exception ex) {
        return ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
    }

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
            throw new IllegalStateException("Control Center did not return an active process template ID");
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
            throw new IllegalStateException("Control Center did not return a process execution ID");
        }
        return controlCenterResponse.get("id").toString();
    }

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
        log.info("[CONTEXT] Context window trimmed: kept={} messages (max={})",
                messages.size(), config.maxContextMessages());
    }

    private boolean runAgentLoop(FlowDefinition flow, String flowId, String executionId,
                                 FlowResponse response, List<Map<String, Object>> messages,
                                 AgentLoopState state, String traceId) {
        for (int turn = 0; turn < config.maxAgentTurns(); turn++) {
            log.info("[AGENT] Turn {}/{} starting: flowId={}, executionId={}",
                    turn + 1, config.maxAgentTurns(), flowId, executionId);
            trimContextIfNeeded(messages);

            List<ToolSpec> toolSpecs = getToolSpecs(flow, state);
            log.info("[TOOLS] Offering {} tools to LLM: flowId={}, executionId={}, tools={}",
                    toolSpecs.size(), flowId, executionId,
                    toolSpecs.stream().map(ToolSpec::name).toList());

            AiResponse aiResponse = aiService.generateResponse(messages, toolSpecs, traceId, flowId);
            if (aiResponse.degraded()) {
                // Si todos los pasos de negocio ya se ejecutaron, el fallo del turno
                // final (resumen) no debe marcar el flujo como fallido.
                if (state.completedTools.containsAll(getRequiredTools(flow))) {
                    String fallbackMessage = completionFallbackMessage(flow, state.completedTools);
                    log.warn("[AI-DEGRADED] LLM unavailable after all required steps completed; "
                                    + "closing flow with fallback summary: flowId={}, executionId={}",
                            flowId, executionId);
                    completeFlow(flow, flowId, executionId, response, fallbackMessage,
                            state.completedTools);
                    return true;
                }
                throw new LlmUnavailableException(
                        "LLM unavailable for flow " + flowId,
                        aiResponse.flowCode(), aiResponse.userMessage(), aiResponse.userAction());
            }

            String content = aiResponse.content();
            log.info("[AI] Ollama response received: flowId={}, executionId={}, content={}",
                    flowId, executionId, piiMasker.mask(content));

            List<AiToolCall> toolCalls = aiResponse.toolCalls();
            if (toolCalls.isEmpty()) {
                toolCalls = parseToolCallsFromContent(content, flow, flowId, executionId);
            }
            messages.add(assistantMessage(content, toolCalls));

            if (toolCalls.isEmpty()) {
                if (isRequestingUserInput(content)) {
                    log.info("[USER_INPUT] LLM requesting user input: flowId={}, executionId={}",
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

    private String completionFallbackMessage(FlowDefinition flow, Set<String> completedTools) {
        return "El flujo " + flow.getProcessTemplateName() + " se completó correctamente ("
                + completedTools.size() + " pasos ejecutados).";
    }

    private void completeFlow(FlowDefinition flow, String flowId, String executionId,
                              FlowResponse response, String content, Set<String> completedTools) {
        Set<String> requiredTools = getRequiredTools(flow);
        if (!completedTools.containsAll(requiredTools)) {
            throw new IllegalStateException("Agent finished before completing all required tools: "
                    + difference(requiredTools, completedTools));
        }
        log.info("[DONE] Flow completed: flowId={}, executionId={}", flowId, executionId);
        flowEventPublisher.publish(FlowExecutionEvent.withError(
                executionId, flowId, "FLOW_COMPLETED", null, null, null, null,
                "COMPLETED", null));
        response.setStatus("COMPLETED");
        response.setMessage(content);
    }

    private void processToolCalls(FlowDefinition flow, String flowId, String executionId,
                                  List<Map<String, Object>> messages, AgentLoopState state,
                                  List<AiToolCall> toolCalls) {
        for (AiToolCall toolCall : toolCalls) {
            String toolName = toolCall.name();
            log.info("[TOOLS] Processing tool call: flowId={}, executionId={}, toolName={}, toolCallId={}",
                    flowId, executionId, toolName, toolCall.id());
            ToolDefinition tool;
            try {
                tool = findTool(flow, toolName);
            } catch (IllegalArgumentException ex) {
                log.warn("[BLOCKED] Agent requested a tool not allowed by the flow: "
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
                log.warn("[RETRY] Agent tool call rejected: flowId={}, executionId={}, tool={}, reason={}",
                        flowId, executionId, toolName, ex.getMessage());
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

    private void resetCurrentStep(AgentLoopState state) {
        state.currentStepOrder = 0;
        state.currentStep = null;
    }

    private boolean executeToolStep(String flowId, String executionId,
                                    List<Map<String, Object>> messages, AgentLoopState state,
                                    ToolDefinition tool, AiToolCall toolCall) {
        String toolName = tool.getName();
        log.info("[CALL] Agent requested MCP tool: flowId={}, executionId={}, server={}, tool={}, step={}",
                flowId, executionId, tool.getServer(), toolName, state.currentStep);
        long toolStartedAt = System.nanoTime();
        try {
            Map<String, Object> result = executeToolWithRetry(tool, toolCall.arguments());
            if (isInterventionRequired(result)) {
                throw new RequiresHumanInterventionException(extractInterventionReason(result));
            }
            state.toolResults.put(toolName, result);
            state.completedTools.add(toolName);
            flowEventPublisher.publish(FlowExecutionEvent.withError(
                    executionId, flowId, "STEP_COMPLETED", state.currentStepOrder, state.currentStep,
                    toolName, tool.getServer(), "COMPLETED", null));
            log.info("[OK] MCP tool completed: flowId={}, executionId={}, tool={}, durationMs={}",
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
            String toolError = messageOf(ex);
            log.warn("[FAIL] MCP tool failed, adding error to context for agent retry: "
                            + "flowId={}, executionId={}, tool={}, error={}",
                    flowId, executionId, toolName, toolError);
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

    static class AgentLoopState {
        private final Set<String> completedTools = new HashSet<>();
        private final Map<String, Map<String, Object>> toolResults = new HashMap<>();
        private final Map<String, List<DiscoveredTool>> discoveredByServer = new HashMap<>();
        private int currentStepOrder = 0;
        private String currentStep = null;

        AgentStateSnapshot snapshot() {
            // discoveredByServer se omite a propósito: al reanudar se redescubren
            // las herramientas para no reutilizar esquemas obsoletos.
            return new AgentStateSnapshot(
                    new HashSet<>(completedTools),
                    new HashMap<>(toolResults),
                    currentStepOrder,
                    currentStep);
        }

        static AgentLoopState fromSnapshot(AgentStateSnapshot snapshot) {
            AgentLoopState state = new AgentLoopState();
            if (snapshot.completedTools() != null) {
                state.completedTools.addAll(snapshot.completedTools());
            }
            if (snapshot.toolResults() != null) {
                state.toolResults.putAll(snapshot.toolResults());
            }
            state.currentStepOrder = snapshot.currentStepOrder();
            state.currentStep = snapshot.currentStep();
            return state;
        }
    }

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

    private DiscoveredTool discoverTool(String server, String toolName, AgentLoopState state) {
        List<DiscoveredTool> discoveredTools = state.discoveredByServer.computeIfAbsent(
                server, serverName -> {
                    List<DiscoveredTool> tools = mcpClientRegistry.listTools(serverName);
                    log.info("[DISCOVER] MCP tools discovered: server={}, tools={}", serverName,
                            tools.stream().map(DiscoveredTool::name).toList());
                    return tools;
                });
        return discoveredTools.stream()
                .filter(candidate -> candidate.name().equals(toolName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("MCP tool is not available: " + toolName));
    }

    private Set<String> getRequiredTools(FlowDefinition flow) {
        return flow.getTools().stream()
                .filter(ToolDefinition::isRequired)
                .map(ToolDefinition::getName)
                .collect(Collectors.toSet());
    }

    private void validateToolCall(ToolDefinition tool,
                                  Map<String, Object> modelArguments,
                                  AgentLoopState state) {
        for (String dependency : tool.getDependsOn()) {
            if (!state.completedTools.contains(dependency)) {
                throw new IllegalArgumentException("Required tool has not completed: " + dependency);
            }
        }
        DiscoveredTool discoveredTool = discoverTool(tool.getServer(), tool.getName(), state);
        validateArguments(discoveredTool, modelArguments);
    }

    private Map<String, Object> executeTool(ToolDefinition tool, Map<String, Object> arguments) {
        return mcpClientRegistry.callTool(tool.getServer(), tool.getName(), arguments);
    }

    private Map<String, Object> executeToolWithRetry(ToolDefinition tool, Map<String, Object> arguments) {
        CircuitBreaker circuitBreaker = circuitBreakerFor(tool.getServer());
        Retry retry = retryFor(tool.getServer());

        Supplier<Map<String, Object>> supplier = () -> {
            try {
                return callToolWithTimeout(tool, arguments);
            } catch (TimeoutException e) {
                throw new IllegalStateException("MCP tool timeout: " + tool.getName(), e);
            }
        };

        Try<Map<String, Object>> result = Try.ofSupplier(
                CircuitBreaker.decorateSupplier(circuitBreaker,
                        Retry.decorateSupplier(retry, supplier)));

        return result.get();
    }

    private CircuitBreaker circuitBreakerFor(String server) {
        return circuitBreakers.computeIfAbsent(server, name ->
                circuitBreakerRegistry.circuitBreaker("mcp-" + name, CircuitBreakerConfig.custom()
                        .failureRateThreshold(config.mcpCircuitBreakerThreshold())
                        .slidingWindowSize(10)
                        .minimumNumberOfCalls(5)
                        .waitDurationInOpenState(config.mcpCircuitBreakerResetTimeout())
                        .build())
        );
    }

    private Retry retryFor(String server) {
        return retries.computeIfAbsent(server, name ->
                retryRegistry.retry("mcp-" + name, RetryConfig.custom()
                        .maxAttempts(config.maxMcpRetries() + 1)
                        .intervalFunction(IntervalFunction.ofExponentialBackoff(
                                Duration.ofSeconds(1), 2.0))
                        .retryExceptions(RuntimeException.class, TimeoutException.class)
                        .build())
        );
    }

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
            throw new IllegalStateException("Interrupted while calling MCP tool: " + tool.getName(), ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException("MCP tool failed: " + tool.getName(), cause);
        }
    }

    private boolean isInterventionRequired(Map<String, Object> result) {
        Object value = result.get("requiresIntervention");
        return value instanceof Boolean bool && bool;
    }

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
                        "Argument is not allowed for MCP tool " + tool.name() + ": " + argument
                                + ". Allowed arguments: " + allowedArguments);
            }
        }
        Object requiredValue = tool.inputSchema().get("required");
        if (requiredValue instanceof List<?> required) {
            for (Object argument : required) {
                if (arguments.get(String.valueOf(argument)) == null) {
                    throw new IllegalArgumentException(
                            "Missing argument for MCP tool " + tool.name() + ": " + argument
                                    + ". Allowed arguments: " + allowedArguments);
                }
            }
        }
    }

    private Map.Entry<String, FlowDefinition> selectFlow(String requestedFlowId) {
        if (requestedFlowId == null || requestedFlowId.isBlank()) {
            throw new IllegalArgumentException("flowId is required");
        }
        String normalizedFlowId = requestedFlowId.trim();
        Map.Entry<String, FlowDefinition> selectedFlow = flowDefinitions.getFlows().entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(normalizedFlowId)
                        || entry.getValue().getProcessTemplateName().equalsIgnoreCase(normalizedFlowId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown flowId or process template name: " + requestedFlowId
                                + ". Available flowIds: " + flowDefinitions.getFlows().keySet()));
        validateFlow(selectedFlow.getValue());
        return selectedFlow;
    }

    private void validateFlow(FlowDefinition flow) {
        if (flow.getProcessTemplateName() == null || flow.getProcessTemplateName().isBlank()
                || flow.getSystemPromptTemplate() == null || flow.getSystemPromptTemplate().isBlank()
                || flow.getUserPromptTemplate() == null || flow.getUserPromptTemplate().isBlank()
                || flow.getTools().isEmpty()) {
            throw new IllegalStateException("Flow configuration is incomplete");
        }
        Set<String> ids = new HashSet<>();
        Set<Integer> stepOrders = new HashSet<>();
        for (ToolDefinition tool : flow.getTools()) {
            if (tool.getName() == null || !ids.add(tool.getName())
                    || tool.getServer() == null || !flowDefinitions.getMcpServers().containsKey(tool.getServer())
                    || tool.getStepOrder() < 1 || !stepOrders.add(tool.getStepOrder())
                    || tool.getStepName() == null || tool.getStepName().isBlank()) {
                throw new IllegalStateException("Invalid or duplicate MCP tool configuration in flow");
            }
        }
    }

    private ToolDefinition findTool(FlowDefinition flow, String id) {
        return flow.getTools().stream()
                .filter(tool -> tool.getName().equals(id))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Tool is not allowed in this flow: " + id));
    }

    private Set<String> difference(Set<String> required, Set<String> completed) {
        Set<String> missing = new HashSet<>(required);
        missing.removeAll(completed);
        return missing;
    }

    private List<AiToolCall> parseToolCallsFromContent(String content, FlowDefinition flow,
                                                       String flowId, String executionId) {
        List<AiToolCall> result = new ArrayList<>();
        if (content == null || content.isBlank()) {
            return result;
        }

        log.debug("[PARSE] Attempting to parse tool calls from content: flowId={}, content={}",
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
                    log.warn("[PARSE] Tool call parsed but not allowed in flow: tool={}, allowed={}",
                            toolName, flow.getTools().stream().map(ToolDefinition::getName).toList());
                    continue;
                }
                Map<String, Object> arguments = asMap(parsed.containsKey("parameters")
                        ? parsed.get("parameters")
                        : parsed.get("arguments"));
                result.add(new AiToolCall("call_" + UUID.randomUUID(), toolName, arguments));
                log.info("[PARSE] Tool call parsed from content: flowId={}, executionId={}, tool={}, arguments={}",
                        flowId, executionId, toolName, arguments);
            } catch (JsonProcessingException ex) {
                log.warn("[PARSE] Failed to parse JSON fragment: {}, error={}",
                        jsonFragment, ex.getOriginalMessage());
            }
        }
        log.info("[PARSE] Parsing complete: found={} valid tool calls", result.size());
        return result;
    }

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
            throw new IllegalStateException("Could not load prompt template: " + templatePath, ex);
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
            throw new IllegalStateException("Could not serialize tool result", ex);
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

    @PreDestroy
    public void shutdown() {
        toolExecutor.shutdownNow();
        flowExecutor.shutdownNow();
    }
}
