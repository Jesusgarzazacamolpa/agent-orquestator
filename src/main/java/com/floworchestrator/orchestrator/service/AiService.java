package com.floworchestrator.orchestrator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.vavr.control.Try;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.ToolResponseMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.model.tool.ToolCallingChatOptions;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.definition.DefaultToolDefinition;
import org.springframework.ai.tool.definition.ToolDefinition;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Frontera de integración con el LLM. Convierte la conversación del orquestador
 * a mensajes de Spring AI, expone las herramientas MCP como tool callbacks
 * (sin ejecución interna) y aplica timeout, reintentos y circuit breaker.
 */
@Service
public class AiService {

    private static final Logger log = LoggerFactory.getLogger(AiService.class);

    static final String FLOW_CODE_LLM_ERROR = "ERR_LLM_01";
    static final String DEFAULT_USER_MESSAGE =
            "La IA está procesando demasiadas solicitudes, intenta en unos segundos";
    static final String DEFAULT_USER_ACTION = "Reintentar";

    private static final String CIRCUIT_BREAKER_NAME = "llm";
    private static final String RETRY_NAME = "llm";
    private static final String TOOL_TYPE = "function";
    private static final Double TEMPERATURE = 0.0;
    private static final Double TOP_P = 0.1;

    private final ChatModel chatModel;
    private final FlowServiceConfig config;
    private final ObjectMapper objectMapper;
    private final PiiMasker piiMasker;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final ExecutorService llmExecutor;

    private final Map<String, TokenUsage> tokenUsageByFlow = new ConcurrentHashMap<>();
    private final AtomicLong totalPromptTokens = new AtomicLong();
    private final AtomicLong totalGenerationTokens = new AtomicLong();

    public AiService(ChatModel chatModel,
                     FlowServiceConfig config,
                     ObjectMapper objectMapper,
                     PiiMasker piiMasker,
                     CircuitBreakerRegistry circuitBreakerRegistry,
                     RetryRegistry retryRegistry) {
        this.chatModel = chatModel;
        this.config = config;
        this.objectMapper = objectMapper;
        this.piiMasker = piiMasker;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.retryRegistry = retryRegistry;
        this.llmExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Ejecuta un turno del agente contra el proveedor configurado (Ollama u
     * OpenAI) y devuelve el texto y las tool calls del modelo; si el LLM falla
     * devuelve una respuesta degradada en lugar de lanzar.
     */
    public AiResponse generateResponse(List<Map<String, Object>> conversation,
                                       List<ToolSpec> availableTools,
                                       String traceId,
                                       String flowId) {
        Prompt prompt = buildPrompt(conversation, availableTools);

        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        Retry retry = retryRegistry.retry(RETRY_NAME);
        Supplier<AiResponse> llmCall = () -> invoke(prompt, flowId, traceId);

        Try<AiResponse> result = Try.ofSupplier(
                CircuitBreaker.decorateSupplier(circuitBreaker,
                        Retry.decorateSupplier(retry, llmCall)));

        if (result.isSuccess()) {
            return result.get();
        }

        Throwable cause = result.getCause();
        log.error("[AI-FAIL] Falló la llamada al LLM: traceId={}, flowId={}, circuitState={}, error={}",
                traceId, flowId, circuitBreaker.getState(), describeCause(cause));
        return AiResponse.degraded(FLOW_CODE_LLM_ERROR, DEFAULT_USER_MESSAGE, DEFAULT_USER_ACTION);
    }

    private AiResponse invoke(Prompt prompt, String flowId, String traceId) {
        long startedAt = System.nanoTime();
        ChatResponse response = callWithTimeout(prompt);
        AssistantMessage output = requireOutput(response);
        String content = output.getText() == null ? "" : output.getText();
        List<AiToolCall> toolCalls = output.getToolCalls().stream()
                .filter(toolCall -> toolCall.name() != null && !toolCall.name().isBlank())
                .map(this::toAiToolCall)
                .toList();
        TokenUsage usage = auditTokens(response, flowId, traceId);

        log.info("[AI] Respuesta del LLM: traceId={}, flowId={}, model={}, toolCalls={}, "
                        + "promptTokens={}, generationTokens={}, durationMs={}, content={}",
                traceId, flowId, config.llmModel(), toolCalls.size(),
                usage.promptTokens(), usage.generationTokens(),
                (System.nanoTime() - startedAt) / 1_000_000, piiMasker.mask(content));

        return AiResponse.success(content, toolCalls);
    }

    /** Arma el prompt desactivando la ejecución interna de tools para que el orquestador decida cuándo invocarlas. */
    private Prompt buildPrompt(List<Map<String, Object>> conversation, List<ToolSpec> availableTools) {
        ToolCallingChatOptions.Builder options = ToolCallingChatOptions.builder()
                .model(config.llmModel())
                .temperature(TEMPERATURE)
                .topP(TOP_P)
                .internalToolExecutionEnabled(false);
        if (availableTools != null && !availableTools.isEmpty()) {
            options.toolCallbacks(toToolCallbacks(availableTools));
        }
        return new Prompt(toMessages(conversation), options.build());
    }

    /**
     * Llama al modelo en un hilo virtual y corta la espera con {@code llmTimeout};
     * el timeout lanza {@link IllegalStateException} para que retry y circuit breaker lo cuenten.
     */
    private ChatResponse callWithTimeout(Prompt prompt) {
        CompletableFuture<ChatResponse> future =
                CompletableFuture.supplyAsync(() -> chatModel.call(prompt), llmExecutor);
        try {
            return future.get(config.llmTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            future.cancel(true);
            throw new IllegalStateException(
                    "La llamada al LLM agotó el tiempo de espera tras " + config.llmTimeout().toSeconds() + "s", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("La llamada al LLM fue interrumpida", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException("La llamada al LLM falló", cause);
        }
    }

    private AssistantMessage requireOutput(ChatResponse response) {
        if (response == null || response.getResult() == null || response.getResult().getOutput() == null) {
            throw new IllegalStateException("El LLM devolvió una respuesta vacía");
        }
        return response.getResult().getOutput();
    }

    /** Traduce el historial propio del orquestador (role/content) a mensajes de Spring AI. */
    private List<Message> toMessages(List<Map<String, Object>> conversation) {
        List<Message> messages = new ArrayList<>();
        if (conversation == null) {
            return messages;
        }
        for (Map<String, Object> rawMessage : conversation) {
            String role = asString(rawMessage.get("role"), "user");
            String content = asString(rawMessage.get("content"), "");
            switch (role) {
                case "system" -> messages.add(new SystemMessage(content));
                case "assistant" -> messages.add(toAssistantMessage(rawMessage, content));
                case "tool" -> messages.add(toToolResponseMessage(rawMessage, content));
                default -> messages.add(new UserMessage(content));
            }
        }
        return messages;
    }

    /** Reconstruye el mensaje assistant con sus tool calls para que el historial enviado al modelo sea válido. */
    private AssistantMessage toAssistantMessage(Map<String, Object> rawMessage, String content) {
        List<AssistantMessage.ToolCall> toolCalls = new ArrayList<>();
        if (rawMessage.get("tool_calls") instanceof List<?> rawCalls) {
            for (Object rawCall : rawCalls) {
                Map<String, Object> call = asMap(rawCall);
                Map<String, Object> function = asMap(call.get("function"));
                String name = asString(function.get("name"), null);
                if (name == null || name.isBlank()) {
                    continue;
                }
                String id = asString(call.get("id"), "call_" + UUID.randomUUID());
                toolCalls.add(new AssistantMessage.ToolCall(
                        id, TOOL_TYPE, name, writeJson(function.get("arguments"))));
            }
        }
        return new AssistantMessage(content, Map.of(), toolCalls);
    }

    private ToolResponseMessage toToolResponseMessage(Map<String, Object> rawMessage, String content) {
        String toolName = asString(rawMessage.get("tool_name"), "tool");
        String toolCallId = asString(rawMessage.get("tool_call_id"), toolName);
        return new ToolResponseMessage(List.of(
                new ToolResponseMessage.ToolResponse(toolCallId, toolName, content)));
    }

    private AiToolCall toAiToolCall(AssistantMessage.ToolCall toolCall) {
        String id = toolCall.id() == null || toolCall.id().isBlank()
                ? "call_" + UUID.randomUUID()
                : toolCall.id();
        return new AiToolCall(id, toolCall.name(), parseArguments(toolCall.arguments(), toolCall.name()));
    }

    /** Convierte los argumentos JSON de una tool call; si vienen malformados devuelve vacío para que el orquestador valide y corrija. */
    private Map<String, Object> parseArguments(String rawArguments, String toolName) {
        if (rawArguments == null || rawArguments.isBlank()) {
            return Map.of();
        }
        try {
            Object parsed = objectMapper.readValue(rawArguments, Object.class);
            if (parsed instanceof Map<?, ?> map) {
                Map<String, Object> arguments = new LinkedHashMap<>();
                map.forEach((key, value) -> arguments.put(String.valueOf(key), value));
                return arguments;
            }
            log.warn("[AI] Los argumentos de la llamada a herramienta no son un objeto JSON: tool={}", toolName);
        } catch (JsonProcessingException ex) {
            log.warn("[AI] Argumentos JSON malformados para la herramienta {}: {}", toolName, ex.getOriginalMessage());
        }
        // Se devuelven argumentos vacíos para que la validación del orquestador
        // devuelva el error al modelo y este pueda corregir la llamada.
        return Map.of();
    }

    private List<ToolCallback> toToolCallbacks(List<ToolSpec> availableTools) {
        List<ToolCallback> callbacks = new ArrayList<>(availableTools.size());
        for (ToolSpec tool : availableTools) {
            callbacks.add(toToolCallback(tool));
        }
        return callbacks;
    }

    /**
     * Expone la tool al modelo solo como esquema: si el LLM intentara ejecutarla
     * internamente, {@code call} falla porque la ejecución pertenece al orquestador.
     */
    private ToolCallback toToolCallback(ToolSpec tool) {
        String inputSchema = tool.inputSchema() == null || tool.inputSchema().isEmpty()
                ? "{\"type\":\"object\",\"properties\":{}}"
                : writeJson(tool.inputSchema());
        ToolDefinition definition = DefaultToolDefinition.builder()
                .name(tool.name())
                .description(tool.description() == null ? "" : tool.description())
                .inputSchema(inputSchema)
                .build();
        return new ToolCallback() {
            @Override
            public ToolDefinition getToolDefinition() {
                return definition;
            }

            @Override
            public String call(String input) {
                throw new IllegalStateException(
                        "Las herramientas MCP debe ejecutarlas el orquestador, no el modelo de chat");
            }
        };
    }

    /** Acumula y registra el consumo de tokens por llamada, por flujo y global. */
    private TokenUsage auditTokens(ChatResponse response, String flowId, String traceId) {
        Usage usage = response.getMetadata() == null ? null : response.getMetadata().getUsage();
        long promptTokens = usage == null || usage.getPromptTokens() == null ? 0 : usage.getPromptTokens();
        long generationTokens = usage == null || usage.getCompletionTokens() == null
                ? 0
                : usage.getCompletionTokens();

        totalPromptTokens.addAndGet(promptTokens);
        totalGenerationTokens.addAndGet(generationTokens);
        TokenUsage callUsage = new TokenUsage(promptTokens, generationTokens);
        TokenUsage flowUsage = tokenUsageByFlow.merge(flowId, callUsage, TokenUsage::plus);

        log.info("[AI-TOKENS] traceId={}, flowId={}, model={}, promptTokens={}, generationTokens={}, "
                        + "callTotalTokens={}, flowTotalTokens={}",
                traceId, flowId, config.llmModel(), promptTokens, generationTokens,
                callUsage.totalTokens(), flowUsage.totalTokens());
        return callUsage;
    }

    /** Devuelve el consumo acumulado de un flujo, o {@code null} si aún no ha llamado al LLM. */
    public TokenUsage getTokenUsage(String flowId) {
        return tokenUsageByFlow.get(flowId);
    }

    /** Total de tokens de prompt consumidos desde el arranque del servicio. */
    public long getTotalPromptTokens() {
        return totalPromptTokens.get();
    }

    /** Total de tokens generados por el modelo desde el arranque del servicio. */
    public long getTotalGenerationTokens() {
        return totalGenerationTokens.get();
    }

    private String writeJson(Object value) {
        if (value == null) {
            return "{}";
        }
        if (value instanceof String text) {
            return text;
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            log.warn("[AI] No se pudieron serializar los argumentos de la herramienta: {}", ex.getOriginalMessage());
            return "{}";
        }
    }

    private Map<String, Object> asMap(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return new HashMap<>();
        }
        Map<String, Object> result = new HashMap<>();
        map.forEach((key, item) -> result.put(String.valueOf(key), item));
        return result;
    }

    private String asString(Object value, String defaultValue) {
        return value == null ? defaultValue : String.valueOf(value);
    }

    private String describeCause(Throwable cause) {
        if (cause == null) {
            return "Error desconocido";
        }
        String message = cause.getMessage();
        return cause.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    /** Cierra el executor de hilos virtuales usado para las llamadas al LLM. */
    @PreDestroy
    public void shutdown() {
        llmExecutor.shutdownNow();
    }

    /**
     * Resultado de un turno del LLM. Un {@code degraded} indica que el proveedor
     * falló y el flujo debe detenerse o cerrarse con un mensaje de respaldo.
     */
    public record AiResponse(String content,
                             List<AiToolCall> toolCalls,
                             boolean degraded,
                             String flowCode,
                             String userMessage,
                             String userAction) {

        public AiResponse {
            content = content == null ? "" : content;
            toolCalls = toolCalls == null
                    ? List.of()
                    : Collections.unmodifiableList(new ArrayList<>(toolCalls));
        }

        /** Respuesta válida del modelo con su contenido y las llamadas a herramientas solicitadas. */
        public static AiResponse success(String content, List<AiToolCall> toolCalls) {
            return new AiResponse(content, toolCalls, false, null, null, null);
        }

        /** Respuesta de respaldo cuando el LLM no está disponible; transporta el código y el mensaje para el usuario. */
        public static AiResponse degraded(String flowCode, String userMessage, String userAction) {
            return new AiResponse("", List.of(), true, flowCode, userMessage, userAction);
        }
    }

    /** Llamada a herramienta solicitada por el modelo, ya con argumentos normalizados a mapa. */
    public record AiToolCall(String id, String name, Map<String, Object> arguments) {

        public AiToolCall {
            arguments = arguments == null
                    ? Map.of()
                    : Collections.unmodifiableMap(new LinkedHashMap<>(arguments));
        }
    }

    /** Herramienta ofrecida al modelo: nombre, descripción y JSON Schema de sus argumentos. */
    public record ToolSpec(String name, String description, Map<String, Object> inputSchema) {
    }

    /** Consumo de tokens de una llamada o de un flujo, con utilidades de suma. */
    public record TokenUsage(long promptTokens, long generationTokens) {

        public long totalTokens() {
            return promptTokens + generationTokens;
        }

        public TokenUsage plus(TokenUsage other) {
            return new TokenUsage(
                    promptTokens + other.promptTokens,
                    generationTokens + other.generationTokens);
        }
    }
}
