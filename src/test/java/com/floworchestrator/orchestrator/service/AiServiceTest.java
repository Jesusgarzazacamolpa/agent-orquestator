package com.floworchestrator.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
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

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AiServiceTest {

    private ChatModel chatModel;
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private RetryRegistry retryRegistry;

    @BeforeEach
    void setUp() {
        chatModel = mock(ChatModel.class);
        circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
        retryRegistry = RetryRegistry.of(RetryConfig.custom()
                .maxAttempts(1)
                .waitDuration(Duration.ZERO)
                .build());
    }

    @Test
    @DisplayName("debe devolver contenido, tool calls y auditar tokens")
    void shouldReturnContentAndAuditTokens() {
        AssistantMessage.ToolCall toolCall = new AssistantMessage.ToolCall(
                "call-1", "function", "get_vehicle_information", "{\"vin\":\"VIN-123\"}");
        when(chatModel.call(any(Prompt.class)))
                .thenReturn(chatResponse("Listo", 12, 7, List.of(toolCall)));

        AiService service = newService(Duration.ofSeconds(2));
        AiService.AiResponse response = service.generateResponse(
                conversation(), List.of(toolSpec()), "trace-1", "compra-vehiculo");

        assertFalse(response.degraded());
        assertEquals("Listo", response.content());
        assertEquals(1, response.toolCalls().size());
        assertEquals("VIN-123", response.toolCalls().getFirst().arguments().get("vin"));
        assertEquals(new AiService.TokenUsage(12, 7), service.getTokenUsage("compra-vehiculo"));
        assertEquals(12, service.getTotalPromptTokens());
        assertEquals(7, service.getTotalGenerationTokens());
    }

    @Test
    @DisplayName("debe enviar las herramientas al modelo sin ejecución interna")
    void shouldExposeToolsToModelWithoutInternalExecution() {
        when(chatModel.call(any(Prompt.class))).thenReturn(chatResponse("ok", 1, 1, List.of()));

        AiService service = newService(Duration.ofSeconds(2));
        service.generateResponse(conversation(), List.of(toolSpec()), "trace-1", "flow");

        ArgumentCaptor<Prompt> promptCaptor = ArgumentCaptor.forClass(Prompt.class);
        verify(chatModel).call(promptCaptor.capture());
        ToolCallingChatOptions options =
                assertInstanceOf(ToolCallingChatOptions.class, promptCaptor.getValue().getOptions());
        assertEquals(1, options.getToolCallbacks().size());
        assertEquals("get_vehicle_information",
                options.getToolCallbacks().getFirst().getToolDefinition().name());
        assertFalse(options.getInternalToolExecutionEnabled());
        assertEquals(2, promptCaptor.getValue().getInstructions().size());
    }

    @Test
    @DisplayName("debe convertir mensajes de herramienta conservando el tool_call_id")
    void shouldMapToolMessagesToSpringAi() {
        when(chatModel.call(any(Prompt.class))).thenReturn(chatResponse("ok", 1, 1, List.of()));

        List<Map<String, Object>> conversation = new java.util.ArrayList<>(conversation());
        conversation.add(Map.of("role", "assistant", "content", "",
                "tool_calls", List.of(Map.of("id", "call-1",
                        "function", Map.of("name", "get_vehicle_information",
                                "arguments", Map.of("vin", "VIN-123"))))));
        conversation.add(Map.of("role", "tool", "tool_name", "get_vehicle_information",
                "tool_call_id", "call-1", "content", "{\"status\":\"OK\"}"));

        AiService service = newService(Duration.ofSeconds(2));
        service.generateResponse(conversation, List.of(toolSpec()), "trace-1", "flow");

        ArgumentCaptor<Prompt> promptCaptor = ArgumentCaptor.forClass(Prompt.class);
        verify(chatModel).call(promptCaptor.capture());
        ToolResponseMessage toolMessage = promptCaptor.getValue().getInstructions().stream()
                .filter(ToolResponseMessage.class::isInstance)
                .map(ToolResponseMessage.class::cast)
                .findFirst()
                .orElseThrow();
        assertEquals("call-1", toolMessage.getResponses().getFirst().id());
        assertEquals("get_vehicle_information", toolMessage.getResponses().getFirst().name());
    }

    @Test
    @DisplayName("debe degradar con flow_code cuando el LLM falla")
    void shouldDegradeWhenLlmFails() {
        when(chatModel.call(any(Prompt.class)))
                .thenThrow(new IllegalStateException("rate limit exceeded"));

        AiService service = newService(Duration.ofSeconds(1));
        AiService.AiResponse response = service.generateResponse(
                conversation(), List.of(toolSpec()), "trace-1", "flow");

        assertTrue(response.degraded());
        assertEquals(AiService.FLOW_CODE_LLM_ERROR, response.flowCode());
        assertTrue(response.userMessage().contains("IA"));
        assertEquals(AiService.DEFAULT_USER_ACTION, response.userAction());
        verify(chatModel, times(1)).call(any(Prompt.class));
    }

    @Test
    @DisplayName("debe degradar cuando el LLM excede el timeout")
    void shouldDegradeOnTimeout() {
        when(chatModel.call(any(Prompt.class))).thenAnswer(invocation -> {
            Thread.sleep(5_000);
            return chatResponse("tarde", 1, 1, List.of());
        });

        AiService service = newService(Duration.ofMillis(100));
        AiService.AiResponse response = assertTimeoutPreemptively(Duration.ofSeconds(2),
                () -> service.generateResponse(conversation(), List.of(toolSpec()), "trace-1", "flow"));

        assertTrue(response.degraded());
        assertEquals(AiService.FLOW_CODE_LLM_ERROR, response.flowCode());
    }

    @Test
    @DisplayName("debe manejar tool calls con argumentos JSON malformados")
    void shouldHandleMalformedToolArguments() {
        AssistantMessage.ToolCall malformed = new AssistantMessage.ToolCall(
                "call-1", "function", "get_vehicle_information", "{not-json");
        when(chatModel.call(any(Prompt.class)))
                .thenReturn(chatResponse("", 1, 1, List.of(malformed)));

        AiService service = newService(Duration.ofSeconds(2));
        AiService.AiResponse response = service.generateResponse(
                conversation(), List.of(toolSpec()), "trace-1", "flow");

        assertFalse(response.degraded());
        assertEquals(1, response.toolCalls().size());
        assertTrue(response.toolCalls().getFirst().arguments().isEmpty());
        assertNotNull(response.toolCalls().getFirst().id());
    }

    @Test
    @DisplayName("debe degradar cuando el LLM devuelve una respuesta vacía")
    void shouldDegradeOnEmptyResponse() {
        when(chatModel.call(any(Prompt.class))).thenReturn(null);

        AiService service = newService(Duration.ofSeconds(1));
        AiService.AiResponse response = service.generateResponse(
                conversation(), List.of(toolSpec()), "trace-1", "flow");

        assertTrue(response.degraded());
    }

    private AiService newService(Duration llmTimeout) {
        return new AiService(chatModel, config(llmTimeout), new ObjectMapper(),
                new PiiMasker(), circuitBreakerRegistry, retryRegistry);
    }

    private FlowServiceConfig config(Duration llmTimeout) {
        return new FlowServiceConfig(
                "http://localhost:8081",
                "ollama",
                "qwen2.5:7b",
                5,
                0,
                Duration.ofSeconds(5),
                5,
                Duration.ofSeconds(5),
                20,
                llmTimeout,
                1,
                Duration.ZERO,
                50,
                Duration.ofSeconds(30),
                Duration.ofSeconds(5)
        );
    }

    private List<Map<String, Object>> conversation() {
        return List.of(
                Map.of("role", "system", "content", "Eres un asistente"),
                Map.of("role", "user", "content", "Consulta el vehículo VIN-123"));
    }

    private AiService.ToolSpec toolSpec() {
        return new AiService.ToolSpec(
                "get_vehicle_information",
                "Consulta información del vehículo",
                Map.of("type", "object",
                        "properties", Map.of("vin", Map.of("type", "string")),
                        "required", List.of("vin")));
    }

    private ChatResponse chatResponse(String content, int promptTokens, int generationTokens,
                                      List<AssistantMessage.ToolCall> toolCalls) {
        AssistantMessage message = new AssistantMessage(content, Map.of(), toolCalls);
        ChatResponseMetadata metadata = ChatResponseMetadata.builder()
                .model("qwen2.5:7b")
                .usage(new DefaultUsage(promptTokens, generationTokens))
                .build();
        return new ChatResponse(List.of(new Generation(message)), metadata);
    }
}
