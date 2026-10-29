package com.floworchestrator.orchestrator.config;

import com.floworchestrator.orchestrator.service.FlowServiceConfig;
import io.micrometer.observation.ObservationRegistry;
import io.netty.channel.ChannelOption;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.model.tool.ToolCallingManager;
import org.springframework.ai.ollama.OllamaChatModel;
import org.springframework.ai.ollama.api.OllamaApi;
import org.springframework.ai.ollama.api.OllamaOptions;
import org.springframework.ai.ollama.management.ModelManagementOptions;
import org.springframework.ai.openai.OpenAiChatModel;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.ai.openai.api.OpenAiApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.client.RestClient;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.Locale;

/**
 * Cablea la infraestructura del orquestador: límites del agente, modelo de chat
 * del proveedor activo (Ollama u OpenAI) y WebClient hacia Control Center con
 * sus timeouts.
 */
@Configuration
public class OrchestratorConfiguration {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorConfiguration.class);
    private static final String PROVIDER_OLLAMA = "ollama";
    private static final String PROVIDER_OPENAI = "openai";

    /**
     * Construye los límites y timeouts del servicio a partir de las propiedades
     * {@code llm.*}, {@code ollama.*}, {@code openai.*}, {@code mcp.*} y
     * {@code services.control-center.*}. Resuelve aquí el proveedor activo para
     * que el resto del servicio solo maneje el modelo y timeout efectivos.
     */
    @Bean
    public FlowServiceConfig flowServiceConfig(
            @Value("${services.control-center.base-url:http://localhost:8081}") String controlCenterBaseUrl,
            @Value("${llm.provider:ollama}") String llmProvider,
            @Value("${ollama.model:qwen2.5:7b}") String ollamaModel,
            @Value("${openai.model:gpt-4o-mini}") String openaiModel,
            @Value("${ollama.max-agent-turns:8}") int maxAgentTurns,
            @Value("${ollama.max-context-messages:20}") int maxContextMessages,
            @Value("${ollama.timeout-seconds:180}") long ollamaTimeoutSeconds,
            @Value("${openai.timeout-seconds:60}") long openaiTimeoutSeconds,
            @Value("${ollama.max-retries:3}") int llmMaxRetries,
            @Value("${ollama.retry-wait-seconds:2}") long llmRetryWaitSeconds,
            @Value("${ollama.circuit-breaker-threshold:50}") int llmCircuitBreakerThreshold,
            @Value("${ollama.circuit-breaker-reset-seconds:60}") long llmCircuitBreakerResetSeconds,
            @Value("${mcp.max-retries:2}") int maxMcpRetries,
            @Value("${mcp.step-timeout-seconds:30}") long stepTimeoutSeconds,
            @Value("${mcp.circuit-breaker-threshold:5}") int mcpCircuitBreakerThreshold,
            @Value("${mcp.circuit-breaker-reset-seconds:60}") long mcpCircuitBreakerResetSeconds,
            @Value("${services.control-center.timeout-seconds:10}") long controlCenterTimeoutSeconds
    ) {
        String provider = normalizeProvider(llmProvider);
        boolean openAi = PROVIDER_OPENAI.equals(provider);
        return new FlowServiceConfig(
                controlCenterBaseUrl,
                provider,
                openAi ? openaiModel : ollamaModel,
                maxAgentTurns,
                maxMcpRetries,
                Duration.ofSeconds(stepTimeoutSeconds),
                mcpCircuitBreakerThreshold,
                Duration.ofSeconds(mcpCircuitBreakerResetSeconds),
                maxContextMessages,
                Duration.ofSeconds(openAi ? openaiTimeoutSeconds : ollamaTimeoutSeconds),
                llmMaxRetries,
                Duration.ofSeconds(llmRetryWaitSeconds),
                llmCircuitBreakerThreshold,
                Duration.ofSeconds(llmCircuitBreakerResetSeconds),
                Duration.ofSeconds(controlCenterTimeoutSeconds)
        );
    }

    /**
     * Crea el modelo de chat del proveedor activo con inferencia determinista y
     * ejecución interna de herramientas desactivada (las tools MCP las ejecuta
     * el orquestador).
     */
    @Bean
    public ChatModel chatModel(
            RestClient.Builder restClientBuilder,
            FlowServiceConfig config,
            @Value("${ollama.base-url:http://localhost:11434}") String ollamaBaseUrl,
            @Value("${ollama.temperature:0.0}") double ollamaTemperature,
            @Value("${ollama.top-p:0.1}") double ollamaTopP,
            @Value("${ollama.keep-alive:30m}") String keepAlive,
            @Value("${ollama.num-predict:256}") int numPredict,
            @Value("${openai.api-key:}") String openaiApiKey,
            @Value("${openai.base-url:https://api.openai.com}") String openaiBaseUrl,
            @Value("${openai.temperature:0.0}") double openaiTemperature,
            @Value("${openai.top-p:0.1}") double openaiTopP) {
        // El timeout del socket debe cortar la llamada HTTP real; cancelar el Future
        // del executor no interrumpe la petición en curso.
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofSeconds(10));
        requestFactory.setReadTimeout(config.llmTimeout().plusSeconds(5));

        boolean openAi = PROVIDER_OPENAI.equals(config.llmProvider());
        log.info("[LLM] Proveedor activo: {}, modelo: {}, endpoint: {}, timeout: {}s",
                config.llmProvider(), config.llmModel(),
                openAi ? openaiBaseUrl : ollamaBaseUrl, config.llmTimeout().toSeconds());

        if (openAi) {
            return openAiChatModel(restClientBuilder, config, requestFactory,
                    openaiApiKey, openaiBaseUrl, openaiTemperature, openaiTopP);
        }
        return ollamaChatModel(restClientBuilder, config, requestFactory,
                ollamaBaseUrl, ollamaTemperature, ollamaTopP, keepAlive, numPredict);
    }

    private ChatModel openAiChatModel(RestClient.Builder restClientBuilder,
            FlowServiceConfig config,
            SimpleClientHttpRequestFactory requestFactory,
            String apiKey,
            String baseUrl,
            double temperature,
            double topP) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException(
                    "llm.provider=openai requiere la API key en openai.api-key (variable OPENAI_API_KEY)");
        }
        OpenAiApi openAiApi = OpenAiApi.builder()
                .baseUrl(baseUrl)
                .apiKey(apiKey)
                .restClientBuilder(restClientBuilder.requestFactory(requestFactory))
                .build();
        OpenAiChatOptions defaultOptions = OpenAiChatOptions.builder()
                .model(config.llmModel())
                .temperature(temperature)
                .topP(topP)
                .build();
        return OpenAiChatModel.builder()
                .openAiApi(openAiApi)
                .defaultOptions(defaultOptions)
                .toolCallingManager(ToolCallingManager.builder().build())
                .observationRegistry(ObservationRegistry.NOOP)
                .build();
    }

    private ChatModel ollamaChatModel(RestClient.Builder restClientBuilder,
            FlowServiceConfig config,
            SimpleClientHttpRequestFactory requestFactory,
            String ollamaBaseUrl,
            double temperature,
            double topP,
            String keepAlive,
            int numPredict) {
        OllamaApi ollamaApi = OllamaApi.builder()
                .baseUrl(ollamaBaseUrl)
                .restClientBuilder(restClientBuilder.requestFactory(requestFactory))
                .build();
        OllamaOptions defaultOptions = OllamaOptions.builder()
                .model(config.llmModel())
                .temperature(temperature)
                .topP(topP)
                .keepAlive(keepAlive)
                .numPredict(numPredict)
                .build();
        return OllamaChatModel.builder()
                .ollamaApi(ollamaApi)
                .defaultOptions(defaultOptions)
                .toolCallingManager(ToolCallingManager.builder().build())
                .observationRegistry(ObservationRegistry.NOOP)
                .modelManagementOptions(ModelManagementOptions.defaults())
                .build();
    }

    private static String normalizeProvider(String provider) {
        String normalized = provider == null ? "" : provider.trim().toLowerCase(Locale.ROOT);
        if (!PROVIDER_OLLAMA.equals(normalized) && !PROVIDER_OPENAI.equals(normalized)) {
            throw new IllegalArgumentException("Proveedor LLM no soportado: '" + provider
                    + "'. Usa 'ollama' u 'openai' en llm.provider");
        }
        return normalized;
    }

    /**
     * Crea el WebClient hacia Control Center con timeout de conexión y de
     * respuesta, usado para resolver plantillas y crear ejecuciones de proceso.
     */
    @Bean
    public WebClient orchestratorWebClient(WebClient.Builder builder,
            @Value("${services.control-center.timeout-seconds:10}") long timeoutSeconds) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5_000)
                .responseTimeout(Duration.ofSeconds(timeoutSeconds));
        return builder.clientConnector(new ReactorClientHttpConnector(httpClient)).build();
    }
}
