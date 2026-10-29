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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.client.RestClient;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

/**
 * Cablea la infraestructura del orquestador: límites del agente, cliente de
 * Ollama y WebClient hacia Control Center con sus timeouts.
 */
@Configuration
public class OrchestratorConfiguration {

    /**
     * Construye los límites y timeouts del servicio a partir de las propiedades
     * {@code ollama.*}, {@code mcp.*} y {@code services.control-center.*}.
     */
    @Bean
    public FlowServiceConfig flowServiceConfig(
            @Value("${services.control-center.base-url:http://localhost:8081}") String controlCenterBaseUrl,
            @Value("${ollama.base-url:http://localhost:11434}") String ollamaBaseUrl,
            @Value("${ollama.model:qwen2.5:7b}") String ollamaModel,
            @Value("${ollama.max-agent-turns:8}") int maxAgentTurns,
            @Value("${ollama.max-context-messages:20}") int maxContextMessages,
            @Value("${ollama.timeout-seconds:60}") long llmTimeoutSeconds,
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
        return new FlowServiceConfig(
                controlCenterBaseUrl,
                ollamaBaseUrl,
                ollamaModel,
                maxAgentTurns,
                maxMcpRetries,
                Duration.ofSeconds(stepTimeoutSeconds),
                mcpCircuitBreakerThreshold,
                Duration.ofSeconds(mcpCircuitBreakerResetSeconds),
                maxContextMessages,
                Duration.ofSeconds(llmTimeoutSeconds),
                llmMaxRetries,
                Duration.ofSeconds(llmRetryWaitSeconds),
                llmCircuitBreakerThreshold,
                Duration.ofSeconds(llmCircuitBreakerResetSeconds),
                Duration.ofSeconds(controlCenterTimeoutSeconds)
        );
    }

    /**
     * Crea el modelo de chat de Ollama con inferencia determinista y ejecución
     * interna de herramientas desactivada (las tools MCP las ejecuta el orquestador).
     */
    @Bean
    public ChatModel chatModel(
            RestClient.Builder restClientBuilder,
            @Value("${ollama.base-url:http://localhost:11434}") String ollamaBaseUrl,
            @Value("${ollama.model:qwen2.5:7b}") String ollamaModel,
            @Value("${ollama.temperature:0.0}") double temperature,
            @Value("${ollama.top-p:0.1}") double topP,
            @Value("${ollama.keep-alive:30m}") String keepAlive,
            @Value("${ollama.num-predict:256}") int numPredict,
            @Value("${ollama.timeout-seconds:120}") long llmTimeoutSeconds) {
        // El timeout del socket debe cortar la llamada HTTP real; cancelar el Future
        // del executor no interrumpe la petición en curso.
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofSeconds(10));
        requestFactory.setReadTimeout(Duration.ofSeconds(llmTimeoutSeconds).plusSeconds(5));

        OllamaApi ollamaApi = OllamaApi.builder()
                .baseUrl(ollamaBaseUrl)
                .restClientBuilder(restClientBuilder.requestFactory(requestFactory))
                .build();
        OllamaOptions defaultOptions = OllamaOptions.builder()
                .model(ollamaModel)
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
