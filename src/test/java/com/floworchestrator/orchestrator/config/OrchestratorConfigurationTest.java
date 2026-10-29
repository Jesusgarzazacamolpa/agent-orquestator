package com.floworchestrator.orchestrator.config;

import com.floworchestrator.orchestrator.service.FlowServiceConfig;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.ollama.OllamaChatModel;
import org.springframework.ai.openai.OpenAiChatModel;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.web.client.RestClient;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class OrchestratorConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(RestClient.Builder.class, RestClient::builder)
            .withBean(WebClient.Builder.class, WebClient::builder)
            .withUserConfiguration(OrchestratorConfiguration.class);

    @Test
    void defaultsToOllamaWithItsModelAndTimeout() {
        runner.run(context -> {
            FlowServiceConfig config = context.getBean(FlowServiceConfig.class);

            assertThat(config.llmProvider()).isEqualTo("ollama");
            assertThat(config.llmModel()).isEqualTo("qwen2.5:7b");
            assertThat(config.llmTimeout()).isEqualTo(Duration.ofSeconds(180));
            assertThat(context.getBean(ChatModel.class)).isInstanceOf(OllamaChatModel.class);
        });
    }

    @Test
    void selectsOpenAiWithItsModelAndTimeout() {
        runner.withPropertyValues(
                        "llm.provider=openai",
                        "openai.api-key=test-key",
                        "openai.model=gpt-4o-mini",
                        "openai.timeout-seconds=45")
                .run(context -> {
                    FlowServiceConfig config = context.getBean(FlowServiceConfig.class);

                    assertThat(config.llmProvider()).isEqualTo("openai");
                    assertThat(config.llmModel()).isEqualTo("gpt-4o-mini");
                    assertThat(config.llmTimeout()).isEqualTo(Duration.ofSeconds(45));
                    assertThat(context.getBean(ChatModel.class)).isInstanceOf(OpenAiChatModel.class);
                });
    }

    @Test
    void failsFastWhenOpenAiHasNoApiKey() {
        runner.withPropertyValues("llm.provider=openai", "openai.api-key=")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseMessage(
                            "llm.provider=openai requiere la API key en openai.api-key (variable OPENAI_API_KEY)");
                });
    }

    @Test
    void rejectsUnknownProvider() {
        runner.withPropertyValues("llm.provider=azure")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseMessage(
                            "Proveedor LLM no soportado: 'azure'. Usa 'ollama' u 'openai' en llm.provider");
                });
    }
}
