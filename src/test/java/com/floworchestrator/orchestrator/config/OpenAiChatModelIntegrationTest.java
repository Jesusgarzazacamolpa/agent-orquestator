package com.floworchestrator.orchestrator.config;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.model.tool.ToolCallingChatOptions;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.definition.DefaultToolDefinition;
import org.springframework.ai.tool.definition.ToolDefinition;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.web.client.RestClient;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class OpenAiChatModelIntegrationTest {

    @Test
    void sendsToolsToOpenAiCompatibleEndpointAndParsesToolCalls() throws Exception {
        AtomicReference<String> requestBody = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requestBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] response = """
                    {
                      "id": "chatcmpl-1",
                      "object": "chat.completion",
                      "created": 1700000000,
                      "model": "gpt-4o-mini",
                      "choices": [{
                        "index": 0,
                        "message": {
                          "role": "assistant",
                          "content": "Consultando",
                          "tool_calls": [{
                            "id": "call-1",
                            "type": "function",
                            "function": {
                              "name": "get_vehicle_information",
                              "arguments": "{\\"vin\\":\\"VIN-1\\"}"
                            }
                          }]
                        },
                        "finish_reason": "tool_calls"
                      }],
                      "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
                    }
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            new ApplicationContextRunner()
                    .withBean(RestClient.Builder.class, RestClient::builder)
                    .withBean(WebClient.Builder.class, WebClient::builder)
                    .withUserConfiguration(OrchestratorConfiguration.class)
                    .withPropertyValues(
                            "llm.provider=openai",
                            "openai.api-key=test-key",
                            "openai.base-url=http://127.0.0.1:" + server.getAddress().getPort())
                    .run(context -> {
                        ChatModel model = context.getBean(ChatModel.class);
                        ChatResponse response = model.call(new Prompt(
                                List.of(new UserMessage("Consulta el vehículo VIN-1")),
                                ToolCallingChatOptions.builder()
                                        .toolCallbacks(vehicleInformationTool())
                                        .internalToolExecutionEnabled(false)
                                        .build()));

                        assertThat(response.getResult().getOutput().getText()).isEqualTo("Consultando");
                        assertThat(response.getResult().getOutput().getToolCalls())
                                .singleElement()
                                .satisfies(call -> {
                                    assertThat(call.name()).isEqualTo("get_vehicle_information");
                                    assertThat(call.arguments()).contains("VIN-1");
                                });
                        assertThat(requestBody.get())
                                .contains("\"model\":\"gpt-4o-mini\"")
                                .contains("\"tools\"");
                    });
        } finally {
            server.stop(0);
        }
    }

    private ToolCallback vehicleInformationTool() {
        ToolDefinition definition = DefaultToolDefinition.builder()
                .name("get_vehicle_information")
                .description("Consulta información del vehículo")
                .inputSchema("""
                        {"type":"object","properties":{"vin":{"type":"string"}},"required":["vin"]}
                        """)
                .build();
        return new ToolCallback() {
            @Override
            public ToolDefinition getToolDefinition() {
                return definition;
            }

            @Override
            public String call(String toolInput) {
                throw new IllegalStateException("La tool la ejecuta el orquestador");
            }
        };
    }
}
