package com.floworchestrator.orchestrator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.floworchestrator.orchestrator.config.FlowDefinitions;
import com.floworchestrator.orchestrator.config.FlowDefinitions.FlowDefinition;
import com.floworchestrator.orchestrator.config.FlowDefinitions.ToolDefinition;
import com.floworchestrator.orchestrator.model.FlowRequest;
import com.floworchestrator.orchestrator.model.FlowResponse;
import com.floworchestrator.orchestrator.service.McpToolClient.DiscoveredTool;
import org.stringtemplate.v4.ST;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class FlowService {
    private static final Logger log = LoggerFactory.getLogger(FlowService.class);

    private final WebClient webClient;
    private final String controlCenterBaseUrl;
    private final String ollamaBaseUrl;
    private final String ollamaModel;
    private final int maxAgentTurns;
    private final ObjectMapper objectMapper;
    private final FlowDefinitions flowDefinitions;
    private final McpToolClient mcpClientRegistry;

    /**
     * Constructor que inicializa el servicio de orquestación de flujos.
     *
     * @param webClientBuilder      Constructor de WebClient para realizar llamadas HTTP.
     * @param controlCenterBaseUrl  URL base del servicio Control Center.
     * @param ollamaBaseUrl         URL base del servicio Ollama (IA).
     * @param ollamaModel           Modelo de IA a utilizar (ej. llama3.1).
     * @param maxAgentTurns         Número máximo de turnos que puede tomar el agente.
     * @param objectMapper          Objeto para serialización/deserialización JSON.
     */
    public FlowService(
            WebClient.Builder webClientBuilder,
            @Value("${services.control-center.base-url:http://localhost:8081}") String controlCenterBaseUrl,
            @Value("${ollama.base-url:http://localhost:11434}") String ollamaBaseUrl,
            @Value("${ollama.model:llama3.1}") String ollamaModel,
            @Value("${ollama.max-agent-turns:8}") int maxAgentTurns,
            ObjectMapper objectMapper,
            FlowDefinitions flowDefinitions,
            McpToolClient mcpClientRegistry
    ) {
        this.webClient = webClientBuilder.build();
        this.controlCenterBaseUrl = controlCenterBaseUrl;
        this.ollamaBaseUrl = ollamaBaseUrl;
        this.ollamaModel = ollamaModel;
        this.maxAgentTurns = maxAgentTurns;
        this.objectMapper = objectMapper;
        this.flowDefinitions = flowDefinitions;
        this.mcpClientRegistry = mcpClientRegistry;
    }

    /**
     * Ejecuta un flujo de compra de vehículo coordinado por un agente de IA.
     * <p>
    * El método realiza los siguientes pasos:
     * <ol>
    *   <li>Obtiene la plantilla de flujo seleccionada desde el Control Center.</li>
     *   <li>Crea una ejecución de flujo en el Control Center.</li>
     *   <li>Invoca al agente de IA (Ollama) para que coordine el uso de herramientas.</li>
    *   <li>Descubre y ejecuta por MCP las herramientas permitidas para el flujo.</li>
     *   <li>Actualiza el estado de la ejecución y los pasos en el Control Center.</li>
     * </ol>
     *
     * @param request Solicitud del flujo con datos del cliente, vehículo y agente.
     * @return {@link FlowResponse} con el resultado de la ejecución del flujo.
     */
    public FlowResponse createFlow(FlowRequest request) {
        String requestedFlowId = request.getFlowId();
        if (requestedFlowId == null || requestedFlowId.isBlank()) {
            throw new IllegalArgumentException("flowId is required");
        }
        String normalizedFlowId = requestedFlowId.trim();
        Map.Entry<String, FlowDefinition> selectedFlow = flowDefinitions.getFlows().entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(normalizedFlowId)
                        || entry.getValue().getProcessTemplateName().equalsIgnoreCase(normalizedFlowId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown flowId or process template name: " + requestedFlowId +
                                ". Available flowIds: " + flowDefinitions.getFlows().keySet()));
        String flowId = selectedFlow.getKey();
        FlowDefinition flow = selectedFlow.getValue();
        validateFlow(flow);
        log.info("[START] Starting flow: flowId={}", flowId);

        Map<String, Object> templateResponse = webClient.get()
                .uri(UriComponentsBuilder.fromUriString(controlCenterBaseUrl + "/api/process-templates/by-name")
                        .queryParam("name", flow.getProcessTemplateName())
                        .build()
                        .encode()
                        .toUri())
                .retrieve()
                .bodyToMono(Map.class)
                .block();
        if (templateResponse == null || templateResponse.get("id") == null) {
            throw new IllegalStateException("Control Center did not return an active process template ID");
        }

        Map<String, Object> controlCenterRequest = new HashMap<>();
        controlCenterRequest.put("processTemplateId", templateResponse.get("id"));
        controlCenterRequest.put("customerId", request.getCustomerId());

        Map<String, Object> controlCenterResponse = webClient.post()
                .uri(controlCenterBaseUrl + "/api/processes")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(controlCenterRequest)
                .retrieve()
                .bodyToMono(Map.class)
                .block();
        if (controlCenterResponse == null || controlCenterResponse.get("id") == null) {
            throw new IllegalStateException("Control Center did not return a process execution ID");
        }

        String executionId = controlCenterResponse.get("id").toString();
        log.info("[ID] Process execution created: flowId={}, executionId={}", flowId, executionId);
        FlowResponse response = new FlowResponse();
        response.setId(executionId);
        response.setCustomerId(request.getCustomerId());
        response.setVehicleId(request.getVehicleId());
        response.setSalesAgentId(request.getSalesAgentId());
        response.setSteps(flow.getTools().stream().map(ToolDefinition::getStepName).toList());

        List<Map<String, Object>> messages = new ArrayList<>();
        Map<String, Object> promptAttributes = new HashMap<>();
        promptAttributes.put("flowId", flowId);
        promptAttributes.put("flowName", flow.getProcessTemplateName());
        promptAttributes.put("customerId", request.getCustomerId());
        promptAttributes.put("vehicleId", request.getVehicleId());
        promptAttributes.put("salesAgentId", request.getSalesAgentId());
        promptAttributes.put("toolNames", flow.getTools().stream().map(ToolDefinition::getName).toList());
        messages.add(Map.of(
                "role", "system",
                "content", renderPrompt(flow.getSystemPromptTemplate(), promptAttributes)
        ));
        messages.add(Map.of(
                "role", "user",
                "content", renderPrompt(flow.getUserPromptTemplate(), promptAttributes)
        ));

        Set<String> completedTools = new HashSet<>();
        Map<String, Map<String, Object>> toolResults = new HashMap<>();
        Map<String, List<DiscoveredTool>> discoveredByServer = new HashMap<>();
        int currentStepOrder = 0;
        String currentStep = null;

        try {
            log.info("[info] Entering agent loop: flowId={}, executionId={}, maxAgentTurns={}",
                    flowId, executionId, maxAgentTurns);
            List<Map<String, Object>> availableTools;
            for (int turn = 0; turn < maxAgentTurns; turn++) {
                log.info("[info] Turn {} starting: flowId={}, executionId={}", turn + 1, flowId, executionId);
                log.info("[info] Calling getToolDefinitions: flowId={}, executionId={}, completedTools={}",
                        flowId, executionId, completedTools);
                availableTools = getToolDefinitions(flow, completedTools, discoveredByServer);
                log.info("[info] getToolDefinitions returned {} tools: flowId={}, executionId={}, tools={}",
                        availableTools.size(), flowId, executionId,
                        availableTools.stream().map(t -> String.valueOf(((Map<?, ?>) t.get("function")).get("name"))).toList());
                log.info("[AGENT] Requesting decision: flowId={}, executionId={}, turn={}/{}",
                        flowId, executionId, turn + 1, maxAgentTurns);
                log.info("[info] Calling Ollama: flowId={}, executionId={}, ollamaBaseUrl={}, model={}",
                        flowId, executionId, ollamaBaseUrl, ollamaModel);
                Map<String, Object> ollamaRequest = new HashMap<>();
                ollamaRequest.put("model", ollamaModel);
                ollamaRequest.put("messages", messages);
                ollamaRequest.put("tools", availableTools);
                ollamaRequest.put("stream", false);
                ollamaRequest.put("tool_choice", "required");
                ollamaRequest.put("options", Map.of("temperature", 0.0, "top_p", 0.1));
                Map<String, Object> ollamaResponse = webClient.post()
                        .uri(ollamaBaseUrl + "/api/chat")
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(ollamaRequest)
                        .retrieve()
                        .bodyToMono(Map.class)
                        .block();
                log.info("[info] Ollama response received: flowId={}, executionId={}, response={}",
                        flowId, executionId, ollamaResponse);
                Map<String, Object> assistantMessage = asMap(ollamaResponse == null ? null : ollamaResponse.get("message"));
                if (assistantMessage.isEmpty()) {
                    throw new IllegalStateException("Ollama returned an empty assistant message");
                }
                messages.add(assistantMessage);

                List<?> toolCalls = assistantMessage.get("tool_calls") instanceof List<?> calls ? calls : List.of();
                if (toolCalls.isEmpty()) {
                    // Intentar parsear tool calls embebidas como texto en content
                    String content = String.valueOf(assistantMessage.getOrDefault("content", ""));
                    List<Map<String, Object>> parsedToolCalls = parseToolCallsFromContent(content, flow, flowId, executionId);
                    if (!parsedToolCalls.isEmpty()) {
                        log.info("[PARSE] Tool calls parsed from content: flowId={}, executionId={}, toolCalls={}",
                                flowId, executionId, parsedToolCalls);
                        toolCalls = parsedToolCalls;
                    } else {
                        log.warn("[NO_TOOLS] Ollama response without tool_calls and no parseable content: flowId={}, executionId={}, content={}",
                                flowId, executionId, content);
                    }
                } else {
                    log.info("[TOOLS] Ollama returned tool_calls directly: flowId={}, executionId={}, toolCalls={}",
                            flowId, executionId, toolCalls);
                }
                if (toolCalls.isEmpty()) {
                    Set<String> requiredTools = getRequiredTools(flow);
                    if (!completedTools.containsAll(requiredTools)) {
                        throw new IllegalStateException("Agent finished before completing all required tools: " +
                                difference(requiredTools, completedTools));
                    }
                    updateExecutionStatus(executionId, "COMPLETED", null, null);
                    log.info("[DONE] Flow update status completed: flowId={}, executionId={}", flowId, executionId);
                    response.setStatus("COMPLETED");
                    response.setMessage(String.valueOf(assistantMessage.getOrDefault("content", "")));
                    log.info("[DONE] Flow completed: flowId={}, executionId={}", flowId, executionId);
                    return response;
                }

                for (Object rawToolCall : toolCalls) {
                    Map<String, Object> toolCall = asMap(rawToolCall);
                    Map<String, Object> function = asMap(toolCall.get("function"));
                    String toolName = String.valueOf(function.get("name"));
                    log.info("[info] Processing tool call: flowId={}, executionId={}, toolName={}",
                            flowId, executionId, toolName);
                    ToolDefinition tool;
                    try {
                        tool = findTool(flow, toolName);
                    } catch (IllegalArgumentException ex) {
                        log.warn("[BLOCKED] Agent requested a tool not allowed by the flow: flowId={}, executionId={}, tool={}",
                                flowId, executionId, toolName);
                        addToolErrorMessage(messages, toolName, ex.getMessage());
                        continue;
                    }

                    Map<String, Object> arguments = asMap(function.get("arguments"));
                    try {
                        validateToolCall(tool, arguments, completedTools);
                    } catch (IllegalArgumentException ex) {
                        log.warn("[RETRY] Agent tool call rejected: flowId={}, executionId={}, tool={}, reason={}",
                            flowId, executionId, toolName, ex.getMessage());
                        addToolErrorMessage(messages, toolName, ex.getMessage());
                        continue;
                    }

                    if (!completedTools.contains(toolName)) {
                        currentStepOrder = tool.getStepOrder();
                        currentStep = tool.getStepName();
                        log.info("[CALL] Agent requested MCP tool: flowId={}, executionId={}, server={}, tool={}, step={}",
                                flowId, executionId, tool.getServer(), toolName, currentStep);
                        updateStepStatus(executionId, currentStepOrder, "RUNNING", null);
                        long toolStartedAt = System.nanoTime();
                        Map<String, Object> result = executeTool(tool, arguments);
                        toolResults.put(toolName, result);
                        completedTools.add(toolName);
                        updateStepStatus(executionId, currentStepOrder, "COMPLETED", null);
                        log.info("[OK] MCP tool completed: flowId={}, executionId={}, tool={}, durationMs={}",
                                flowId, executionId, toolName,
                                (System.nanoTime() - toolStartedAt) / 1_000_000);
                    }

                    Map<String, Object> toolMessage = new HashMap<>();
                    toolMessage.put("role", "tool");
                    toolMessage.put("tool_name", toolName);
                    toolMessage.put("content", writeJson(toolResults.get(toolName)));
                    messages.add(toolMessage);
                    currentStepOrder = 0;
                    currentStep = null;
                }
            }
            throw new IllegalStateException("Agent exceeded the maximum number of turns");
        } catch (RuntimeException ex) {
            String errorMessage = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
            log.error("[FAIL] Flow failed: flowId={}, executionId={}, step={}, errorType={}, message={}",
                    flowId, executionId, currentStep, ex.getClass().getSimpleName(), errorMessage);
            try {
                if (currentStepOrder > 0) {
                    updateStepStatus(executionId, currentStepOrder, "FAILED", errorMessage);
                } else {
                    updateExecutionStatus(executionId, "FAILED", currentStep, errorMessage);
                }
            } catch (RuntimeException statusUpdateException) {
                ex.addSuppressed(statusUpdateException);
            }
            response.setStatus("FAILED");
            response.setCurrentStep(currentStep);
            response.setErrorMessage(errorMessage);
        }

        return response;
    }

    /**
     * Obtiene la lista de definiciones de herramientas (functions) disponibles para el agente de IA.
     *
     * @return Lista de mapas con la estructura de cada herramienta.
     */
    private List<Map<String, Object>> getToolDefinitions(
            FlowDefinition flow,
            Set<String> completedTools,
            Map<String, List<DiscoveredTool>> discoveredByServer) {
        return flow.getTools().stream()
                .filter(tool -> !completedTools.contains(tool.getName()))
                .filter(tool -> completedTools.containsAll(tool.getDependsOn()))
                .map(configuredTool -> {
            List<DiscoveredTool> discoveredTools = discoveredByServer.computeIfAbsent(
                    configuredTool.getServer(), serverName -> {
                        List<DiscoveredTool> tools = mcpClientRegistry.listTools(serverName);
                        log.info("[DISCOVER] MCP tools discovered: server={}, tools={}", serverName,
                                tools.stream().map(DiscoveredTool::name).toList());
                        return tools;
                    });
            DiscoveredTool discoveredTool = discoveredTools.stream()
                    .filter(candidate -> candidate.name().equals(configuredTool.getName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("MCP tool is not available: " + configuredTool.getName()));
            return Map.<String, Object>of("type", "function", "function", Map.of(
                    "name", configuredTool.getName(),
                    "description", discoveredTool.description() == null ? "" : discoveredTool.description(),
                    "parameters", discoveredTool.inputSchema()));
        }).toList();
    }

    private Set<String> getRequiredTools(FlowDefinition flow) {
        return flow.getTools().stream()
                .filter(ToolDefinition::isRequired)
                .map(ToolDefinition::getName)
                .collect(java.util.stream.Collectors.toSet());
    }

    private void validateToolCall(
            ToolDefinition tool,
            Map<String, Object> modelArguments,
            Set<String> completedTools) {
        for (String dependency : tool.getDependsOn()) {
            if (!completedTools.contains(dependency)) {
                throw new IllegalArgumentException("Required tool has not completed: " + dependency);
            }
        }
        DiscoveredTool discoveredTool = mcpClientRegistry.listTools(tool.getServer()).stream()
                .filter(candidate -> candidate.name().equals(tool.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("MCP tool is not available: " + tool.getName()));
        validateArguments(discoveredTool, modelArguments);
    }

    private Map<String, Object> executeTool(ToolDefinition tool, Map<String, Object> arguments) {
        return mcpClientRegistry.callTool(tool.getServer(), tool.getName(), arguments);
    }

    private void addToolErrorMessage(List<Map<String, Object>> messages, String toolName, String errorMessage) {
        Map<String, Object> toolMessage = new HashMap<>();
        toolMessage.put("role", "tool");
        toolMessage.put("tool_name", toolName);
        toolMessage.put("content", writeJson(Map.of("error", errorMessage, "retryable", true)));
        messages.add(toolMessage);
    }

    private void validateArguments(DiscoveredTool tool, Map<String, Object> arguments) {
        Map<String, Object> properties = asMap(tool.inputSchema().get("properties"));
        for (String argument : arguments.keySet()) {
            if (!properties.containsKey(argument)) {
                throw new IllegalArgumentException("Argument is not allowed for MCP tool " + tool.name() + ": " + argument);
            }
        }
        Object requiredValue = tool.inputSchema().get("required");
        if (requiredValue instanceof List<?> required) {
            for (Object argument : required) {
                if (arguments.get(String.valueOf(argument)) == null) {
                    throw new IllegalArgumentException("Missing argument for MCP tool " + tool.name() + ": " + argument);
                }
            }
        }
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

    /**
     * Intenta parsear tool calls embebidas como texto JSON en el content del mensaje.
     * Algunos modelos de Ollama devuelven los tool calls como texto en lugar de usar la estructura correcta.
     *
     * @param content Texto del mensaje del asistente.
     * @param flow    Definición del flujo actual.
     * @return Lista de tool calls parseadas, o lista vacía si no se encontraron.
     */
    private List<Map<String, Object>> parseToolCallsFromContent(String content, FlowDefinition flow, String flowId, String executionId) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (content == null || content.isBlank()) {
            return result;
        }

        log.debug("[PARSE] Attempting to parse tool calls from content: flowId={}, content={}", flowId, content);

        // Buscar todos los fragmentos que parezcan objetos JSON con "name"
        // Usamos un patrón que busque objetos JSON que contengan "name" y "parameters" o "arguments"
        Pattern pattern = Pattern.compile("\\{[^{}]*\"name\"\\s*:\\s*\"[^\"]+\"[^{}]*\\}");
        Matcher matcher = pattern.matcher(content);
        int foundCount = 0;
        while (matcher.find()) {
            String jsonFragment = matcher.group();
            log.debug("[PARSE] Found JSON fragment: {}", jsonFragment);
            try {
                Map<String, Object> parsed = objectMapper.readValue(jsonFragment, Map.class);
                if (parsed.containsKey("name") && (parsed.containsKey("parameters") || parsed.containsKey("arguments"))) {
                    String toolName = String.valueOf(parsed.get("name"));
                    // Verificar que la herramienta esté permitida en el flujo
                    boolean allowed = flow.getTools().stream()
                            .anyMatch(t -> t.getName().equals(toolName));
                    if (!allowed) {
                        log.warn("[PARSE] Tool call parsed but not allowed in flow: tool={}, allowed={}",
                                toolName, flow.getTools().stream().map(ToolDefinition::getName).toList());
                        continue;
                    }
                    Map<String, Object> arguments = (Map<String, Object>) parsed.getOrDefault("parameters",
                            parsed.getOrDefault("arguments", Map.of()));
                    Map<String, Object> toolCall = new HashMap<>();
                    toolCall.put("id", "call_" + System.nanoTime());
                    Map<String, Object> function = new HashMap<>();
                    function.put("name", toolName);
                    function.put("arguments", arguments);
                    toolCall.put("function", function);
                    result.add(toolCall);
                    foundCount++;
                    log.info("[PARSE] Tool call parsed from content: tool={}, arguments={}", toolName, arguments);
                } else {
                    log.debug("[PARSE] Fragment does not contain name+parameters/arguments: {}", jsonFragment);
                }
            } catch (JsonProcessingException ex) {
                log.warn("[PARSE] Failed to parse JSON fragment: {}, error={}", jsonFragment, ex.getMessage());
            }
        }
        log.info("[PARSE] Parsing complete: found={} valid tool calls", foundCount);
        return result;
    }

    private String renderPrompt(String templatePath, Map<String, Object> attributes) {
        ClassPathResource resource = new ClassPathResource(templatePath);
        try (var input = resource.getInputStream()) {
            String templateText = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            ST template = new ST(templateText);
            attributes.forEach(template::add);
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

    private void updateExecutionStatus(String executionId, String status, String currentStep, String errorMessage) {
        Map<String, Object> statusUpdate = new HashMap<>();
        statusUpdate.put("status", status);
        statusUpdate.put("currentStep", currentStep);
        statusUpdate.put("errorMessage", errorMessage);

        webClient.patch()
            .uri(controlCenterBaseUrl + "/api/processes/" + executionId + "/status")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(statusUpdate)
            .retrieve()
            .toBodilessEntity()
            .block();
    }

    private void updateStepStatus(String executionId, int stepOrder, String status, String errorMessage) {
        Map<String, Object> statusUpdate = new HashMap<>();
        statusUpdate.put("status", status);
        statusUpdate.put("errorMessage", errorMessage);

        webClient.put()
            .uri(controlCenterBaseUrl + "/api/processes/" + executionId + "/steps/" + stepOrder)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(statusUpdate)
            .retrieve()
            .toBodilessEntity()
            .block();
    }
}
