package com.floworchestrator.orchestrator.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "orchestrator")
public class FlowDefinitions {
    private Map<String, FlowDefinition> flows = new LinkedHashMap<>();
    private Map<String, String> mcpServers = new LinkedHashMap<>();

    @Data
    public static class FlowDefinition {
        private String processTemplateName;
        private String systemPromptTemplate;
        private String userPromptTemplate;
        private List<ToolDefinition> tools = new ArrayList<>();
    }

    @Data
    public static class ToolDefinition {
        private String server;
        private String name;
        private int stepOrder;
        private String stepName;
        private boolean required = true;
        private List<String> dependsOn = new ArrayList<>();
    }
}