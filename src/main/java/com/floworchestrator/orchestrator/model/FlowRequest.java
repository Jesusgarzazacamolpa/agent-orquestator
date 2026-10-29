package com.floworchestrator.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Petición de arranque de un flujo. Además de {@code flowId} y {@code executionId},
 * acepta atributos arbitrarios (por ejemplo {@code customerId}) que se inyectan en
 * los prompts como variables de plantilla.
 */
@Data
public class FlowRequest {
    @NotBlank(message = "El flowId es obligatorio.")
    private String flowId;
    private String executionId;

    @JsonIgnore
    private Map<String, Object> attributes = new HashMap<>();

    /** Recoge cualquier propiedad desconocida del JSON y la trata como atributo del prompt. */
    @JsonAnySetter
    public void setAttribute(String name, Object value) {
        if ("attributes".equals(name) && value instanceof Map) {
            attributes.putAll((Map<String, Object>) value);
        } else if (!"flowId".equals(name) && !"executionId".equals(name)) {
            attributes.put(name, value);
        }
    }
}
