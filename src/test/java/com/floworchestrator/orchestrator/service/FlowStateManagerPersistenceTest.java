package com.floworchestrator.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.floworchestrator.orchestrator.config.FlowDefinitions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifica que el estado pausado sobreviva a un reinicio del servicio y que
 * los identificadores de ejecución no puedan escapar del directorio de estado.
 */
class FlowStateManagerPersistenceTest {

    @TempDir
    Path stateDirectory;

    @Test
    @DisplayName("debe recuperar un estado pausado tras reiniciar el servicio")
    void shouldRestorePausedStateAfterRestart() throws IOException {
        String executionId = "exec-persisted";
        FlowStateManager firstInstance = manager();
        firstInstance.store(executionId, pausedState());

        assertEquals(1, jsonFiles().size(), "Debe persistirse un archivo de estado");

        FlowStateManager restarted = manager();
        assertTrue(restarted.hasState(executionId), "El estado debe detectarse tras reiniciar");

        FlowStateManager.PausedFlowState restored = restarted.retrieve(executionId);
        assertNotNull(restored);
        assertEquals("compra-vehiculo", restored.flowId());
        assertEquals("compra-vehiculo", restored.flowDefinition().getProcessTemplateName());
        assertEquals(1, restored.messages().size());

        FlowStateManager.AgentStateSnapshot agentState = restored.agentState().snapshot();
        assertEquals(Set.of("get_vehicle_information"), agentState.completedTools());
        assertEquals(3, agentState.currentStepOrder());
        assertEquals("Configurar pago", agentState.currentStep());
        assertEquals("ok", agentState.toolResults().get("get_vehicle_information").get("result"));

        assertFalse(restarted.hasState(executionId), "El estado debe consumirse al recuperarlo");
        assertEquals(0, jsonFiles().size(), "El archivo debe eliminarse tras el retrieve");
    }

    @Test
    @DisplayName("no debe escribir fuera del directorio de estado con executionId malicioso")
    void shouldNotEscapeStateDirectory() throws IOException {
        String executionId = "../../escape";
        FlowStateManager manager = manager();
        manager.store(executionId, pausedState());

        List<Path> files = jsonFiles();
        assertEquals(1, files.size());
        assertEquals(stateDirectory.toAbsolutePath().normalize(),
                files.getFirst().getParent().toAbsolutePath().normalize());

        FlowStateManager restarted = manager();
        assertNotNull(restarted.retrieve(executionId));
    }

    @Test
    @DisplayName("debe descartar y borrar un archivo de estado corrupto")
    void shouldDeleteCorruptStateFile() throws IOException {
        String executionId = "exec-corrupt";
        FlowStateManager manager = manager();
        manager.store(executionId, pausedState());

        Path file = jsonFiles().getFirst();
        Files.writeString(file, "{not-json");

        FlowStateManager restarted = manager();
        assertNull(restarted.retrieve(executionId));
        assertEquals(0, jsonFiles().size(), "El archivo corrupto no debe quedar como veneno");
    }

    @Test
    @DisplayName("debe funcionar en memoria cuando la persistencia está desactivada")
    void shouldWorkInMemoryWhenPersistenceIsDisabled() {
        FlowStateManager manager = new FlowStateManager(new ObjectMapper(), "");
        manager.store("exec-memory", pausedState());

        assertTrue(manager.hasState("exec-memory"));
        assertNotNull(manager.retrieve("exec-memory"));
        assertFalse(manager.hasState("exec-memory"));
    }

    private FlowStateManager manager() {
        return new FlowStateManager(new ObjectMapper(), stateDirectory.toString());
    }

    private FlowStateManager.PausedFlowState pausedState() {
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("compra-vehiculo");

        FlowDefinitions.ToolDefinition tool = new FlowDefinitions.ToolDefinition();
        tool.setName("get_vehicle_information");
        tool.setServer("mcp-domain");
        tool.setStepOrder(1);
        tool.setStepName("Consultar vehículo");
        flow.setTools(List.of(tool));

        FlowService.AgentLoopState agentState = FlowService.AgentLoopState.fromSnapshot(
                new FlowStateManager.AgentStateSnapshot(
                        Set.of("get_vehicle_information"),
                        Map.of("get_vehicle_information", Map.of("result", "ok")),
                        3,
                        "Configurar pago"));

        return new FlowStateManager.PausedFlowState(
                "compra-vehiculo",
                flow,
                List.of(Map.of("role", "user", "content", "continúa")),
                agentState);
    }

    private List<Path> jsonFiles() throws IOException {
        try (Stream<Path> files = Files.list(stateDirectory)) {
            return files.filter(path -> path.getFileName().toString().endsWith(".json")).toList();
        }
    }
}
