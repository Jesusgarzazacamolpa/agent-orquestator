package com.floworchestrator.orchestrator.service;

import com.floworchestrator.orchestrator.config.FlowDefinitions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pruebas unitarias para FlowStateManager:
 * - Almacenamiento y recuperación de estado por executionId
 * - Bloqueo por ejecución para evitar condiciones de carrera
 * - Seguridad bajo concurrencia
 */
class FlowStateManagerTest {

    @Test
    @DisplayName("debe almacenar y recuperar un estado de flujo pausado")
    void shouldStoreAndRetrievePausedState() throws Exception {
        FlowStateManager manager = new FlowStateManager();
        String executionId = "exec-001";
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("compra-vehiculo");

        FlowDefinitions.ToolDefinition tool = new FlowDefinitions.ToolDefinition();
        tool.setName("get_vehicle_information");
        tool.setStepOrder(1);
        tool.setStepName("Consultar vehículo");
        flow.setTools(List.of(tool));

        List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", "system prompt"));

        FlowService.AgentLoopState agentState = new FlowService.AgentLoopState();
        setCompletedTools(agentState, List.of("get_vehicle_information"));

        FlowStateManager.PausedFlowState pausedState =
                new FlowStateManager.PausedFlowState("compra-vehiculo", flow, messages, agentState);

        manager.store(executionId, pausedState);

        assertTrue(manager.hasState(executionId), "El estado debe existir tras almacenarlo");

        FlowStateManager.PausedFlowState retrieved = manager.retrieve(executionId);
        assertNotNull(retrieved, "No debe devolver null al recuperar");
        assertEquals("compra-vehiculo", retrieved.flowId());
        assertEquals("compra-vehiculo", retrieved.flowDefinition().getProcessTemplateName());
        assertEquals(1, retrieved.messages().size());
        assertEquals(1, getCompletedTools(retrieved.agentState()).size());
        assertTrue(getCompletedTools(retrieved.agentState()).contains("get_vehicle_information"));

        // Tras retrieve, el estado debe eliminarse
        assertFalse(manager.hasState(executionId), "El estado debe eliminarse tras retrieve");
        assertNull(manager.retrieve(executionId), "Debe devolver null tras estar vacío");
    }

    @Test
    @DisplayName("debe devolver false para executionId inexistente")
    void shouldReturnFalseForNonExistentExecution() {
        FlowStateManager manager = new FlowStateManager();
        assertFalse(manager.hasState("nonexistent"));
        assertNull(manager.retrieve("nonexistent"));
        assertNull(manager.peek("nonexistent"));
    }

    @Test
    @DisplayName("peek debe devolver el estado sin consumirlo")
    void shouldPeekWithoutConsumingState() {
        FlowStateManager manager = new FlowStateManager();
        String executionId = "exec-peek";
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("flujo-peek");
        manager.store(executionId, new FlowStateManager.PausedFlowState(
                "flujo-peek", flow, List.of(), new FlowService.AgentLoopState()));

        assertEquals("flujo-peek", manager.peek(executionId).flowDefinition().getProcessTemplateName());
        assertEquals("flujo-peek", manager.peek(executionId).flowDefinition().getProcessTemplateName());
        assertTrue(manager.hasState(executionId), "peek no debe eliminar el estado");

        assertNotNull(manager.retrieve(executionId));
        assertFalse(manager.hasState(executionId));
    }

    @Test
    @DisplayName("debe sobrescribir el estado existente para el mismo executionId")
    void shouldOverwriteExistingState() {
        FlowStateManager manager = new FlowStateManager();
        String executionId = "exec-002";
        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("flujo-v1");

        FlowStateManager.PausedFlowState state1 =
                new FlowStateManager.PausedFlowState("flujo-v1", flow, List.of(), new FlowService.AgentLoopState());
        manager.store(executionId, state1);

        FlowDefinitions.FlowDefinition flow2 = new FlowDefinitions.FlowDefinition();
        flow2.setProcessTemplateName("flujo-v2");
        FlowStateManager.PausedFlowState state2 =
                new FlowStateManager.PausedFlowState("flujo-v2", flow2, List.of(), new FlowService.AgentLoopState());
        manager.store(executionId, state2);

        FlowStateManager.PausedFlowState retrieved = manager.retrieve(executionId);
        assertNotNull(retrieved);
        assertEquals("flujo-v2", retrieved.flowDefinition().getProcessTemplateName());
    }

    @Test
    @DisplayName("debe permitir acceso concurrente sin corromper datos")
    @Timeout(10)
    void shouldHandleConcurrentAccess() throws InterruptedException {
        FlowStateManager manager = new FlowStateManager();
        int threadCount = 20;
        int operationsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successfulStores = new AtomicInteger(0);
        AtomicInteger successfulRetrieves = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        String executionId = "exec-" + threadId + "-" + i;
                        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
                        flow.setProcessTemplateName("flujo-" + threadId);

                        FlowStateManager.PausedFlowState state =
                                new FlowStateManager.PausedFlowState(
                                        "flujo-" + threadId, flow,
                                        List.of(Map.of("role", "user", "content", "msg-" + i)),
                                        new FlowService.AgentLoopState());

                        manager.store(executionId, state);
                        successfulStores.incrementAndGet();

                        Thread.yield();

                        FlowStateManager.PausedFlowState retrieved = manager.retrieve(executionId);
                        if (retrieved != null) {
                            successfulRetrieves.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Las operaciones concurrentes deben completarse");
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "El executor debe terminar");

        assertEquals(threadCount * operationsPerThread, successfulStores.get(),
                "Todas las store deben completarse");
        assertEquals(threadCount * operationsPerThread, successfulRetrieves.get(),
                "Todas las retrieve deben recuperar su estado correspondiente");
    }

    @Test
    @DisplayName("withExecutionLock debe ejecutar la acción bajo bloqueo y devolver resultado")
    void shouldExecuteWithLockAndReturnResult() {
        FlowStateManager manager = new FlowStateManager();
        String executionId = "exec-lock";

        AtomicInteger counter = new AtomicInteger(0);

        FlowDefinitions.FlowDefinition flow = new FlowDefinitions.FlowDefinition();
        flow.setProcessTemplateName("test-flow");
        manager.store(executionId,
                new FlowStateManager.PausedFlowState("test-flow", flow, List.of(), new FlowService.AgentLoopState()));

        Integer result = manager.withExecutionLock(executionId, () -> {
            FlowStateManager.PausedFlowState current = manager.retrieve(executionId);
            return counter.incrementAndGet();
        });

        assertEquals(1, result);
    }

    @Test
    @DisplayName("runWithExecutionLock debe ejecutar la acción bajo bloqueo")
    void shouldRunWithLock() {
        FlowStateManager manager = new FlowStateManager();
        AtomicInteger executed = new AtomicInteger(0);

        manager.runWithExecutionLock("exec-run", () -> executed.incrementAndGet());

        assertEquals(1, executed.get());
    }

    // ========== Helpers de reflexión para AgentLoopState ==========

    private void setCompletedTools(FlowService.AgentLoopState state, List<String> tools) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("completedTools");
        f.setAccessible(true);
        f.set(state, new java.util.HashSet<>(tools));
    }

    private List<String> getCompletedTools(FlowService.AgentLoopState state) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("completedTools");
        f.setAccessible(true);
        Object val = f.get(state);
        return val != null ? new ArrayList<>((java.util.Set<String>) val) : List.of();
    }

    private void setCurrentStep(FlowService.AgentLoopState state, String step) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("currentStep");
        f.setAccessible(true);
        f.set(state, step);
    }

    private void setCurrentStepOrder(FlowService.AgentLoopState state, int order) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("currentStepOrder");
        f.setAccessible(true);
        f.set(state, order);
    }

    private String getCurrentStep(FlowService.AgentLoopState state) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("currentStep");
        f.setAccessible(true);
        return (String) f.get(state);
    }

    private int getCurrentStepOrder(FlowService.AgentLoopState state) throws Exception {
        Field f = FlowService.AgentLoopState.class.getDeclaredField("currentStepOrder");
        f.setAccessible(true);
        return (int) f.get(state);
    }
}
