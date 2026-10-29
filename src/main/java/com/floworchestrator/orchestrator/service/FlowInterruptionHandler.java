package com.floworchestrator.orchestrator.service;

import com.floworchestrator.orchestrator.config.FlowDefinitions.FlowDefinition;
import com.floworchestrator.orchestrator.config.FlowDefinitions.ToolDefinition;
import com.floworchestrator.orchestrator.model.FlowExecutionEvent;
import com.floworchestrator.orchestrator.model.FlowResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Construye la respuesta y los eventos cuando el bucle del agente se detiene:
 * pausa por intervención humana, indisponibilidad del LLM o fallo del flujo.
 * Persiste el estado en cada caso para permitir {@code resume} y {@code retry}.
 */
@Component
public class FlowInterruptionHandler {

    private static final Logger log = LoggerFactory.getLogger(FlowInterruptionHandler.class);

    private static final String FLOW_CODE_PAUSED = "PAUSED_FOR_HUMAN";
    private static final String FLOW_CODE_FLOW_ERROR = "ERR_FLOW_01";

    private final FlowEventPublisher flowEventPublisher;
    private final FlowStateManager flowStateManager;
    private final UserFacingErrorMapper errorMapper;

    public FlowInterruptionHandler(FlowEventPublisher flowEventPublisher,
                                   FlowStateManager flowStateManager,
                                   UserFacingErrorMapper errorMapper) {
        this.flowEventPublisher = flowEventPublisher;
        this.flowStateManager = flowStateManager;
        this.errorMapper = errorMapper;
    }

    /**
     * Traduce la excepción que detuvo el loop en una respuesta con estado
     * PAUSED/FAILED, publica el evento SSE y guarda el estado para poder reanudar.
     */
    public FlowResponse handle(
            String executionId, String flowId, FlowDefinition flow,
            List<Map<String, Object>> messages, FlowService.AgentLoopState state,
            RuntimeException ex, boolean isResume,
            Map<String, Object> attributes, String traceId) {
        FlowResponse response = new FlowResponse();
        response.setId(executionId);
        response.setTraceId(traceId);
        response.setSteps(flow.getTools().stream().map(ToolDefinition::getStepName).toList());
        response.setAttributes(attributes == null ? new HashMap<>() : new HashMap<>(attributes));

        if (ex instanceof RequiresHumanInterventionException intervention) {
            handlePause(executionId, flowId, flow, messages, state, intervention, isResume, response);
        } else if (ex instanceof LlmUnavailableException llmUnavailable) {
            handleLlmUnavailable(executionId, flowId, flow, messages, state, llmUnavailable, response);
        } else {
            handleFailure(executionId, flowId, flow, messages, state, ex, isResume, response);
        }
        return response;
    }

    /** Pausa el flujo con estado {@code PAUSED_FOR_HUMAN} conservando la conversación y el avance del agente. */
    private void handlePause(String executionId, String flowId, FlowDefinition flow,
                             List<Map<String, Object>> messages, FlowService.AgentLoopState state,
                             RequiresHumanInterventionException intervention, boolean isResume,
                             FlowResponse response) {
        String errorMessage = messageOf(intervention);
        String userTitle = "Se requiere información adicional";
        String userMessage = "El agente necesita información adicional para continuar. "
                + "Proporciona los datos solicitados y reanuda el proceso.";
        String userActionLabel = "Reanudar proceso";
        String userStatusType = "info";
        String logPrefix = isResume ? "[PAUSE] Flujo pausado nuevamente" : "[PAUSE] Flujo pausado";
        log.warn("{}: executionId={}, step={}, reason={}",
                logPrefix, executionId, state.currentStepName(), errorMessage);
        flowEventPublisher.publish(FlowExecutionEvent.withUserAgentError(
                executionId, flowId, "FLOW_PAUSED", state.currentStepNumber(), state.currentStepName(),
                null, null, FLOW_CODE_PAUSED, errorMessage,
                userTitle, userMessage, userActionLabel, userStatusType, FLOW_CODE_PAUSED));
        storeState(executionId, flowId, flow, messages, state);
        response.setStatus(FLOW_CODE_PAUSED);
        response.setFlowCode(FLOW_CODE_PAUSED);
        response.setCurrentStep(state.currentStepName());
        response.setErrorMessage(errorMessage);
        response.setUserErrorTitle(userTitle);
        response.setUserErrorMessage(userMessage);
        response.setUserErrorActionLabel(userActionLabel);
        response.setUserErrorStatusType(userStatusType);
    }

    /** Marca el flujo como FAILED por indisponibilidad del LLM, conservando el código y mensaje amigable de la excepción. */
    private void handleLlmUnavailable(String executionId, String flowId, FlowDefinition flow,
                                      List<Map<String, Object>> messages, FlowService.AgentLoopState state,
                                      LlmUnavailableException ex, FlowResponse response) {
        String userTitle = "Servicio de IA no disponible";
        log.error("[AI-DEGRADED] Flujo detenido por indisponibilidad del LLM: executionId={}, "
                        + "step={}, flowCode={}, error={}",
                executionId, state.currentStepName(), ex.getFlowCode(), messageOf(ex));
        flowEventPublisher.publish(FlowExecutionEvent.withUserAgentError(
                executionId, flowId, "FLOW_FAILED", state.currentStepNumber(), state.currentStepName(),
                null, null, "FAILED", messageOf(ex),
                userTitle, ex.getUserMessage(), ex.getUserAction(),
                "warning", ex.getFlowCode()));
        storeState(executionId, flowId, flow, messages, state);
        response.setStatus("FAILED");
        response.setFlowCode(ex.getFlowCode());
        response.setCurrentStep(state.currentStepName());
        response.setErrorMessage(messageOf(ex));
        response.setUserErrorTitle(userTitle);
        response.setUserErrorMessage(ex.getUserMessage());
        response.setUserErrorActionLabel(ex.getUserAction());
        response.setUserErrorStatusType("warning");
    }

    /** Marca el flujo como FAILED por un error no clasificado, delegando el mensaje al mapeador de errores. */
    private void handleFailure(String executionId, String flowId, FlowDefinition flow,
                               List<Map<String, Object>> messages, FlowService.AgentLoopState state,
                               RuntimeException ex, boolean isResume, FlowResponse response) {
        String errorMessage = messageOf(ex);
        String logPrefix = isResume ? "[FAIL] Flujo falló al reanudar" : "[FAIL] El flujo falló";
        log.error("{}: executionId={}, step={}, errorType={}, message={}",
                logPrefix, executionId, state.currentStepName(), ex.getClass().getSimpleName(), errorMessage);
        UserFacingErrorMapper.ErrorMapping userError =
                errorMapper.mapError(state.currentStepName(), null, errorMessage);
        flowEventPublisher.publish(FlowExecutionEvent.withUserAgentError(
                executionId, flowId, "FLOW_FAILED", state.currentStepNumber(), state.currentStepName(),
                null, null, "FAILED", errorMessage,
                userError.title(), userError.message(), userError.actionLabel(),
                userError.statusType(), userError.errorCode()));
        storeState(executionId, flowId, flow, messages, state);
        response.setStatus("FAILED");
        response.setFlowCode(FLOW_CODE_FLOW_ERROR);
        response.setCurrentStep(state.currentStepName());
        response.setErrorMessage(errorMessage);
        response.setUserErrorTitle(userError.title());
        response.setUserErrorMessage(userError.message());
        response.setUserErrorActionLabel(userError.actionLabel());
        response.setUserErrorStatusType(userError.statusType());
    }

    private void storeState(String executionId, String flowId, FlowDefinition flow,
                            List<Map<String, Object>> messages, FlowService.AgentLoopState state) {
        flowStateManager.store(executionId,
                new FlowStateManager.PausedFlowState(flowId, flow, List.copyOf(messages), state));
    }

    private String messageOf(Exception ex) {
        return ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
    }
}
