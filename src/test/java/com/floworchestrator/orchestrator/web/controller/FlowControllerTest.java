package com.floworchestrator.orchestrator.web.controller;

import com.floworchestrator.orchestrator.model.FlowAsyncAccepted;
import com.floworchestrator.orchestrator.model.FlowRequest;
import com.floworchestrator.orchestrator.service.FlowService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FlowControllerTest {

    @Test
    @DisplayName("debe responder 202 al reanudar sin bloquear la petición")
    void shouldReturnAcceptedWhenResuming() {
        FlowService flowService = mock(FlowService.class);
        when(flowService.resumeFlowAsync("exec-1", "dato"))
                .thenReturn(FlowAsyncAccepted.accepted(
                        "request-1", "compra-vehiculo", "exec-1", "trace-1"));

        FlowController controller = new FlowController(flowService);

        ResponseEntity<FlowAsyncAccepted> response = controller.resumeFlow("exec-1", "dato");

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertEquals("exec-1", response.getBody().executionId());
    }

    @Test
    @DisplayName("debe responder 404 al reanudar sin estado pausado")
    void shouldReturnNotFoundWhenResumingUnknownExecution() {
        FlowService flowService = mock(FlowService.class);
        when(flowService.resumeFlowAsync("exec-missing", null))
                .thenThrow(new IllegalStateException("sin estado"));

        FlowController controller = new FlowController(flowService);

        ResponseEntity<FlowAsyncAccepted> response = controller.resumeFlow("exec-missing", null);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    @Test
    @DisplayName("debe responder 202 al iniciar un flujo asíncrono")
    void shouldReturnAcceptedWhenStartingFlowAsynchronously() {
        FlowService flowService = mock(FlowService.class);
        when(flowService.startFlowAsync(any(FlowRequest.class)))
                .thenReturn(FlowAsyncAccepted.accepted(
                        "request-1", "compra-vehiculo", null, "trace-1"));

        FlowController controller = new FlowController(flowService);
        FlowRequest request = new FlowRequest();
        request.setFlowId("compra-vehiculo");

        ResponseEntity<FlowAsyncAccepted> response = controller.createFlowAsync(request);

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("ACCEPTED", response.getBody().status());
        assertEquals("trace-1", response.getBody().traceId());
    }
}
