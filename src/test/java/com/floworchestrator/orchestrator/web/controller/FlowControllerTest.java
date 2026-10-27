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
