package com.floworchestrator.orchestrator.web.controller;

import com.floworchestrator.orchestrator.model.FlowExecutionEvent;
import com.floworchestrator.orchestrator.service.FlowEventPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * Endpoint SSE que expone los eventos de ejecución de flujos en tiempo real.
 * Control Center se suscribe a este stream para realizar el bitácora de pasos y ejecuciones.
 */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class FlowStreamController {

    private final FlowEventPublisher flowEventPublisher;

    @GetMapping(value = "/flows/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<FlowExecutionEvent>> stream() {
        return flowEventPublisher.stream()
                .map(event -> ServerSentEvent.<FlowExecutionEvent>builder()
                        .id(event.executionId() + "-" + event.timestamp().toInstant().toEpochMilli())
                        .event(event.eventType())
                        .data(event)
                        .build());
    }
}
