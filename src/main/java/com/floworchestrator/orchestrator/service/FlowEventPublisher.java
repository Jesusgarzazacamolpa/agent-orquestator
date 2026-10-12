package com.floworchestrator.orchestrator.service;

import com.floworchestrator.orchestrator.model.FlowExecutionEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Publica eventos de ejecución de flujos y los expone como un stream reactivo
 * para que otros servicios (por ejemplo Control Center) puedan consumirlos vía SSE.
 */
@Component
public class FlowEventPublisher {

    private final Sinks.Many<FlowExecutionEvent> sink = Sinks.many().replay().limit(100);

    /**
     * Publica un evento de ejecución en el stream.
     *
     * @param event Evento a publicar.
     */
    public void publish(FlowExecutionEvent event) {
        sink.tryEmitNext(event);
    }

    /**
     * Devuelve el flux de eventos para que los consumidores SSE puedan suscribirse.
     *
     * @return Flux de eventos de ejecución.
     */
    public Flux<FlowExecutionEvent> stream() {
        return sink.asFlux();
    }
}
