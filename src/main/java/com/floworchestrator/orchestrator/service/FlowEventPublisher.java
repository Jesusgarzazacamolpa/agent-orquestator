package com.floworchestrator.orchestrator.service;

import com.floworchestrator.orchestrator.model.FlowExecutionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Publica eventos de ejecución de flujos y los expone como un stream reactivo
 * para que otros servicios (por ejemplo Control Center) puedan consumirlos vía SSE.
 */
@Component
public class FlowEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(FlowEventPublisher.class);
    private static final int REPLAY_LIMIT = 100;

    private final Sinks.Many<FlowExecutionEvent> sink = Sinks.many().replay().limit(REPLAY_LIMIT);

    /**
     * Publica un evento de ejecución en el stream.
     *
     * @param event Evento a publicar.
     */
    public void publish(FlowExecutionEvent event) {
        Sinks.EmitResult result = sink.tryEmitNext(event);
        if (result.isFailure()) {
            log.warn("[SSE] No se pudo publicar el evento: type={}, executionId={}, result={}",
                    event.eventType(), event.executionId(), result);
        }
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
