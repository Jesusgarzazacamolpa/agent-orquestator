package com.floworchestrator.orchestrator.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

/**
 * Registro del avance de un paso del flujo (orden, estado, reintentos y marcas
 * de tiempo); se usa para bitácora y para el reintento de pasos fallidos.
 */
@Data
@NoArgsConstructor
public class FlowStepExecution {
    private String id;
    private int stepOrder;
    private String name;
    private String status;
    private int retryCount;
    private String errorMessage;
    private OffsetDateTime startedAt;
    private OffsetDateTime completedAt;
}
