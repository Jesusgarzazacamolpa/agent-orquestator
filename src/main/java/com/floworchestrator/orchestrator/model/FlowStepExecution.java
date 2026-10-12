package com.floworchestrator.orchestrator.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

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
