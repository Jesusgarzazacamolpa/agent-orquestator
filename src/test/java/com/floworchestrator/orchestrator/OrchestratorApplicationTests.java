package com.floworchestrator.orchestrator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Verifica que el contexto de Spring se construye con todos sus beans
 * (ChatModel, FlowService, resiliencia) sin depender de servicios externos.
 */
@SpringBootTest
class OrchestratorApplicationTests {

    @Test
    void contextLoads() {
    }
}
