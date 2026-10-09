package com.floworchestrator.orchestrator.web.controller;

import com.floworchestrator.orchestrator.model.FlowRequest;
import com.floworchestrator.orchestrator.model.FlowResponse;
import com.floworchestrator.orchestrator.service.FlowService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class FlowController {

    private final FlowService flowService;

    @PostMapping("/flows")
    public ResponseEntity<FlowResponse> createFlow(@RequestBody FlowRequest request) {
        return ResponseEntity.ok(flowService.createFlow(request));
    }

    @GetMapping("/flows/health")
    public String health() {
        return "Flow Orchestrator OK";
    }
}
