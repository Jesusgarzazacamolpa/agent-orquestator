package com.floworchestrator.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.floworchestrator.orchestrator.config.FlowDefinitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Almacena el estado de los flujos pausados o fallidos para permitir
 * {@code resume}/{@code retry}. El estado vive en memoria y, si se configura
 * un directorio, también se persiste en disco para sobrevivir reinicios.
 */
@Component
public class FlowStateManager {

    private static final Logger log = LoggerFactory.getLogger(FlowStateManager.class);
    private static final int SCHEMA_VERSION = 1;
    private static final String DEFAULT_STATE_DIRECTORY = ".flow-state";

    private final Map<String, PausedFlowState> states = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ReentrantLock> executionLocks = new ConcurrentHashMap<>();
    private final Path stateDirectory;
    private final ObjectMapper objectMapper;

    @Autowired
    public FlowStateManager(ObjectMapper objectMapper,
            @Value("${orchestrator.state.directory:" + DEFAULT_STATE_DIRECTORY + "}") String directory) {
        this.objectMapper = objectMapper;
        this.stateDirectory = initializeDirectory(directory);
    }

    FlowStateManager() {
        this.objectMapper = new ObjectMapper();
        this.stateDirectory = null;
    }

    private Path initializeDirectory(String directory) {
        if (directory == null || directory.isBlank()) {
            log.info("[STATE] Persistencia de estados pausados deshabilitada");
            return null;
        }
        try {
            Path path = Path.of(directory).toAbsolutePath().normalize();
            Files.createDirectories(path);
            log.info("[STATE] Directorio de estados de flujos pausados: {}", path);
            return path;
        } catch (IOException ex) {
            log.error("[STATE] No se pudo crear el directorio de estados '{}'; persistencia deshabilitada",
                    directory, ex);
            return null;
        }
    }

    private ReentrantLock lockFor(String executionId) {
        return executionLocks.computeIfAbsent(executionId, k -> new ReentrantLock());
    }

    /** Guarda el estado en memoria y, si hay directorio configurado, en disco antes de liberar el lock. */
    public void store(String executionId, PausedFlowState state) {
        ReentrantLock lock = lockFor(executionId);
        lock.lock();
        try {
            states.put(executionId, state);
            persist(executionId, state);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Recupera el estado y lo elimina (memoria y disco) para que solo una
     * reanudación pueda consumirlo; devuelve {@code null} si no existe.
     */
    public PausedFlowState retrieve(String executionId) {
        ReentrantLock lock = lockFor(executionId);
        lock.lock();
        try {
            PausedFlowState state = states.remove(executionId);
            if (state == null) {
                state = load(executionId);
            }
            delete(executionId);
            return state;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Recupera el estado sin eliminarlo (primero de memoria y luego de disco),
     * para validar una solicitud antes de modificarla. Devuelve {@code null} si
     * no existe.
     */
    public PausedFlowState peek(String executionId) {
        ReentrantLock lock = lockFor(executionId);
        lock.lock();
        try {
            PausedFlowState state = states.get(executionId);
            return state != null ? state : load(executionId);
        } finally {
            lock.unlock();
        }
    }

    /** Indica si hay un estado pausado en memoria o en disco, sin consumirlo. */
    public boolean hasState(String executionId) {
        ReentrantLock lock = lockFor(executionId);
        lock.lock();
        try {
            return states.containsKey(executionId)
                    || (stateDirectory != null && Files.exists(stateFile(executionId)));
        } finally {
            lock.unlock();
        }
    }

    /** Ejecuta una acción bajo el lock de la ejecución para evitar reanudaciones concurrentes. */
    public <T> T withExecutionLock(String executionId, java.util.function.Supplier<T> action) {
        ReentrantLock lock = lockFor(executionId);
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    /** Variante sin valor de retorno de {@link #withExecutionLock}. */
    public void runWithExecutionLock(String executionId, Runnable action) {
        ReentrantLock lock = lockFor(executionId);
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    private void persist(String executionId, PausedFlowState state) {
        if (stateDirectory == null) {
            return;
        }
        try {
            PersistedFlowState snapshot = PersistedFlowState.from(executionId, state);
            Path tempFile = Files.createTempFile(stateDirectory, "state-", ".tmp");
            try {
                objectMapper.writeValue(tempFile.toFile(), snapshot);
                moveAtomically(tempFile, stateFile(executionId));
            } finally {
                Files.deleteIfExists(tempFile);
            }
        } catch (IOException ex) {
            log.error("[STATE] No se pudo persistir el estado pausado: executionId={}", executionId, ex);
        }
    }

    private void moveAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ex) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private PausedFlowState load(String executionId) {
        if (stateDirectory == null) {
            return null;
        }
        Path file = stateFile(executionId);
        if (!Files.exists(file)) {
            return null;
        }
        try {
            PersistedFlowState snapshot = objectMapper.readValue(file.toFile(), PersistedFlowState.class);
            if (!executionId.equals(snapshot.executionId())) {
                log.warn("[STATE] Se ignora el archivo de estado con executionId distinto: esperado={}, encontrado={}",
                        executionId, snapshot.executionId());
                return null;
            }
            log.info("[STATE] Estado pausado restaurado desde disco: executionId={}, flowId={}",
                    executionId, snapshot.flowId());
            return snapshot.toPausedState();
        } catch (IOException ex) {
            log.error("[STATE] Archivo de estado pausado corrupto; se elimina: executionId={}, error={}",
                    executionId, ex.getMessage());
            delete(executionId);
            return null;
        }
    }

    private void delete(String executionId) {
        if (stateDirectory == null) {
            return;
        }
        try {
            Files.deleteIfExists(stateFile(executionId));
        } catch (IOException ex) {
            log.warn("[STATE] No se pudo eliminar el archivo de estado: executionId={}", executionId, ex);
        }
    }

    private Path stateFile(String executionId) {
        return stateDirectory.resolve(sha256(executionId) + ".json");
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 no está disponible", ex);
        }
    }

    /** Estado de espera de una ejecución: definición del flujo, conversación completa y avance del agente. */
    public record PausedFlowState(
            String flowId,
            FlowDefinitions.FlowDefinition flowDefinition,
            List<Map<String, Object>> messages,
            FlowService.AgentLoopState agentState
    ) {
    }

    /** Instantánea serializable de {@code AgentLoopState}; excluye el descubrimiento MCP, que se repite al reanudar. */
    public record AgentStateSnapshot(
            Set<String> completedTools,
            Set<String> failedTools,
            Map<String, Map<String, Object>> toolResults,
            int currentStepOrder,
            String currentStep,
            String pendingInterventionTool,
            String pendingInterventionToolCallId,
            Map<String, Object> pendingInterventionArguments
    ) {
    }

    /** Envoltorio con versión de esquema que se escribe en el archivo JSON de estado. */
    public record PersistedFlowState(
            int version,
            String executionId,
            String flowId,
            FlowDefinitions.FlowDefinition flowDefinition,
            List<Map<String, Object>> messages,
            AgentStateSnapshot agentState
    ) {
        static PersistedFlowState from(String executionId, PausedFlowState state) {
            return new PersistedFlowState(
                    SCHEMA_VERSION,
                    executionId,
                    state.flowId(),
                    state.flowDefinition(),
                    state.messages(),
                    state.agentState().snapshot());
        }

        PausedFlowState toPausedState() {
            return new PausedFlowState(
                    flowId,
                    flowDefinition,
                    messages == null ? List.of() : messages,
                    FlowService.AgentLoopState.fromSnapshot(
                            agentState == null
                                    ? new AgentStateSnapshot(Set.of(), Set.of(), Map.of(), 0, null,
                                            null, null, null)
                                    : agentState));
        }
    }
}
