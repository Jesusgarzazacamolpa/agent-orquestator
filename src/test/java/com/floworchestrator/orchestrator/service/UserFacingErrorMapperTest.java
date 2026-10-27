package com.floworchestrator.orchestrator.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pruebas unitarias para UserFacingErrorMapper:
 * - Mapeo de errores técnicos a mensajes de usuario en español
 * - Detección por tipo de herramienta (SAT, vehículo, pago)
 * - Detección por patrón de error (timeout, conexión, validación, etc.)
 * - Valor por defecto para errores desconocidos
 */
class UserFacingErrorMapperTest {

    private final UserFacingErrorMapper mapper = new UserFacingErrorMapper();

    @ParameterizedTest
    @DisplayName("debe mapear errores de timeout del SAT")
    @ValueSource(strings = {
            "SAT_TIMEOUT: El servicio del SAT no responde",
            "sat timeout al validar RFC",
            "RFC timeout exceeded",
            "tax timeout occurred"
    })
    void shouldMapSatTimeout(String rawError) {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("validate_tax_id", "mcp-domain", rawError);

        assertEquals("sat_timeout", mapping.errorCode());
        assertEquals("warning", mapping.statusType());
        assertEquals("No pudimos validar tu RFC", mapping.title());
        assertTrue(mapping.message().contains("datos fiscales"));
        assertEquals("Reintentar validación", mapping.actionLabel());
    }

    @ParameterizedTest
    @DisplayName("debe mapear errores de conexión")
    @ValueSource(strings = {
            "Connection refused",
            "Unable to connect to server",
            "Host unreachable",
            "Network connection error"
    })
    void shouldMapConnectionError(String rawError) {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("get_vehicle_information", "mcp-domain", rawError);

        assertEquals("connection_error", mapping.errorCode());
        assertEquals("warning", mapping.statusType());
        assertEquals("No se pudo conectar al servicio", mapping.title());
        assertEquals("Reintentar", mapping.actionLabel());
    }

    @ParameterizedTest
    @DisplayName("debe mapear errores de validación")
    @ValueSource(strings = {
            "Validation failed: model_id is required",
            "Invalid input data",
            "Bad request: missing field",
            "HTTP 400 - validation error"
    })
    void shouldMapValidationError(String rawError) {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("register_vehicle_purchase", "mcp-domain", rawError);

        assertEquals("validation_error", mapping.errorCode());
        assertEquals("error", mapping.statusType());
        assertEquals("Datos inválidos", mapping.title());
        assertEquals("Corregir y reintentar", mapping.actionLabel());
    }

    @ParameterizedTest
    @DisplayName("debe mapear errores de permiso")
    @ValueSource(strings = {
            "permission denied",
            "forbidden access",
            "unauthorized access",
            "HTTP 403 Forbidden"
    })
    void shouldMapPermissionError(String rawError) {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("set_payment_method", "mcp-domain", rawError);

        assertEquals("permission_error", mapping.errorCode());
        assertEquals("error", mapping.statusType());
        assertEquals("Acceso denegado", mapping.title());
        assertEquals("Contactar soporte", mapping.actionLabel());
    }

    @ParameterizedTest
    @DisplayName("debe mapear errores de rate limit")
    @ValueSource(strings = {
            "Rate limit exceeded",
            "Too many requests",
            "HTTP 429 - rate limited"
    })
    void shouldMapRateLimit(String rawError) {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("get_vehicle_information", "mcp-domain", rawError);

        assertEquals("rate_limit", mapping.errorCode());
        assertEquals("warning", mapping.statusType());
        assertEquals("Demasiadas solicitudes", mapping.title());
        assertEquals("Esperar e intentar", mapping.actionLabel());
    }

    @ParameterizedTest
    @DisplayName("debe mapear errores no encontrados")
    @ValueSource(strings = {
            "Resource not found",
            "not_found: vehicle-123",
            "HTTP 404 - not found"
    })
    void shouldMapNotFound(String rawError) {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("get_vehicle_information", "mcp-domain", rawError);

        assertEquals("not_found", mapping.errorCode());
        assertEquals("error", mapping.statusType());
        assertEquals("Recurso no encontrado", mapping.title());
        assertEquals("Reintentar", mapping.actionLabel());
    }

    @Test
    @DisplayName("debe mapear errores de servicio de vehículos por nombre de herramienta")
    void shouldMapVehicleServiceByToolName() {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("search_vehicle", "mcp-domain",
                "vehicle service error");

        assertEquals("vehicle_error", mapping.errorCode());
        assertEquals("warning", mapping.statusType());
        assertEquals("Error en servicio de vehículos", mapping.title());
        assertEquals("Reintentar", mapping.actionLabel());
    }

    @Test
    @DisplayName("debe mapear errores de servicio de pagos por nombre de herramienta")
    void shouldMapPaymentServiceByToolName() {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("create_purchase", "mcp-domain",
                "payment processing failed");

        assertEquals("payment_error", mapping.errorCode());
        assertEquals("warning", mapping.statusType());
        assertEquals("Error en servicio de pagos", mapping.title());
        assertEquals("Reintentar pago", mapping.actionLabel());
    }

    @Test
    @DisplayName("debe mapear intervención requerida desde el mensaje")
    void shouldMapInterventionRequired() {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("get_vehicle_information", "mcp-domain",
                "El agente necesita información adicional del usuario para continuar");

        assertEquals("intervention_required", mapping.errorCode());
        assertEquals("info", mapping.statusType());
        assertEquals("Se requiere información adicional", mapping.title());
        assertEquals("Reanudar proceso", mapping.actionLabel());
    }

    @Test
    @DisplayName("debe usar valor por defecto para errores desconocidos")
    void shouldMapDefaultForUnknownError() {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("unknown_tool", "mcp-domain", "algo extraño ocurrió");

        assertEquals("unknown_error", mapping.errorCode());
        assertEquals("error", mapping.statusType());
        assertEquals("Error inesperado", mapping.title());
        assertTrue(mapping.message().contains("inesperado"));
        assertEquals("Reintentar", mapping.actionLabel());
    }

    @Test
    @DisplayName("debe manejar mensaje de error nulo")
    void shouldHandleNullErrorMessage() {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError(null, null, null);

        assertNotNull(mapping);
        assertEquals("unknown_error", mapping.errorCode());
    }

    @Test
    @DisplayName("debe manejar servidor nulo")
    void shouldHandleNullServer() {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("get_vehicle_information", null, "connection refused");

        assertEquals("connection_error", mapping.errorCode());
    }

    @Test
    @DisplayName("el nombre de herramienta solo es candidato, el patrón debe coincidir con el error")
    void toolNameIsCandidatePatternMustMatch() {
        // "timeout occurred" no contiene "payment", así que PAYMENT_SERVICE no matchea
        // Aunque create_purchase esté en TOOL_SPECIFIC -> PAYMENT_SERVICE, el patrón no coincide
        // con el texto del error, por lo que cae en TIMEOUT
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("create_purchase", "mcp-domain",
                "timeout occurred");

        assertEquals("timeout", mapping.errorCode());
    }

    @Test
    @DisplayName("cuando el nombre de herramienta y el patrón coinciden, se usa el mapeo específico")
    void toolNameAndPatternMatch() {
        UserFacingErrorMapper.ErrorMapping mapping = mapper.mapError("create_purchase", "mcp-domain",
                "payment gateway failed");

        assertEquals("payment_error", mapping.errorCode());
        assertEquals("Reintentar pago", mapping.actionLabel());
    }
}
