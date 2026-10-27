package com.floworchestrator.orchestrator.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.regex.Pattern;

@Component
@Slf4j
public class UserFacingErrorMapper {

    @Getter
    public enum ErrorPattern {
        TIMEOUT(".*timeout.*", "timeout", "warning",
                "La validación tardó demasiado",
                "El servicio externo no respondió a tiempo. Por favor, verifica tus datos e intenta nuevamente.",
                "Reintentar"),
        CONNECTION_ERROR(".*connect.*|.*connection.*|.*refused.*|.*unreachable.*", "connection_error", "warning",
                "No se pudo conectar al servicio",
                "Hay un problema de conectividad con un servicio externo. Intenta nuevamente en unos minutos.",
                "Reintentar"),
        VALIDATION_ERROR(".*validation.*|.*invalid.*|.*bad request.*|.*400.*", "validation_error", "error",
                "Datos inválidos",
                "Los datos proporcionados no son válidos. Por favor, corrige los campos marcados e intenta nuevamente.",
                "Corregir y reintentar"),
        NOT_FOUND(".*not found.*|.*not_found.*|.*404.*", "not_found", "error",
                "Recurso no encontrado",
                "No se encontró el recurso solicitado. Verifica los datos e intenta nuevamente.",
                "Reintentar"),
        PERMISSION_ERROR(".*permission.*|.*forbidden.*|.*403.*|.*unauthorized.*|.*401.*", "permission_error", "error",
                "Acceso denegado",
                "No tienes permisos para realizar esta operación. Contacta al administrador si el problema persiste.",
                "Contactar soporte"),
        RATE_LIMIT(".*rate.*|.*too many.*|.*429.*", "rate_limit", "warning",
                "Demasiadas solicitudes",
                "El servicio está recibiendo demasiadas solicitudes. Espera unos minutos e intenta nuevamente.",
                "Esperar e intentar"),
        SAT_TIMEOUT(".*SAT_TIMEOUT.*|.*sat.*timeout.*|.*RFC.*timeout.*|.*tax.*timeout.*", "sat_timeout", "warning",
                "No pudimos validar tu RFC",
                "Hubo un inconveniente al validar tus datos fiscales. Por favor, verifica que sean correctos o intenta nuevamente en unos minutos.",
                "Reintentar validación"),
        INTERVENTION_REQUIRED(".*intervención.*|.*intervention.*|.*necesita.*información.*|.*user.*input.*", "intervention_required", "info",
                "Se requiere información adicional",
                "El agente necesita información adicional para continuar. Proporciona los datos solicitados y reanuda el proceso.",
                "Reanudar proceso"),
        VEHICLE_SERVICE(".*vehicle.*|.*vehiculo.*|.*vin.*", "vehicle_error", "warning",
                "Error en servicio de vehículos",
                "Hubo un problema al consultar la información del vehículo. Verifica el VIN e intenta nuevamente.",
                "Reintentar"),
        PAYMENT_SERVICE(".*payment.*|.*pago.*|.*payment_method.*", "payment_error", "warning",
                "Error en servicio de pagos",
                "No pudimos procesar la información de pago. Verifica los datos e intenta nuevamente.",
                "Reintentar pago"),
        DEFAULT(".*", "unknown_error", "error",
                "Error inesperado",
                "Ocurrió un error inesperado durante el proceso. El error ha sido registrado y un administrador será notificado.",
                "Reintentar");

        private final Pattern pattern;
        private final String errorCode;
        private final String statusType;
        private final String title;
        private final String message;
        private final String actionLabel;

        ErrorPattern(String regex, String errorCode, String statusType, String title, String message, String actionLabel) {
            this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
            this.errorCode = errorCode;
            this.statusType = statusType;
            this.title = title;
            this.message = message;
            this.actionLabel = actionLabel;
        }

        public boolean matches(String error) {
            return pattern.matcher(error != null ? error : "").matches();
        }
    }

    private static final Map<String, ErrorPattern> TOOL_SPECIFIC = Map.of(
        "search_vehicle", ErrorPattern.VEHICLE_SERVICE,
        "validate_vehicle", ErrorPattern.VEHICLE_SERVICE,
        "create_purchase", ErrorPattern.PAYMENT_SERVICE,
        "set_payment_method", ErrorPattern.PAYMENT_SERVICE,
        "validate_tax_id", ErrorPattern.SAT_TIMEOUT,
        "verify_tax_id", ErrorPattern.SAT_TIMEOUT
    );

    public ErrorMapping mapError(String toolName, String server, String rawError) {
        String context = (rawError != null ? rawError : "") + " " + (server != null ? server : "");

        if (toolName != null) {
            ErrorPattern toolSpecific = TOOL_SPECIFIC.get(toolName);
            if (toolSpecific != null && toolSpecific.matches(context)) {
                return buildMapping(toolSpecific);
            }
        }

        for (ErrorPattern pattern : ErrorPattern.values()) {
            if (pattern != ErrorPattern.DEFAULT && pattern.matches(context)) {
                return buildMapping(pattern);
            }
        }

        return buildMapping(ErrorPattern.DEFAULT);
    }

    private ErrorMapping buildMapping(ErrorPattern pattern) {
        return new ErrorMapping(
            pattern.getErrorCode(),
            pattern.getTitle(),
            pattern.getMessage(),
            pattern.getActionLabel(),
            pattern.getStatusType()
        );
    }

    public record ErrorMapping(
        String errorCode,
        String title,
        String message,
        String actionLabel,
        String statusType
    ) {}
}
