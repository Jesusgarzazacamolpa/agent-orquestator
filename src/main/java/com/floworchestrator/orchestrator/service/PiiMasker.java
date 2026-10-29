package com.floworchestrator.orchestrator.service;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Enmascara información de identificación personal (PII) antes de escribir
 * prompts o respuestas en logs, trazas o almacenamiento.
 */
@Component
public class PiiMasker {

    private record Rule(Pattern pattern, String replacement) {
    }

    private static final List<Rule> RULES = List.of(
            new Rule(Pattern.compile("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b"),
                    "[EMAIL_REDACTED]"),
            new Rule(Pattern.compile("\\b[A-Z]{4}\\d{6}[HM][A-Z]{5}[A-Z0-9]\\d\\b", Pattern.CASE_INSENSITIVE),
                    "[CURP_REDACTED]"),
            new Rule(Pattern.compile("\\b[A-ZÑ&]{3,4}\\d{6}[A-Z0-9]{3}\\b", Pattern.CASE_INSENSITIVE),
                    "[RFC_REDACTED]"),
            new Rule(Pattern.compile("\\b\\d{18}\\b"), "[ACCOUNT_REDACTED]"),
            new Rule(Pattern.compile("\\b(?:\\d[ -]?){13,19}\\b"), "[CARD_REDACTED]"),
            new Rule(Pattern.compile("(?<!\\d)(?:\\+?52[ -]?)?(?:\\d{2,3}[ -]?)?\\d{3}[ -]?\\d{4}(?!\\d)"),
                    "[PHONE_REDACTED]")
    );

    /** Enmascara correos, CURP, RFC, cuentas, tarjetas y teléfonos en un texto libre. */
    public String mask(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        String masked = text;
        for (Rule rule : RULES) {
            masked = rule.pattern().matcher(masked).replaceAll(rule.replacement());
        }
        return masked;
    }

    /** Aplica el enmascarado recursivamente a los valores de un mapa (mapas y listas anidados). */
    public Map<String, Object> maskMap(Map<String, Object> source) {
        if (source == null) {
            return Map.of();
        }
        Map<String, Object> masked = new LinkedHashMap<>();
        source.forEach((key, value) -> masked.put(key, maskValue(value)));
        return masked;
    }

    private Object maskValue(Object value) {
        if (value instanceof String text) {
            return mask(text);
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> nested = new LinkedHashMap<>();
            map.forEach((key, item) -> nested.put(String.valueOf(key), maskValue(item)));
            return nested;
        }
        if (value instanceof List<?> list) {
            List<Object> maskedList = new ArrayList<>(list.size());
            list.forEach(item -> maskedList.add(maskValue(item)));
            return maskedList;
        }
        return value;
    }
}
