package com.floworchestrator.orchestrator.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PiiMaskerTest {

    private final PiiMasker masker = new PiiMasker();

    @Test
    @DisplayName("debe enmascarar correos electrónicos")
    void shouldMaskEmail() {
        String masked = masker.mask("Contacto: john.doe@example.com para continuar");

        assertTrue(masked.contains("[EMAIL_REDACTED]"));
        assertFalse(masked.contains("john.doe@example.com"));
    }

    @Test
    @DisplayName("debe enmascarar teléfonos")
    void shouldMaskPhone() {
        String masked = masker.mask("Llama al 5512345678 cuando termines");

        assertTrue(masked.contains("[PHONE_REDACTED]"));
        assertFalse(masked.contains("5512345678"));
    }

    @Test
    @DisplayName("debe enmascarar tarjetas y cuentas")
    void shouldMaskCardAndAccount() {
        String masked = masker.mask("Tarjeta 4111 1111 1111 1111 y CLABE 012345678901234567");

        assertTrue(masked.contains("[CARD_REDACTED]"));
        assertTrue(masked.contains("[ACCOUNT_REDACTED]"));
        assertFalse(masked.contains("4111"));
        assertFalse(masked.contains("012345678901234567"));
    }

    @Test
    @DisplayName("debe enmascarar RFC y CURP")
    void shouldMaskRfcAndCurp() {
        String masked = masker.mask("RFC GARC850101ABC, CURP GARC850101HDFRRN01");

        assertTrue(masked.contains("[RFC_REDACTED]"));
        assertTrue(masked.contains("[CURP_REDACTED]"));
        assertFalse(masked.contains("GARC850101ABC"));
        assertFalse(masked.contains("GARC850101HDFRRN01"));
    }

    @Test
    @DisplayName("debe enmascarar valores anidados en mapas y listas")
    void shouldMaskNestedValues() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("email", "john.doe@example.com");
        nested.put("items", List.of(Map.of("phone", "5512345678")));

        Map<String, Object> masked = masker.maskMap(Map.of("customer", nested));

        String serialized = masked.toString();
        assertTrue(serialized.contains("[EMAIL_REDACTED]"));
        assertTrue(serialized.contains("[PHONE_REDACTED]"));
        assertFalse(serialized.contains("john.doe@example.com"));
        assertFalse(serialized.contains("5512345678"));
    }

    @Test
    @DisplayName("debe manejar entradas nulas o vacías")
    void shouldHandleNullAndEmptyInput() {
        assertNull(masker.mask(null));
        assertEquals("", masker.mask(""));
        assertEquals(Map.of(), masker.maskMap(null));
    }

    @Test
    @DisplayName("no debe alterar texto sin PII")
    void shouldKeepPlainText() {
        String text = "El vehículo está disponible";
        assertEquals(text, masker.mask(text));
    }
}
