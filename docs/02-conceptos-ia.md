# 2. Conceptos de IA aplicados al código

Esta sección explica los conceptos que aparecen en el proyecto y dónde vive cada
uno. Si vienes de backend tradicional, la idea central es esta:

> El LLM no ejecuta nada por sí solo. Solo **propone** una llamada a herramienta.
> El orquestador decide si la acepta, la ejecuta y le devuelve el resultado.

## 2.1 LLM y Ollama

Un **LLM** (Large Language Model) es un modelo que recibe texto y devuelve texto.
En este proyecto se ejecuta localmente con **Ollama**, que expone una API HTTP en
`http://localhost:11434`.

- Modelo por defecto: `qwen2.5:7b` ([application.yml](../src/main/resources/application.yml)).
- Se configura como bean en
  [OrchestratorConfiguration.java](../src/main/java/com/floworchestrator/orchestrator/config/OrchestratorConfiguration.java).
- El cliente HTTP tiene timeouts reales de conexión y lectura; no basta con
  cancelar un `Future`, porque eso no corta la petición de red en curso.

Por qué local: evita enviar datos del cliente a un proveedor externo y permite
trabajar sin conexión. El costo es latencia: en un equipo de desarrollo cada turno
puede tardar entre 30 y 60 segundos.

## 2.2 Prompt

Un **prompt** es la instrucción que recibe el modelo. Aquí se separa en:

| Parte | Archivo | Propósito |
|---|---|---|
| System prompt | [compra-vehiculo-system.st](../src/main/resources/prompts/compra-vehiculo-system.st) | Reglas estables: qué tools existen, que no invente datos, cómo debe llamar herramientas |
| User prompt | [compra-vehiculo-user.st](../src/main/resources/prompts/compra-vehiculo-user.st) | Datos concretos de la ejecución: cliente, vehículo, agente |

Los prompts son **código versionado** y se renderizan con StringTemplate 4
(`ST4`). El método `renderPrompt` de `FlowService` admite tres formas:

1. Ruta a un recurso del classpath (lo normal).
2. `inline:...` para plantillas pequeñas embebidas.
3. Plantilla vacía: genera un texto con los atributos disponibles.

Variables disponibles en este flujo: `flowId`, `flowName`, `toolNames`,
`customerId`, `vehicleId`, `salesAgentId` y cualquier atributo extra que llegue en
la petición.

## 2.3 Temperatura y top-p

Son parámetros que controlan la creatividad:

- `temperature = 0.0`: respuestas lo más deterministas posible.
- `top_p = 0.1`: el modelo considera solo las palabras más probables.

Para un flujo de negocio queremos reproducibilidad, no creatividad. Por eso ambos
están bajos tanto en el bean de Ollama como en `application.yml`.

## 2.4 Contexto y ventana de mensajes

El LLM no recuerda entre llamadas: cada turno se le envía **toda la conversación**
(system, user, assistant, tool). Eso se llama **contexto**.

`FlowService.trimContextIfNeeded` limita el contexto a
`ollama.max-context-messages` (20 por defecto):

- conserva siempre el mensaje `system`;
- conserva los mensajes más recientes;
- evita dejar respuestas `tool` huérfanas sin su llamada asociada.

Esto reduce tokens y evita que el modelo se pierda con conversaciones muy largas.

## 2.5 Tokens y auditoría

Un **token** es una unidad de texto (aproximadamente una palabra o parte de ella).
Los proveedores cobran por tokens; en local el costo es cómputo, pero igual se
miden para saber cuánto consume cada flujo.

`AiService.auditTokens` registra:

- tokens de prompt y de generación por llamada;
- acumulado por `flowId`;
- acumulado global.

Se escriben en el log con el prefijo `[AI-TOKENS]`. Hoy los acumulados viven en
memoria y no se exponen por actuator; se consultan con los getters de `AiService`.

## 2.6 Tool calling (function calling)

**Tool calling** es la capacidad del modelo de responder “no me des texto, dame
una llamada a esta función con estos argumentos”.

El orquestador hace lo siguiente:

1. Descubre las tools MCP y sus esquemas JSON.
2. Filtra solo las permitidas en el flujo y cuyas dependencias ya se completaron.
3. Se las ofrece al modelo como `ToolCallback`, **sin permitir ejecución interna**.
4. Si el modelo responde con `tool_calls`, el orquestador los valida y ejecuta.
5. Devuelve el resultado como mensaje de rol `tool`.
6. El modelo recibe ese resultado y decide el siguiente paso.

Ejemplo conceptual de lo que devuelve el modelo:

```json
{
  "tool_calls": [
    {
      "id": "call_1",
      "function": {
        "name": "get_vehicle_information",
        "arguments": { "vehicle_id": "model-101" }
      }
    }
  ]
}
```

Reglas de seguridad aplicadas por el orquestador:

- **Lista blanca**: una tool que no está en `flow-definitions.yml` se rechaza.
- **Dependencias**: no se ejecuta un paso si su `depends-on` no está completo.
- **Argumentos**: se valida contra el JSON Schema de la tool. Si el modelo inventa
  un parámetro o omite uno requerido, el error se le devuelve como mensaje de tool
  para que se corrija.
- **Ejecución**: el `ToolCallback` lanza una excepción si alguien intenta ejecutar
  la tool desde Spring AI; la ejecución real siempre pasa por el orquestador.

## 2.7 MCP (Model Context Protocol)

**MCP** es un protocolo estándar para que un cliente (el orquestador) descubra y
ejecute herramientas expuestas por un servidor. Aquí hay dos servidores:

- `mcp-domain`: información de vehículos, registro de compra y método de pago.
- `mcp-notifications`: envío de correo con el estado de la compra.

El cliente vive en
[McpClientRegistry.java](../src/main/java/com/floworchestrator/orchestrator/service/McpClientRegistry.java):

- crea un `McpSyncClient` por servidor usando **Streamable HTTP** en `/mcp`;
- lista tools con `listTools()`;
- ejecuta con `callTool()`;
- si la sesión se invalida, cierra el cliente y reintenta una vez con una sesión
  nueva;
- convierte el resultado de texto JSON a `Map<String, Object>`.

La interfaz `McpToolClient` permite sustituir el registro por un doble de prueba.

## 2.8 Bucle del agente

Un **agente** es un LLM que puede actuar varias veces antes de responder. El
bucle está en `FlowService.runAgentLoop` y tiene un máximo de
`ollama.max-agent-turns` (8) para evitar bucles infinitos.

```text
turno 1: LLM -> tool_call(get_vehicle_information) -> orquestador ejecuta -> resultado al LLM
turno 2: LLM -> tool_call(register_vehicle_purchase) -> ...
turno 3: LLM -> tool_call(set_payment_method) -> ...
turno 4: LLM -> tool_call(send_purchase_status_email) -> ...
turno 5: LLM -> texto final -> flujo COMPLETED
```

En cada turno pueden darse tres salidas:

| Salida del LLM | Qué hace el orquestador |
|---|---|
| `tool_calls` | Valida y ejecuta; continúa el bucle |
| Texto que pide datos al usuario | Lanza `RequiresHumanInterventionException` y pausa |
| Texto final | Marca `COMPLETED`, pero solo si todas las tools requeridas se ejecutaron |

Si el modelo no emite `tool_calls` nativos, hay un **parser de respaldo**
(`parseToolCallsFromContent`) que busca fragmentos JSON con `name` y
`arguments`/`parameters`. Es una red de seguridad para modelos pequeños.

## 2.9 Resiliencia: timeout, retry y circuit breaker

El proyecto usa **Resilience4j** en dos niveles.

### Llamadas al LLM (`AiService`)

- Timeout propio (`ollama.timeout-seconds`, 120 s) y timeout de socket en el
  cliente HTTP.
- Reintentos solo para fallos transitorios de red/servicio.
- Circuit breaker `llm`: si hay demasiados fallos, deja de llamar al modelo
  durante `wait-duration-in-open-state` (30 s) y devuelve una respuesta degradada.

El orden es **circuit breaker por fuera, retry por dentro**:

```java
CircuitBreaker.decorateSupplier(circuitBreaker,
        Retry.decorateSupplier(retry, llamada))
```

Así el circuito ve el resultado final de la operación (con sus reintentos), no
cada intento individual.

### Llamadas a tools MCP (`FlowService`)

- Timeout por paso (`mcp.step-timeout-seconds`, 30 s).
- Retry con backoff exponencial por servidor.
- Circuit breaker por servidor (`mcp-<nombre>`).

### Degradación

Si el LLM falla pero **ya se completaron todas las tools requeridas**, el flujo se
cierra con un resumen de respaldo en lugar de marcarse como fallido. Es lo que
evita perder una compra ya registrada porque falló el turno final de resumen.

## 2.10 Human in the loop

No todo lo puede resolver la IA. Cuando falta un dato que solo el usuario tiene
(por ejemplo, elegir un modelo o confirmar una condición), el flujo se pausa:

- excepción `RequiresHumanInterventionException`;
- estado persistido en `FlowStateManager`;
- evento `FLOW_PAUSED`;
- intervención creada por el Control Center;
- reanudación con `resume` y, opcionalmente, `userInput`.

También se pausa cuando una tool devuelve `requiresIntervention: true`.

## 2.11 Observabilidad

- **traceId**: se genera un identificador corto por ejecución y se coloca en MDC.
  Todos los logs incluyen `traceId=...`, incluso los que corren en virtual threads.
- **SSE**: cada cambio relevante se publica como evento (`FLOW_STARTED`,
  `STEP_STARTED`, `STEP_COMPLETED`, `STEP_FAILED`, `FLOW_PAUSED`,
  `FLOW_COMPLETED`, `FLOW_FAILED`).
- **PII masking**: `PiiMasker` enmascara correos, CURP, RFC, cuentas, tarjetas y
  teléfonos antes de escribir contenido del LLM en logs.
- **Actuator**: health, métricas, estado de circuit breakers y retries.

## 2.12 Lo que este proyecto no hace (todavía)

Para evitar expectativas incorrectas:

- No usa **RAG** ni base vectorial: no hay embeddings ni búsqueda semántica.
- No tiene memoria entre ejecuciones distintas; el estado es por `executionId`.
- No entrena ni ajusta el modelo.
- No expone autenticación propia en el orquestador.
- No tiene un catálogo dinámico de flows: los flujos se declaran en YAML.

## Mapa concepto → código

| Concepto | Clase / archivo |
|---|---|
| Cliente LLM | `AiService`, `OrchestratorConfiguration.chatModel` |
| Prompt | `FlowService.buildPromptMessages`, `renderPrompt`, `prompts/*.st` |
| Tool calling | `AiService.buildPrompt`, `FlowService.runAgentLoop` |
| Descubrimiento MCP | `McpClientRegistry.listTools`, `FlowService.discoverTool` |
| Ejecución MCP | `McpClientRegistry.callTool`, `FlowService.executeToolStep` |
| Validación | `FlowService.validateToolCall`, `validateArguments` |
| Agente | `FlowService.runAgentLoop`, `processToolCalls` |
| Resiliencia LLM | `AiService.generateResponse`, `callWithTimeout`, `application.yml` |
| Resiliencia MCP | `FlowService.executeToolWithRetry`, `circuitBreakerFor`, `retryFor` |
| Pausa humana | `RequiresHumanInterventionException`, `FlowStateManager` |
| Eventos | `FlowEventPublisher`, `FlowStreamController` |
| PII | `PiiMasker` |
| Errores amigables | `UserFacingErrorMapper` |
