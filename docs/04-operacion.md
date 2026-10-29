# 4. Operación y solución de problemas

## 4.1 Requisitos

| Herramienta | Versión recomendada |
|---|---|
| JDK | 21 o superior para ejecutar; 25 funciona en local con el flag de Byte Buddy que ya está en `pom.xml` |
| Maven | 3.9+ |
| Ollama | Con el modelo descargado (`ollama pull qwen2.5:7b`) |
| Node.js | 20+ solo si vas a ejecutar `mcp-notifications` o los frontends |
| PostgreSQL | 15+ si usas el Control Center o el dominio de vehículos |

El `pom.xml` compila con `source/target 21`. El `java` del PATH en Windows puede
ser un JDK anterior; comprueba con:

```bash
java -version
"$JAVA_HOME/bin/java" -version
```

Si el jar falla con `UnsupportedClassVersionError`, estás ejecutando con un JDK
más viejo que el usado para compilar.

## 4.2 Compilar, probar y ejecutar

```bash
# Compilar sin pruebas
mvn -o package -DskipTests

# Suite unitaria (excluye la prueba de integración que requiere otro servicio)
mvn -o test -Dtest='!McpClientRegistryIntegrationTest'

# Ejecutar el jar
"$JAVA_HOME/bin/java" -jar target/orchestrator-0.1.0.jar

# Ejecutar en otro puerto
"$JAVA_HOME/bin/java" -jar target/orchestrator-0.1.0.jar --server.port=18080
```

Verificación rápida:

```bash
curl http://localhost:8080/actuator/health
curl http://localhost:8080/api/health
```

Si usas el `docker-compose.yml` raíz, Ollama debe estar corriendo en el host y el
contenedor lo busca en `host.docker.internal:11434`.

## 4.3 Configuración principal

Archivo: [application.yml](../src/main/resources/application.yml).

| Propiedad | Valor por defecto | Para qué |
|---|---|---|
| `server.port` | `8080` | Puerto HTTP |
| `ollama.base-url` | `http://localhost:11434` | API de Ollama |
| `ollama.model` | `qwen2.5:7b` | Modelo usado |
| `ollama.max-agent-turns` | `8` | Máximo de turnos del agente |
| `ollama.max-context-messages` | `20` | Mensajes máximos enviados al LLM |
| `ollama.timeout-seconds` | `120` | Timeout lógico por llamada |
| `ollama.max-retries` | `3` | Reintentos configurados en `FlowServiceConfig` |
| `ollama.retry-wait-seconds` | `2` | Espera base entre reintentos |
| `mcp.max-retries` | `2` | Reintentos por tool MCP |
| `mcp.step-timeout-seconds` | `30` | Timeout por paso MCP |
| `mcp.circuit-breaker-threshold` | `5` | Umbral de fallos por servidor MCP |
| `mcp.circuit-breaker-reset-seconds` | `60` | Tiempo antes de probar de nuevo |
| `orchestrator.state.directory` | `.flow-state` | Persistencia de pausas; vacío = solo memoria |
| `services.control-center.base-url` | `http://localhost:8081` | Control Center |
| `services.mcp-domain.base-url` | `http://localhost:9001` | MCP de dominio |
| `services.mcp-notifications.base-url` | `http://localhost:9002` | MCP de notificaciones |

Variables de entorno útiles (Spring aplica *relaxed binding*):

```bash
export OLLAMA_BASE_URL=http://localhost:11434
export OLLAMA_MODEL=qwen2.5:7b
export SERVICES_CONTROL_CENTER_BASE_URL=http://localhost:8081
export SERVICES_MCP_DOMAIN_BASE_URL=http://localhost:9001
export SERVICES_MCP_NOTIFICATIONS_BASE_URL=http://localhost:9002
```

### Resiliencia del LLM

En `resilience4j.circuitbreaker.instances.llm`:

- ventana de 20 llamadas, mínimo 5;
- umbral de fallos 50 %;
- llamada lenta por encima de 90 s;
- 30 s en estado abierto.

En `resilience4j.retry.instances.llm`:

- 2 intentos;
- backoff exponencial (2 s, factor 2, máximo 10 s);
- solo se reintentan errores transitorios de red/servicio.

Los valores están ajustados a la latencia de Ollama local. Si cambias de modelo o
de proveedor, revisa `slow-call-duration-threshold` y los timeouts.

## 4.4 Endpoints

### Iniciar un flujo (síncrono)

```bash
curl -X POST http://localhost:8080/api/flows \
  -H 'Content-Type: application/json' \
  -d '{
    "flowId": "compra-vehiculo",
    "executionId": "d716b9a5-5e74-41b8-83a1-592ff1f287ab",
    "customerId": "customer-1",
    "vehicleId": "model-101",
    "salesAgentId": "agent-1"
  }'
```

### Iniciar un flujo (asíncrono)

```bash
curl -i -X POST http://localhost:8080/api/flows/async \
  -H 'Content-Type: application/json' \
  -d '{"flowId":"compra-vehiculo","executionId":"exec-1","vehicleId":"model-101"}'
```

Respuesta `202 Accepted`:

```json
{
  "requestId": "bfb30ad9-ed66-455b-bc3b-d98f8e248173",
  "flowId": "compra-vehiculo",
  "executionId": "exec-1",
  "traceId": "f49e434b",
  "status": "ACCEPTED"
}
```

### Reanudar un flujo pausado

```bash
curl -X POST "http://localhost:8080/api/flows/exec-1/resume?userInput=model-101"
```

Sin `userInput` el agente no recibe datos nuevos y puede volver a pausar.

### Reintentar un paso

```bash
curl -X POST http://localhost:8080/api/flows/exec-1/steps/2/retry
```

Después hay que llamar a `resume` para que el flujo continúe.

### Cancelar un flujo pausado

```bash
curl -X POST http://localhost:8080/api/flows/exec-1/cancel
```

Responde `200` con `status: CANCELLED` y `flowCode: FLOW_CANCELLED`. Si la
ejecución no tiene estado almacenado responde `404`: no había nada que cancelar.
El estado descartado ya no puede reanudarse.

### Consumir eventos SSE

```bash
curl -N http://localhost:8080/api/flows/stream
```

Cada evento tiene tipo (`FLOW_STARTED`, `STEP_COMPLETED`, etc.) y un `traceId`
para correlacionar logs.

### Documentación interactiva

- Swagger UI: `http://localhost:8080/swagger-ui.html`
- OpenAPI JSON: `http://localhost:8080/v3/api-docs`

## 4.5 Estado persistido

Si `orchestrator.state.directory` no está vacío:

- se crea el directorio al arrancar;
- cada pausa/fallo se guarda como `<sha256(executionId)>.json`;
- la escritura es atómica;
- `resume`, `retry` o `cancel` consumen el archivo (lo borran después de leerlo);
- un archivo corrupto se elimina y se registra el error.

El archivo contiene los mensajes completos de la conversación, incluidos posibles
datos personales. Protege el directorio y mantén `/.flow-state/` fuera de git.

## 4.6 Logs y trazabilidad

El patrón de consola incluye `traceId=%X{traceId}`:

```text
2026-10-27T09:56:48.616-06:00 INFO [virtual-74] c.f.o.service.FlowService traceId=f49e434b - [START] Starting flow...
```

Prefijos útiles:

| Prefijo | Significado |
|---|---|
| `[START]`, `[ID]` | Inicio y resolución del `executionId` |
| `[AGENT]` | Turnos del agente |
| `[TOOLS]`, `[DISCOVER]` | Tools ofrecidas/descubiertas |
| `[CALL]`, `[OK]`, `[FAIL]` | Ejecución de una tool |
| `[PAUSE]` | Pausa por intervención humana |
| `[RESUME]` | Reanudación |
| `[RETRY-STEP]` | Paso marcado para reintento |
| `[AI-TOKENS]` | Consumo de tokens |
| `[SSE]` | Publicación/consumo de eventos |

Para seguir una ejecución concreta:

```bash
grep 'traceId=f49e434b' orchestrator.log
```

## 4.7 Problemas comunes

### El LLM tarda demasiado o el circuito se abre

Síntomas: `ERR_LLM_01`, `CallNotPermittedException`, logs `[AI-FAIL]`.

- Verifica que Ollama responde: `curl http://localhost:11434/api/tags`.
- Comprueba que el modelo está descargado.
- Ajusta `ollama.timeout-seconds` y los umbrales de slow-call a la latencia real.
- El circuito se reabre solo después de `wait-duration-in-open-state`.

### Una tool MCP falla por timeout

Síntomas: `MCP tool timeout`, `STEP_FAILED`, `mcp-<servidor>` abierto.

- Comprueba que el servidor MCP está levantado y en la URL correcta.
- Revisa `mcp.step-timeout-seconds`.
- Si el servidor reinició, `McpClientRegistry` recrea la sesión una vez; si el
  error persiste, el problema es del servidor.

### `resume` devuelve 404

Significado: no hay estado para ese `executionId`.

- El estado ya se consumió en un resume anterior.
- El orquestador se reinició y la persistencia estaba desactivada.
- El `executionId` no coincide.
- El flujo terminó y el estado se borró.

### El flujo se pausa pidiendo un dato

- Si el dato viene del usuario, hay que enviarlo en `userInput` al reanudar.
- Si el dato debería venir de una tool, revisa el contrato de la tool y el prompt.
- Si la tool declara argumentos que el flujo no recibe, el agente no puede
  completarla y pedirá el dato: corrige el schema en el servidor MCP.
- Tras cambiar el schema de una tool, reinicia el servidor MCP y, si la sesión
  quedó cacheada, también el orquestador. Las herramientas se redescubren en
  cada ejecución.

### `UnsupportedClassVersionError`

El jar se compiló con Java 21 pero se ejecuta con un JDK anterior. Usa
`"$JAVA_HOME/bin/java"` con un JDK 21+.

### `Java 25 is not supported by Byte Buddy`

Ocurre en pruebas con Mockito. El `pom.xml` ya incluye
`-Dnet.bytebuddy.experimental=true` en `argLine`. Si ejecutas pruebas desde el
IDE, agrega el mismo flag a la configuración de la JVM.

### Caracteres raros en consola (`informaci├│n`)

Es un problema de codificación, no de datos. El `pom.xml` ya fuerza UTF-8 en
tests y en el jar. Si ejecutas desde otro script, agrega:

```bash
-Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8
```

### El puerto está ocupado

```bash
netstat -ano | grep ':8080'
```

En Windows puedes terminar el proceso con `taskkill //PID <pid> //F` o arrancar en
otro puerto con `--server.port=18080`.

## 4.8 Seguridad

- El orquestador **no tiene autenticación**; debe vivir en una red interna.
- No expongas `/api/flows/stream` ni `/api/flows` a Internet sin un gateway.
- `PiiMasker` protege los logs, pero el estado persistido guarda los mensajes
  completos.
- Revisa los permisos del directorio `.flow-state`.
- No incluyas secretos en `application.yml`; usa variables de entorno.
- El LLM es local, pero las tools pueden escribir en sistemas reales (compras,
  pagos, correos). Las validaciones del orquestador son la última barrera antes
  de ejecutar una acción de negocio.
