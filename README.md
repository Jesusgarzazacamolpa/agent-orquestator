# Orchestrator - Orquestador de flujos IA

Servicio Java 21 + Spring Boot que coordina un agente LLM (Ollama local u OpenAI)
para ejecutar flujos de negocio mediante herramientas MCP. El LLM decide el
siguiente paso; el orquestador valida, ejecuta, registra y pausa cuando hace
falta una persona.

## Qué hace

- Carga flujos declarados en YAML (`flow-definitions.yml`) y prompts versionados.
- Ejecuta un bucle de agente con tool calling sobre servidores MCP.
- Valida herramientas, dependencias y argumentos antes de ejecutar.
- Aplica timeouts, reintentos con backoff y circuit breakers.
- Pausa y reanuda flujos con estado persistente.
- Publica eventos SSE para el Control Center.
- Enmascara PII en logs y devuelve errores amigables al usuario.

## Inicio rápido

```bash
# Requisitos: JDK 21+, Maven 3.9+, y Ollama con qwen2.5:7b u OPENAI_API_KEY
ollama pull qwen2.5:7b

mvn -o package -DskipTests

# Proveedor local (por defecto)
"$JAVA_HOME/bin/java" -jar target/orchestrator-0.1.0.jar

# Alternativa remota con OpenAI
export OPENAI_API_KEY=sk-...
"$JAVA_HOME/bin/java" -jar target/orchestrator-0.1.0.jar --llm.provider=openai

curl http://localhost:8080/actuator/health
```

```bash
curl -X POST http://localhost:8080/api/flows \
  -H 'Content-Type: application/json' \
  -d '{
    "flowId": "compra-vehiculo",
    "executionId": "exec-1",
    "customerId": "customer-1",
    "vehicleId": "model-101",
    "salesAgentId": "agent-1"
  }'
```

## Documentación

La guía completa está en [docs/](docs/README.md):

1. [Arquitectura y flujo completo](docs/01-arquitectura.md)
2. [Conceptos de IA aplicados al código](docs/02-conceptos-ia.md)
3. [Guía del código del orquestador](docs/03-codigo-orquestador.md)
4. [Operación y solución de problemas](docs/04-operacion.md)

## Arquitectura en una línea

```text
Frontends -> Control Center -> Orchestrator -> Ollama/OpenAI
                                   |-> mcp-domain -> vehicle-domain-service
                                   |-> mcp-notifications
                                   \-> SSE -> Control Center
```

## Pruebas

```bash
mvn -o test -Dtest='!McpClientRegistryIntegrationTest'
```

La prueba de integración `McpClientRegistryIntegrationTest` requiere el servicio
`mcp-notifications` levantado.
