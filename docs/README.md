# Documentación del orquestador de flujos IA

Esta carpeta explica **qué hace cada parte del proyecto** y cómo encajan las piezas.
Está pensada para alguien que está aprendiendo integración de IA con Java, no solo
para consultar una API.

## Ruta de lectura recomendada

1. [Arquitectura y flujo completo](01-arquitectura.md)
   Qué servicios existen, quién llama a quién y cómo viaja una compra de vehículo
   de principio a fin.
2. [Conceptos de IA aplicados al código](02-conceptos-ia.md)
   LLM, prompts, tokens, tool calling, MCP, agente, reintentos y circuit breaker,
   cada concepto conectado con la clase Java que lo implementa.
3. [Guía del código del orquestador](03-codigo-orquestador.md)
   Recorrido clase por clase, métodos importantes, estados, eventos y recetas para
   cambiar o extender el flujo.
4. [Operación y solución de problemas](04-operacion.md)
   Cómo compilar, ejecutar, configurar, probar y diagnosticar los errores más
   comunes.

## Mapa rápido del repositorio

```text
orquestador-flujos-ia/
├── docs/                         Documentación de arquitectura del monorepo
├── database/                     Esquema SQL inicial
├── docker-compose.yml            Levanta todos los servicios
├── frontend/                     App Shell, Flow Portal, Flow Viewer e Intervención
└── services/
    ├── orchestrator/             Este servicio (Java + Spring AI + Ollama)
    ├── control-center/           Estado, bitácora e intervención humana (Java)
    ├── mcp-domain/              Herramientas MCP de negocio (Java)
    ├── mcp-notifications/       Herramienta MCP de correo (Node.js)
    ├── vehicle-domain-service/  Datos de vehículos, compras y pagos (Java)
    ├── catalog-service/         Catálogo de clientes/agentes/servicios (Java)
    ├── auth-service/            Autenticación JWT (Go)
    └── orchestrator-service/    Servicio TypeScript legado, no usado por el flujo actual
```

## Convenciones de esta guía

- Las rutas de código son relativas a `services/orchestrator/`.
- Los diagramas usan Mermaid; GitHub y muchos IDE los renderizan directamente.
- Los nombres de clases, métodos y variables se mantienen tal como están en el código.
- Cuando se menciona un comportamiento “actual”, corresponde al código de este
  repositorio, no a una versión desplegada distinta.
