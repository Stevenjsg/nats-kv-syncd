# NATS KV Syncd

Agente de sincronización para almacenes KV de NATS utilizando CRDTs (Conflict-Free Replicated Data Types) para garantizar consistencia eventual en sistemas distribuidos.

## Descripción

`nats-kv-syncd` mantiene sincronizados múltiples buckets KV distribuidos geográficamente (o en diferentes clusters). El agente vigila los cambios locales y los replica a otros nodos utilizando JetStream. En caso de conflictos, utiliza una regla determinista Last-Writer-Wins (LWW) basada en timestamps y node IDs.

Este proyecto ha sido diseñado con una arquitectura modular siguiendo principios de **Clean Code** y robustez para sistemas distribuidos.

## Características Principales

1.  **Sincronización Bidireccional**: Detecta cambios locales y aplica cambios remotos.
2.  **CRDT Last-Writer-Wins (LWW)**: Resolución determinista de conflictos.
3.  **Tolerancia a Fallos (Partition Tolerance)**: Los sitios pueden operar desconectados y sincronizarse al volver.
4.  **Anti-Entropía (Reconciliación)**: Proceso periódico que garantiza convergencia eventual ante pérdida de mensajes.
5.  **Arquitectura Modular**: Código refactorizado para facilitar mantenimiento y pruebas.

## Estructura del Proyecto

El código fuente (`src/`) está organizado en módulos:

- `agent.ts`: Punto de entrada principal. Coordina los componentes.
- `infrastructure.ts`: Gestión de conexiones NATS y JetStream.
- `crdt.ts`: Lógica pura de resolución de conflictos (LWW).
- `state.ts`: Gestión del estado local (Reloj de Lamport, escrituras pendientes).
- `reconciliation.ts`: Lógica de anti-entropía para la recuperación de datos.
- `types.ts`: Definiciones de tipos compartidas.

## Requisitos

- Node.js >= 16
- Docker y Docker Compose
- NATS Server (incluido en docker-compose)

## Instalación

```bash
npm install
```

## Ejecución

### 1. Iniciar Infraestructura

Levanta dos servidores NATS simulando dos sitios distintos (`site-a` y `site-b`):

```bash
docker compose -f src/docker-compose.yml up -d
```

### 2. Crear Buckets

```bash
nats kv add config --server localhost:4222
nats kv add config --server localhost:5222
```

### 3. Ejecutar Agentes

En terminales separadas:

**Sitio A:**

```bash
npm run start:a
```

**Sitio B:**

```bash
npm run start:b
```

## Pruebas Automáticas (Test Scenario)

El proyecto incluye un script de prueba automatizado que verifica la tolerancia a particiones (Split-Brain) y la convergencia de datos.

Este script realiza los siguientes pasos:

1.  Reinicia el entorno Docker.
2.  Crea los buckets KV necesarios.
3.  Inicia los agentes de sincronización.
4.  **Simula una partición**: Detiene el nodo A.
5.  Escribe datos en el nodo B (mientras A está caído).
6.  **Recupera la partición**: Reinicia el nodo A.
7.  **Verifica la convergencia**: Comprueba que A haya recibido los datos de B.

Para ejecutar la prueba:

```bash
npx ts-node scripts/test_scenario.ts
```

_Nota: Asegúrate de tener el puerto 4222 y 5222 libres antes de ejecutar el test._

## Lógica CRDT (Last-Writer-Wins)

Para cada clave, se mantiene un metadato en un bucket paralelo (`<bucket>_meta`) que incluye:

- `ts`: Timestamp lógico (reloj de Lamport).
- `node_id`: Identificador único del nodo que originó el cambio.

La regla de resolución es:

```ts
(ts_remoto > ts_local) OR
(ts_remoto == ts_local AND node_id_remoto > node_id_local)
 → GANA REMOTO
else
 → REFUTA (CONSERVA LOCAL)
```

## Estrategia de Recuperación

Además de la replicación en tiempo real vía JetStream, el agente ejecuta una tarea de **Anti-Entropía** cada 60 segundos (configurable vía `SYNC_INTERVAL_MS`). Esta tarea retransmite el estado actual de todas las claves locales para asegurar que cualquier mensaje perdido durante una desconexión sea eventualmente recuperado por los otros nodos.
