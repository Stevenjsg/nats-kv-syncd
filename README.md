# NATS KV Syncd

Agente de sincronización para almacenes KV de NATS utilizando CRDTs (Conflict-Free Replicated Data Types) para garantizar consistencia eventual en sistemas distribuidos.

## Descripción

`nats-kv-syncd` mantiene sincronizados múltiples buckets KV distribuidos geográficamente (o en diferentes clusters). El agente vigila los cambios locales y los replica a otros nodos utilizando JetStream. En caso de conflictos, utiliza una regla determinista Last-Writer-Wins (LWW) basada en timestamps y node IDs.

## Lógica CRDT (Last-Writer-Wins)

El sistema utiliza un CRDT basado en registros (LWW-Register). Para cada clave, se mantiene un metadato que incluye:

- `ts`: Timestamp lógico (reloj de Lamport).
- `node_id`: Identificador único del nodo que originó el cambio.

La regla de resolución de conflictos es:

```ts
(ts_remoto > ts_local) OR
(ts_remoto == ts_local AND node_id_remoto > node_id_local)
 → GANA REMOTO
else
 → REFUTA (CONSERVA LOCAL)
```

Esto garantiza que todos los nodos converjan al mismo estado independientemente del orden de llegada de los mensajes.

### Estrategia de Metadatos

Los metadatos se almacenan en un bucket KV separado llamado `<nombre_bucket>_meta`. Esto permite mantener el estado necesario para la resolución de conflictos sin contaminar los datos del usuario en el bucket principal.

## Estrategia de Recuperación (Anti-Entropy)

El agente implementa dos mecanismos de recuperación:

1. **Replicación en Tiempo Real**: Uso de JetStream con consumidores duraderos para garantizar la entrega de mensajes.
2. **Reconciliación Periódica (Anti-Entropy)**: Un proceso en segundo plano se ejecuta cada 60 segundos. Itera sobre todas las claves locales y re-transmite su estado actual. Esto asegura que si un mensaje se pierde definitivamente (o un nodo estuvo desconectado más allá del límite de retención del stream), el sistema eventualmente convergerá.

## Requisitos

- Node.js >= 16
- NATS Server con JetStream habilitado

## Ejecución

### 1. Iniciar Infraestructura

```bash
docker-compose up -d
```

Esto levantará dos servidores NATS en cluster (`nats-a` y `nats-b`).

### 2. Crear Buckets (si no existen)

```bash
nats kv add config --server localhost:4222
nats kv add config --server localhost:5222
```

_Nota: Al estar en cluster, crear en uno debería propagarse si están configurados como espejos, pero para este laboratorio asumimos buckets independientes que sincronizamos vía el agente._

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

### 4. Probar Sincronización

En otra terminal:

```bash
# Escribir en A
nats kv put config saludo "Hola Mundo" --server localhost:4222

# Leer en B
nats kv get config saludo --server localhost:5222
```

## Desarrollo

```bash
npm install
npm run build
```
