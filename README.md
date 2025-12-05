# Proyecto SAD - NATS KV Syncd

Este proyecto implementa un agente de sincronización de almacenes Key-Value (KV) de NATS utilizando CRDTs (Conflict-free Replicated Data Types) con una estrategia Last-Writer-Wins (LWW).

### Hecho por

- Steven Jose Silva Gomez
- Valeria

## Cambios Realizados

1.  **Arquitectura de Cluster**: Se modificó `docker-compose.yml` para configurar los dos servidores NATS (`nats-a` y `nats-b`) como un clúster. Esto permite que los mensajes publicados en un servidor sean recibidos en el otro a través de rutas de red internas.
2.  **Agente de Sincronización (`src/agent.ts`)**: Se implementó la lógica del agente en TypeScript.
    - **Watcher Local**: Escucha cambios en el bucket KV `config` local.
    - **Metadatos CRDT**: Utiliza un bucket separado `config_meta` para almacenar `timestamps` (Reloj Lamport) y `node_id` de cada clave.
    - **Replicación JetStream**: Publica operaciones en `rep.kv.ops` y se suscribe a ellas usando consumidores duraderos.
    - **Resolución de Conflictos LWW**: Al recibir una operación remota, compara el timestamp y node_id con los metadatos locales. Si la operación remota gana, actualiza el KV local y los metadatos.
    - **Prevención de Bucles**: Mantiene un registro de claves pendientes para evitar re-enviar cambios que provienen de la replicación.

## Dependencias Instaladas

Además de las dependencias base (`nats`, `ts-node`, `typescript`), se instaló:

- `minimist`: Para el parseo de argumentos de línea de comandos.
- `@types/minimist`: Tipos TypeScript para `minimist`.

## Cómo Ejecutar

### 1. Iniciar Infraestructura

```bash
docker-compose -f src/docker-compose.yml up -d
```

### 2. Iniciar Agentes

En una terminal (Site A):

```bash
npm run start:a
```

En otra terminal (Site B):

```bash
npm run start:b
```

### 3. Verificar Sincronización

**Site A**:

```bash
nats kv put config theme dark --server localhost:4222
```

**Site B**:
Verificar que el valor se ha propagado:

```bash
nats kv get config theme --server localhost:5222
```
