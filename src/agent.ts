import {
  connect,
  NatsConnection,
  JSONCodec,
  KV,
  JetStreamClient,
  JetStreamManager,
  ConsumerConfig,
  DeliverPolicy,
  AckPolicy,
  ReplayPolicy,
  StringCodec,
} from "nats";
import parseArgs from "minimist";

// --- Types ---

interface Sargs {
  natsUrl: string;
  bucket: string;
  nodeId: string;
  repSubj: string;
}

interface KVOperation {
  op: "PUT" | "DEL";
  bucket: string;
  key: string;
  value: string | null;
  ts: number;
  node_id: string;
}

interface KVMeta {
  ts: number;
  node_id: string;
  deleted: boolean;
}

// --- Globals ---
const jc = JSONCodec();
const sc = StringCodec();
let nc: NatsConnection;
let js: JetStreamClient;
let jsm: JetStreamManager;
let kvStore: KV;
let kvMeta: KV;
let lamportClock = 0;
const pendingRemoteWrites = new Set<string>();

// --- Helper Functions ---

function updateClock(receivedTs: number) {
  lamportClock = Math.max(lamportClock, receivedTs) + 1;
}

async function getMeta(key: string): Promise<KVMeta | null> {
  try {
    const e = await kvMeta.get(key);
    if (e) {
      return jc.decode(e.value) as KVMeta;
    }
  } catch (err) {
    // Key not found
  }
  return null;
}

async function setMeta(key: string, meta: KVMeta) {
  await kvMeta.put(key, jc.encode(meta));
}

// --- CLI Args ---
const argv = parseArgs(process.argv.slice(2));
const args: Sargs = {
  natsUrl: argv["nats-url"] || "nats://localhost:4222",
  bucket: argv["bucket"] || "config",
  nodeId: argv["node-id"] || `node-${Math.floor(Math.random() * 1000)}`,
  repSubj: argv["rep-subj"] || "rep.kv.ops",
};

// --- Core Logic ---

async function handleLocalChange(
  key: string,
  value: Uint8Array | null,
  op: "PUT" | "DEL"
) {
  lamportClock++;
  const currentTs = lamportClock;

  // Update local metadata
  const meta: KVMeta = {
    ts: currentTs,
    node_id: args.nodeId,
    deleted: op === "DEL",
  };

  // We update meta. This might trigger the watcher again if we watched meta?
  // We only watch 'kvStore' (config), not 'kvMeta' (config_meta). So safe.
  await setMeta(key, meta);

  const crdtOp: KVOperation = {
    op: op,
    bucket: args.bucket,
    key: key,
    value: value ? sc.decode(value) : null,
    ts: currentTs,
    node_id: args.nodeId,
  };

  console.log(`[LOCAL] ${op} Key=${key} TS=${currentTs}`);

  // Publish to replication subject
  await js.publish(args.repSubj, jc.encode(crdtOp));
}

async function handleRemoteOperation(op: KVOperation) {
  if (op.node_id === args.nodeId) {
    return; // Ignore own echo
  }

  updateClock(op.ts);

  const localMeta = await getMeta(op.key);

  // LWW Rule: Request Wins if (RemoteTS > LocalTS) OR (RemoteTS == LocalTS AND RemoteNode > LocalNode)
  let shouldApply = false;
  let keepLocal = false;

  if (!localMeta) {
    shouldApply = true;
  } else {
    if (op.ts > localMeta.ts) {
      shouldApply = true;
    } else if (op.ts === localMeta.ts && op.node_id > localMeta.node_id) {
      shouldApply = true;
    } else {
      keepLocal = true;
    }
  }

  if (shouldApply) {
    console.log(
      `[REMOTE] WIN Key=${op.key} TS=${op.ts} Node=${op.node_id} (LocalTS=${localMeta?.ts})`
    );

    // 1. Mark as pending so Watcher ignores the subsequent KV put event
    pendingRemoteWrites.add(op.key);

    // 2. Update Metadata FIRST
    const newMeta: KVMeta = {
      ts: op.ts,
      node_id: op.node_id,
      deleted: op.op === "DEL",
    };
    await setMeta(op.key, newMeta);

    // 3. Apply to KV
    try {
      if (op.op === "PUT" && op.value !== null) {
        await kvStore.put(op.key, sc.encode(op.value));
      } else if (op.op === "DEL") {
        await kvStore.delete(op.key);
      }
    } catch (e) {
      console.error(`Error applying remote op: ${e}`);
      // If failed, we should probably remove from pending set?
      pendingRemoteWrites.delete(op.key);
    }
  } else {
    if (keepLocal) {
      console.log(
        `[REMOTE] LOSE Key=${op.key} TS=${op.ts} (LocalTS=${localMeta?.ts})`
      );
      // We have a newer local value. We could rebroadcast it to help convergence?
      // "Anti-entropy". For now, basic LWW is enough.
    }
  }
}

async function main() {
  console.log(`Starting NATS KV Syncd Agent`);
  console.log(`NodeID: ${args.nodeId}`);
  console.log(`URL: ${args.natsUrl}`);
  console.log(`Bucket: ${args.bucket}`);

  try {
    nc = await connect({ servers: args.natsUrl });
    js = nc.jetstream();
    jsm = await nc.jetstreamManager();

    // 1. Init Stores
    kvStore = await js.views.kv(args.bucket);

    try {
      kvMeta = await js.views.kv(`${args.bucket}_meta`);
    } catch {
      kvMeta = await js.views.kv(`${args.bucket}_meta`, { history: 1 });
    }

    // 2. Start Watcher
    console.log(`Watching bucket: ${args.bucket}`);
    (async () => {
      const iter = await kvStore.watch();
      for await (const entry of iter) {
        // Determine if this is a "real" change or just initialization/noise
        // 'operation' property exists on KvEntry
        if (
          entry.operation === "PUT" ||
          entry.operation === "DEL" ||
          entry.operation === "PURGE"
        ) {
          const key = entry.key;

          if (pendingRemoteWrites.has(key)) {
            // It was us applying a remote op. Ignore.
            console.log(`[WATCHER] Ignoring remote update for ${key}`);
            pendingRemoteWrites.delete(key);
            continue;
          }

          // Must be a local change (User interaction)
          const op =
            entry.operation === "DEL" || entry.operation === "PURGE"
              ? "DEL"
              : "PUT";
          await handleLocalChange(key, entry.value, op);
        }
      }
    })();

    // 3. Start Replication Subscriber (Durable)
    // We create a stream first if it doesn't exist?
    // Or we just expect the subject to be usable.
    // For JetStream, we need a stream covering the subject.
    const streamName = "KV_SYNC_STREAM";
    try {
      await jsm.streams.info(streamName);
    } catch {
      await jsm.streams.add({
        name: streamName,
        subjects: [args.repSubj],
      });
      console.log(`Created Stream ${streamName} for subject ${args.repSubj}`);
    }

    // Durable Consumer
    const consumerName = `sync-consumer-${args.nodeId}`;

    const consumerOpts: ConsumerConfig = {
      durable_name: consumerName.replace(/[^a-zA-Z0-9_-]/g, "_"), // sanitize
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
    };

    // Note: Creating consumer explicitly is safer
    await jsm.consumers.add(streamName, consumerOpts);

    console.log(
      `Subscribed to ${args.repSubj} as ${consumerOpts.durable_name}`
    );

    const psub = await js.consumers.get(streamName, consumerOpts.durable_name);
    const messages = await psub.consume();

    for await (const m of messages) {
      try {
        const op = jc.decode(m.data) as KVOperation;
        await handleRemoteOperation(op);
        m.ack();
      } catch (err) {
        console.error("Error handling msg", err);
        m.nak();
      }
    }
  } catch (err) {
    console.error(`Fatal Error: ${err}`);
    process.exit(1);
  }
}

main();
