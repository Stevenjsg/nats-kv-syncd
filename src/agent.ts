import {
  JSONCodec,
  StringCodec,
  ConsumerConfig,
  AckPolicy,
  DeliverPolicy,
  ReplayPolicy,
} from "nats";
import parseArgs from "minimist";
import { Sargs, KVOperation, KVMeta } from "./types";
import { SyncState } from "./state";
import { shouldApplyRemote } from "./crdt";
import { NatsInfrastructure } from "./infrastructure";
import { startPeriodicReconciliation } from "./reconciliation";

const jc = JSONCodec();
const sc = StringCodec();

const argv = parseArgs(process.argv.slice(2));
const args: Sargs = {
  natsUrl: argv["nats-url"] || "nats://localhost:4222",
  bucket: argv["bucket"] || "config",
  nodeId: argv["node-id"] || `node-${Math.floor(Math.random() * 1000)}`,
  repSubj: argv["rep-subj"] || "rep.kv.ops",
};

const SYNC_INTERVAL = process.env.SYNC_INTERVAL_MS
  ? parseInt(process.env.SYNC_INTERVAL_MS)
  : 60000;

// --- Globals ---
const state = new SyncState(args.nodeId);
const nats = new NatsInfrastructure();

// --- Helper Functions ---

async function getMeta(key: string): Promise<KVMeta | null> {
  try {
    const e = await nats.kvMeta.get(key);
    if (e) {
      return jc.decode(e.value) as KVMeta;
    }
  } catch (err) {
    // Key not found
  }
  return null;
}

async function setMeta(key: string, meta: KVMeta) {
  await nats.kvMeta.put(key, jc.encode(meta));
}

// --- Core Logic ---

async function handleLocalChange(
  key: string,
  value: Uint8Array | null,
  op: "PUT" | "DEL"
) {
  const currentTs = state.incrementClock();

  // Update local metadata
  const meta: KVMeta = {
    ts: currentTs,
    node_id: args.nodeId,
    deleted: op === "DEL",
  };

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
  await nats.js.publish(args.repSubj, jc.encode(crdtOp));
}

async function handleRemoteOperation(op: KVOperation) {
  if (op.node_id === args.nodeId) {
    return; // Ignore own echo
  }

  state.updateClock(op.ts);

  const localMeta = await getMeta(op.key);
  const shouldApply = shouldApplyRemote(op, localMeta);

  if (shouldApply) {
    console.log(
      `[REMOTE] WIN Key=${op.key} TS=${op.ts} Node=${op.node_id} (LocalTS=${localMeta?.ts})`
    );

    // 1. Mark as pending so Watcher ignores the subsequent KV put event
    state.addPendingWrite(op.key);

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
        await nats.kvStore.put(op.key, sc.encode(op.value));
      } else if (op.op === "DEL") {
        await nats.kvStore.delete(op.key);
      }
    } catch (e) {
      console.error(`Error applying remote op: ${e}`);
      state.removePendingWrite(op.key);
    }
  } else {
    // console.log(`[REMOTE] LOSE Key=${op.key} TS=${op.ts}`);
  }
}

async function main() {
  console.log(`Starting NATS KV Syncd Agent`);
  console.log(`NodeID: ${args.nodeId}`);
  console.log(`URL: ${args.natsUrl}`);
  console.log(`Bucket: ${args.bucket}`);

  try {
    await nats.init(args.natsUrl, args.bucket);

    // 2. Start Watcher
    console.log(`Watching bucket: ${args.bucket}`);
    (async () => {
      const iter = await nats.kvStore.watch();
      for await (const entry of iter) {
        if (
          entry.operation === "PUT" ||
          entry.operation === "DEL" ||
          entry.operation === "PURGE"
        ) {
          const key = entry.key;

          if (state.isPending(key)) {
            console.log(`[WATCHER] Ignoring remote update for ${key}`);
            state.removePendingWrite(key);
            continue;
          }

          const op =
            entry.operation === "DEL" || entry.operation === "PURGE"
              ? "DEL"
              : "PUT";
          await handleLocalChange(key, entry.value, op);
        }
      }
    })();

    // 3. Start Replication Subscriber (Durable)
    const streamName = "KV_SYNC_STREAM";
    try {
      await nats.jsm.streams.info(streamName);
    } catch {
      await nats.jsm.streams.add({
        name: streamName,
        subjects: [args.repSubj],
      });
      console.log(`Created Stream ${streamName} for subject ${args.repSubj}`);
    }

    const consumerName = `sync-consumer-${args.nodeId}`;
    const consumerOpts: ConsumerConfig = {
      durable_name: consumerName.replace(/[^a-zA-Z0-9_-]/g, "_"),
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
    };

    await nats.jsm.consumers.add(streamName, consumerOpts);
    console.log(
      `Subscribed to ${args.repSubj} as ${consumerOpts.durable_name}`
    );

    const psub = await nats.js.consumers.get(
      streamName,
      consumerOpts.durable_name
    );

    // Start Anti-Entropy
    startPeriodicReconciliation(nats, args, SYNC_INTERVAL);

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
