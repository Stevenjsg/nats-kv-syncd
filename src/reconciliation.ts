import { JSONCodec, StringCodec } from "nats";
import { NatsInfrastructure } from "./infrastructure";
import { Sargs, KVOperation, KVMeta } from "./types";

const jc = JSONCodec();
const sc = StringCodec();

export function startPeriodicReconciliation(
  nats: NatsInfrastructure,
  args: Sargs,
  intervalMs: number = 60000
) {
  console.log(
    `Starting Periodic Reconciliation Loop (every ${intervalMs}ms)...`
  );
  setInterval(async () => {
    try {
      console.log("[ANTI-ENTROPY] Running reconciliation...");
      const keys = await nats.kvStore.keys();
      for await (const key of keys) {
        // Get Meta
        let meta: KVMeta | null = null;
        try {
          const e = await nats.kvMeta.get(key);
          if (e) {
            meta = jc.decode(e.value) as KVMeta;
          }
        } catch {
          // ignore
        }

        if (!meta) continue;

        let value: Uint8Array | null = null;
        let op: "PUT" | "DEL" = "PUT";

        if (meta.deleted) {
          op = "DEL";
        } else {
          const entry = await nats.kvStore.get(key);
          if (entry) {
            value = entry.value;
          } else {
            // Inconsistency? Meta says not deleted, but KV has no value.
            continue;
          }
        }

        const crdtOp: KVOperation = {
          op: op,
          bucket: args.bucket,
          key: key,
          value: value ? sc.decode(value) : null,
          ts: meta.ts,
          node_id: meta.node_id,
        };

        // Re-broadcast
        await nats.js.publish(args.repSubj, jc.encode(crdtOp));
      }
    } catch (err) {
      console.error("[ANTI-ENTROPY] Error:", err);
    }
  }, intervalMs);
}
