import { KVMeta, KVOperation } from "./types";

/**
 * Determines if a remote operation should win over local state based on LWW rules.
 * Rule: (RemoteTS > LocalTS) OR (RemoteTS == LocalTS AND RemoteNode > LocalNode)
 */
export function shouldApplyRemote(
  op: KVOperation,
  localMeta: KVMeta | null
): boolean {
  if (!localMeta) {
    return true;
  }

  if (op.ts > localMeta.ts) {
    return true;
  } else if (op.ts === localMeta.ts && op.node_id > localMeta.node_id) {
    return true;
  }

  return false;
}
