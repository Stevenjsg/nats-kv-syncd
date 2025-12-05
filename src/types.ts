export interface Sargs {
  natsUrl: string;
  bucket: string;
  nodeId: string;
  repSubj: string;
}

export interface KVOperation {
  op: "PUT" | "DEL";
  bucket: string;
  key: string;
  value: string | null;
  ts: number;
  node_id: string;
}

export interface KVMeta {
  ts: number;
  node_id: string;
  deleted: boolean;
}
