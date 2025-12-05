export class SyncState {
  private lamportClock: number = 0;
  private pendingRemoteWrites: Set<string> = new Set();
  private nodeId: string;

  constructor(nodeId: string) {
    this.nodeId = nodeId;
  }

  public getClock(): number {
    return this.lamportClock;
  }

  public incrementClock(): number {
    this.lamportClock++;
    return this.lamportClock;
  }

  public updateClock(receivedTs: number): void {
    this.lamportClock = Math.max(this.lamportClock, receivedTs) + 1;
  }

  public addPendingWrite(key: string): void {
    this.pendingRemoteWrites.add(key);
  }

  public removePendingWrite(key: string): void {
    this.pendingRemoteWrites.delete(key);
  }

  public isPending(key: string): boolean {
    return this.pendingRemoteWrites.has(key);
  }
}
