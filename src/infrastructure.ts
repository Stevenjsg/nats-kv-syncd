import {
  connect,
  NatsConnection,
  JetStreamClient,
  JetStreamManager,
  KV,
} from "nats";

export class NatsInfrastructure {
  public nc!: NatsConnection;
  public js!: JetStreamClient;
  public jsm!: JetStreamManager;
  public kvStore!: KV;
  public kvMeta!: KV;

  async init(natsUrl: string, bucket: string): Promise<void> {
    this.nc = await connect({ servers: natsUrl });
    this.js = this.nc.jetstream();
    this.jsm = await this.nc.jetstreamManager();

    // Init Stores
    this.kvStore = await this.js.views.kv(bucket);

    try {
      this.kvMeta = await this.js.views.kv(`${bucket}_meta`);
    } catch {
      this.kvMeta = await this.js.views.kv(`${bucket}_meta`, { history: 1 });
    }
  }

  async close(): Promise<void> {
    await this.nc.drain();
  }
}
