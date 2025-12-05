import { execSync, spawn } from "child_process";
import { connect, StringCodec } from "nats";

const sc = StringCodec();

function run(cmd: string) {
  console.log(`> ${cmd}`);
  try {
    execSync(cmd, { stdio: "inherit" });
  } catch (e) {
    console.error(`Command failed: ${cmd}`);
    // don't exit, might be clean up
  }
}

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  console.log("=== Starting Partition/Recovery Test ===");

  // 1. Reset Environment
  console.log("\n[1/7] Resetting Docker Environment...");
  run("docker compose -f src/docker-compose.yml down -v");
  run("docker compose -f src/docker-compose.yml up -d");

  console.log("Waiting 5s for NATS to be ready...");
  await sleep(5000);

  // 2. Create Buckets
  console.log("\n[2/7] Creating KV Buckets...");
  try {
    run("nats kv add config --server localhost:4222 --history 5");
    run("nats kv add config --server localhost:5222 --history 5");
  } catch (e) {
    console.log("Buckets might already exist or error ignorable.");
  }

  // 3. Start Agents
  console.log("\n[3/7] Starting Agents...");
  const agentA = spawn("npm", ["run", "start:a"], {
    shell: true,
    stdio: "ignore",
    env: { ...process.env, SYNC_INTERVAL_MS: "5000" },
  });
  const agentB = spawn("npm", ["run", "start:b"], {
    shell: true,
    stdio: "ignore",
    env: { ...process.env, SYNC_INTERVAL_MS: "5000" },
  });

  console.log("Agents started with PIDs:", agentA.pid, agentB.pid);
  await sleep(5000);

  // 4. Stop NATS A
  console.log("\n[4/7] Simulating Partition: Stopping nats-a...");
  run("docker compose -f src/docker-compose.yml stop nats-a");
  await sleep(2000);

  // 5. Write to NATS B
  console.log("\n[5/7] Writing to nats-b (Site B) while nats-a is DOWN...");
  // Use nats cli or nats library. CLI is safer to simulate external client.
  try {
    run(
      'nats kv put config test_key "value_from_site_b" --server localhost:5222'
    );
  } catch (e) {
    console.error("Failed to write to nats-b");
    process.exit(1);
  }

  // 6. Restart NATS A
  console.log("\n[6/7] Healing Partition: Starting nats-a...");
  run("docker compose -f src/docker-compose.yml start nats-a");

  console.log("Waiting 15s for reconnection and sync...");
  await sleep(15000);

  // 7. Verify Convergence
  console.log("\n[7/7] Verifying Convergence on nats-a (Site A)...");

  // Connect to A and read
  try {
    const nc = await connect({ servers: "nats://localhost:4222" });
    const js = nc.jetstream();
    const kv = await js.views.kv("config");

    const entry = await kv.get("test_key");
    await nc.drain();

    if (entry && sc.decode(entry.value) === "value_from_site_b") {
      console.log("SUCCESS: Site A received value from Site B after recovery!");
    } else {
      console.error("FAILURE: Site A did not receive the expected value.");
      console.log("Actual:", entry ? sc.decode(entry.value) : "null");
      process.exit(1);
    }
  } catch (err) {
    console.error("Verification failed with error:", err);
    process.exit(1);
  }

  // Cleanup
  console.log("\nCleaning up...");
  agentA.kill();
  agentB.kill();
  // run("docker compose down");
  console.log("Done.");
  process.exit(0);
}

main();
