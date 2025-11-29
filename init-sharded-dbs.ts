import { Database } from "bun:sqlite";
import { existsSync, unlinkSync } from "fs";

// Configuration - 3 shards, one per hour
const NUM_SHARDS = 3;
const EVENTS_PER_SHARD = 100;

// Base timestamp: 2024-01-15 00:00:00 UTC in milliseconds
const HOUR_MS = 3600 * 1_000;
const START_TS_MS = 1700870400 * 1000;

const services = [
  "api-gateway",
  "user-service",
  "order-service",
  "payment-service",
  "inventory-service",
];
const envs = ["prod", "staging"];
const endpoints = [
  "/api/users",
  "/api/orders",
  "/api/products",
  "/api/checkout",
  "/health",
  "/metrics",
];
const statusCodes = [200, 200, 200, 200, 201, 400, 404, 500]; // Weighted towards 200

function randomChoice<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]!;
}

function generateTraceId(): string {
  return Array.from({ length: 32 }, () =>
    Math.floor(Math.random() * 16).toString(16)
  ).join("");
}

function generateUserId(): string {
  return `user_${Math.floor(Math.random() * 1000)}`;
}

console.log(`Creating ${NUM_SHARDS} sharded databases (one per hour)...\n`);

for (let shardId = 0; shardId < NUM_SHARDS; shardId++) {
  const dbPath = `shard${shardId}.db`;

  // Remove existing database
  if (existsSync(dbPath)) {
    unlinkSync(dbPath);
  }

  const db = new Database(dbPath);

  const hourStart = START_TS_MS + shardId * HOUR_MS;
  const hourEnd = hourStart + HOUR_MS;
  const hourStartDate = new Date(hourStart);

  console.log(`\n=== Initializing Shard ${shardId} (${dbPath}) ===`);
  console.log(`  Hour: ${hourStartDate.toISOString()}`);

  // Create events table (OTel-style schema)
  db.run(`
    CREATE TABLE IF NOT EXISTS events (
      event_id      INTEGER PRIMARY KEY,
      ts_ms         INTEGER NOT NULL,
      service_name  TEXT,
      env           TEXT,
      endpoint      TEXT,
      status_code   INTEGER,
      user_id       TEXT,
      trace_id      TEXT,
      fields_json   BLOB
    )
  `);

  // Create index on ts_ns for efficient time-based queries
  db.run(`CREATE INDEX IF NOT EXISTS idx_events_ts_ns ON events(ts_ms)`);

  const insertEvent = db.prepare(`
    INSERT INTO events (event_id, ts_ms, service_name, env, endpoint, status_code, user_id, trace_id, fields_json)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  // Insert events for this hour
  for (let i = 0; i < EVENTS_PER_SHARD; i++) {
    const eventId = shardId * EVENTS_PER_SHARD + i;
    // Spread events across the hour
    const offsetMs = (i * HOUR_MS) / EVENTS_PER_SHARD;
    const tsMs = hourStart + offsetMs;

    const serviceName = randomChoice(services);
    const env = randomChoice(envs);
    const endpoint = randomChoice(endpoints);
    const statusCode = randomChoice(statusCodes);
    const userId = generateUserId();
    const traceId = generateTraceId();
    const fieldsJson = JSON.stringify({
      duration_ms: Math.floor(Math.random() * 500) + 10,
      request_size: Math.floor(Math.random() * 10000),
      response_size: Math.floor(Math.random() * 50000),
    });

    insertEvent.run(
      eventId,
      tsMs,
      serviceName,
      env,
      endpoint,
      statusCode,
      userId,
      traceId,
      fieldsJson
    );
  }

  console.log(`  Inserted ${EVENTS_PER_SHARD} events`);

  // Show sample data
  console.log(`\n  Sample events on shard ${shardId}:`);
  const sampleEvents = db
    .prepare(
      "SELECT event_id, ts_ms, service_name, endpoint, status_code FROM events LIMIT 3"
    )
    .all();
  console.table(sampleEvents);

  db.close();
}

console.log(`\n✅ All ${NUM_SHARDS} sharded databases created successfully!`);
console.log("\nShard distribution (by hour):");
console.log(
  `  Shard 0: Hour 0 (${new Date(START_TS_MS).toISOString()})`
);
console.log(
  `  Shard 1: Hour 1 (${new Date(START_TS_MS + HOUR_MS).toISOString()})`
);
console.log(
  `  Shard 2: Hour 2 (${new Date(START_TS_MS + 2 * HOUR_MS).toISOString()})`
);
