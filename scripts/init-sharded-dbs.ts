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
  return crypto.randomUUID().replaceAll("-", "");
}

function generateUserId(): string {
  return `user_${Math.floor(Math.random() * 1000)}`;
}

function randomUserAgent(): string {
  const browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"];
  const os = ["Windows", "Mac", "Linux"];
  const device = ["Desktop", "Mobile", "Tablet"];
  return `${randomChoice(device)} ${randomChoice(browsers)} ${randomChoice(
    os
  )}`;
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
    CREATE TABLE events (
      span_id       TEXT PRIMARY KEY,
      trace_id      TEXT NOT NULL,
      ts_ms         INTEGER NOT NULL,
      duration_ms   INTEGER NOT NULL,
      service_name  TEXT,
      fields_json   TEXT
    )
  `);
  db.run(`CREATE INDEX idx_events_ts_ms ON events(ts_ms desc)`);
  db.run(`CREATE INDEX idx_events_trace_id ON events(trace_id)`);
  db.run(`CREATE INDEX idx_events_service_name ON events(service_name)`);

  db.run(`
    CREATE TABLE fields (
      span_id       TEXT,
      key           TEXT NOT NULL,
      value         TEXT NOT NULL,
      FOREIGN KEY (span_id) REFERENCES events(span_id)
    )
  `);

  db.run(`CREATE INDEX idx_fields_key_value ON fields(key, value)`);
  db.run(`CREATE INDEX idx_fields_span_id ON fields(span_id)`);

  const insertEvent = db.prepare(`
    INSERT INTO events (span_id, trace_id, ts_ms, duration_ms, service_name, fields_json)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  const insertField = db.prepare(
    `INSERT INTO fields (span_id, key, value) VALUES (?, ?, ?)`
  );

  // Insert events for this hour
  for (let i = 0; i < EVENTS_PER_SHARD; i++) {
    const traceId = generateTraceId();
    const spanId = generateTraceId();
    // Spread events across the hour
    const offsetMs = (i * HOUR_MS) / EVENTS_PER_SHARD;
    const tsMs = hourStart + offsetMs;

    const serviceName = randomChoice(services);
    const env = randomChoice(envs);
    const userId = generateUserId();
    const fields = {
      user_agent_string: randomUserAgent(),
      endpoint: randomChoice(endpoints),
      status_code: randomChoice(statusCodes),
    };
    const fieldsJson = JSON.stringify(fields);

    insertEvent.run(
      spanId,
      traceId,
      tsMs,
      Math.floor(Math.random() * 500) + 10,
      serviceName,
      fieldsJson
    );
    for (const [key, value] of Object.entries(fields)) {
      insertField.run(spanId, key, value);
    }
  }

  console.log(`  Inserted ${EVENTS_PER_SHARD} events`);

  // Show sample data
  console.log(`\n  Sample events on shard ${shardId}:`);
  const sampleEvents = db
    .prepare(
      "SELECT trace_id, ts_ms, service_name, fields_json FROM events LIMIT 3"
    )
    .all();
  console.table(sampleEvents);

  const fieldQuery = db
    .prepare(
      `SELECT e.span_id, e.trace_id, e.ts_ms, e.service_name, e.fields_json
     FROM events e
     JOIN fields f ON e.span_id = f.span_id
     WHERE f.key = 'status_code' AND f.value = '200'
     LIMIT 3
     `
    )
    .all();
  console.table(fieldQuery);

  db.close();
}

console.log(`\n✅ All ${NUM_SHARDS} sharded databases created successfully!`);
console.log("\nShard distribution (by hour):");
console.log(`  Shard 0: Hour 0 (${new Date(START_TS_MS).toISOString()})`);
console.log(
  `  Shard 1: Hour 1 (${new Date(START_TS_MS + HOUR_MS).toISOString()})`
);
console.log(
  `  Shard 2: Hour 2 (${new Date(START_TS_MS + 2 * HOUR_MS).toISOString()})`
);
