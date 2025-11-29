# do-otel

```
                        User / Application
                                │
                                ▼
  ┌───────────────────────────────────────────────────────────┐
  │                      SQL QUERY INPUT                      │
  │  "SELECT count(*) FROM logs WHERE date >= '2023-11-15'..."│
  └─────────────────────────────┬─────────────────────────────┘
                                │
                                │ (1) Logical Plan & Pruning
                                ▼
  ╔═══════════════════════════════════════════════════════════╗
  ║                 APACHE DATAFUSION ENGINE                  ║
  ║                                                           ║
  ║  ┌─────────────────────────────────────────────────────┐  ║
  ║  │                REMOTE TABLE PROVIDER                │  ║
  ║  │ (Catalog: Time Ranges ──► Durable Object Endpoints) │  ║
  ║  └──────────────────────────┬──────────────────────────┘  ║
  ║                             │                             ║
  ║                 (2) Filter Pushdown Logic                 ║
  ║                             ▼                             ║
  ║           Which Durable Objects match the filter?         ║
  ╚═════════════════════════════╤═════════════════════════════╝
                                │
         ┌──────────────────────┼──────────────────────┐
         │                      │                      │
 ┌───────┴───────┐      ┌───────┴───────┐      ┌───────┴───────┐
 │ DO_ID: 2023-10│      │ DO_ID: 2023-11│      │ DO_ID: 2023-12│
 │  Range: Oct   │      │  Range: Nov   │      │  Range: Dec   │
 └───────┬───────┘      └───────┬───────┘      └───────┬───────┘
         │                      │                      │
      PRUNED                    │ (3) HTTP POST        │ (3) HTTP POST
   (No Request)                 │     /query           │     /query
                                │     Body: SQL        │     Body: SQL
                                ▼                      ▼
                      ┌───────────────────┐  ┌───────────────────┐
                      │  DURABLE OBJECT   │  │  DURABLE OBJECT   │
                      │ (Shard: 2023-11)  │  │ (Shard: 2023-12)  │
                      │                   │  │                   │
                      │  ┌─────────────┐  │  │  ┌─────────────┐  │
                      │  │   SQLite    │  │  │  │   SQLite    │  │
                      │  │  (Storage)  │  │  │  │  (Storage)  │  │
                      │  └──────┬──────┘  │  │  └──────┬──────┘  │
                      └─────────│─────────┘  └─────────│─────────┘
                                │                      │
                                │ (4) Response Stream  │
                                │                      │
                                ▼                      ▼
                ┌──────────────────────────────────────────────────┐
                │              DATAFUSION AGGREGATION              │
                │       (Collect Streams ──► Merge ──► Yield)      │
                └───────────────────────┬──────────────────────────┘
                                        │
                                        ▼
                                   FINAL RESULT
```
