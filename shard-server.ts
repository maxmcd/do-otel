import { Database } from 'bun:sqlite'

const port = parseInt(process.env.PORT || '8088')
const NUM_SHARDS = 3

console.log(`Starting unified shard server on port ${port}...`)

// Open all shard databases
const databases: Database[] = []
for (let i = 0; i < NUM_SHARDS; i++) {
  const dbPath = `shard${i}.db`
  databases.push(new Database(dbPath))
  console.log(`  Loaded shard ${i}: ${dbPath}`)
}

// Request/Response types
interface QueryRequest {
  sql: string
  params?: any[]
}

interface QueryResponse {
  columns: string[]
  rows: any[][]
}

interface ErrorResponse {
  error: string
}

const server = Bun.serve({
  port,
  async fetch(req) {
    const url = new URL(req.url)

    // Parse path: /shard/{shardId}
    const match = url.pathname.match(/^\/shard\/(\d+)$/)
    if (!match) {
      return new Response(
        JSON.stringify({ error: 'Invalid path. Use /shard/{shardId}' }),
        {
          status: 404,
          headers: { 'Content-Type': 'application/json' },
        }
      )
    }

    const shardId = parseInt(match[1])
    if (shardId < 0 || shardId >= NUM_SHARDS) {
      return new Response(
        JSON.stringify({
          error: `Invalid shard ID. Must be 0-${NUM_SHARDS - 1}`,
        }),
        {
          status: 400,
          headers: { 'Content-Type': 'application/json' },
        }
      )
    }

    // Only accept POST requests
    if (req.method !== 'POST') {
      return new Response(JSON.stringify({ error: 'Method not allowed' }), {
        status: 405,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    const db = databases[shardId]

    try {
      // Parse request body
      const bodyJson = await req.json()
      if (!bodyJson || typeof bodyJson !== 'object') {
        return new Response(JSON.stringify({ error: 'Invalid JSON body' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' },
        })
      }

      const body = bodyJson as QueryRequest

      if (!body.sql) {
        return new Response(JSON.stringify({ error: "Missing 'sql' field" }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' },
        })
      }

      console.log(`[Shard ${shardId}] Executing SQL: ${body.sql}`)

      // Execute query
      const stmt = db.prepare(body.sql)
      const params = Array.isArray(body.params) ? body.params : []
      const rows = (
        params.length > 0 ? stmt.all(...params) : stmt.all()
      ) as Array<Record<string, unknown>>

      // Get column names from first row or from statement
      let columns: string[] = []
      if (rows.length > 0) {
        columns = Object.keys(rows[0] as Record<string, unknown>)
      } else {
        columns = stmt.columnNames || []
      }

      // Convert rows from objects to arrays
      const rowArrays = rows.map((row) =>
        columns.map((col) => (row as any)[col])
      )

      const response: QueryResponse = {
        columns,
        rows: rowArrays,
      }

      console.log(
        `[Shard ${shardId}] Returning ${rows.length} rows with columns: ${columns.join(', ')}`
      )
      console.log(rows)

      return new Response(JSON.stringify(response), {
        headers: { 'Content-Type': 'application/json' },
      })
    } catch (error) {
      console.error(`[Shard ${shardId}] Query error:`, error)

      const errorResponse: ErrorResponse = {
        error: error instanceof Error ? error.message : 'Unknown error',
      }

      return new Response(JSON.stringify(errorResponse), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  },
})

console.log(`\nShard server running on http://localhost:${server.port}`)
console.log(`Routes:`)
for (let i = 0; i < NUM_SHARDS; i++) {
  console.log(`  POST /shard/${i} -> shard${i}.db`)
}
console.log(
  `\nSend POST requests with JSON body: { "sql": "SELECT ...", "params": [...] }`
)
