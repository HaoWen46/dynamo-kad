# dynamo-storage

Durable storage engine: WAL (write-ahead log) + in-memory memtable.

## Architecture

```
PUT request
  |
  v
 WAL append (fsync per policy)
  |
  v
 Memtable insert
  |
  v
 Response
```

All writes hit the WAL first for durability, then the memtable for fast reads.
On startup, the WAL is replayed to rebuild the memtable.

## Modules

| Module | Description |
|--------|-------------|
| `wal` | Append-only write-ahead log with configurable fsync policy |
| `memtable` | In-memory multi-version store with vector-clock-based conflict resolution |
| `record` | `StorageRecord` type (key, value, vclock, tombstone) |
| `engine` | `StorageEngine` combining WAL + memtable |

## Fsync Policies

- **Always**: fsync after every write (safest, slowest)
- **Batch**: caller controls when to fsync (balanced)
- **None**: OS decides (fastest, risk of data loss on crash)
