## Overview

This project implements an in-memory TTL cache in Go using 16 shards, per-shard read/write mutexes, and background expiration cleanup. It supports concurrent access, basic statistics, and graceful shutdown.

The implementation uses only the Go standard library and does not provide persistence or distributed caching.

## Architecture

- **Sharding:** Keys are assigned to one of 16 shards using the FNV-1a 32-bit hash. Each shard owns a map protected by its own `sync.RWMutex`.
- **Concurrency:** `Get` uses a read lock; `Set` and `Delete` use write locks. Operations on different shards can proceed independently.
- **Expiration:** Each entry stores its creation time, TTL, and data. An entry is expired when its age is greater than or equal to its TTL.
- **TTL validation:** TTLs must be positive and no greater than one year. Updating an existing key replaces its data and resets its expiration.
- **Cleanup:** Background workers periodically inspect shard entries and delete expired entries. The cleanup interval is fixed at one second.
- **Statistics:** Atomic counters track the current entry count, successful reads, and unsuccessful reads. Statistics are safe to read concurrently, but the returned values are not a globally consistent snapshot.

## API

| Method | Behavior |
|---|---|
| `Set(k, v)` | Stores or replaces an entry; rejects invalid TTLs. |
| `Get(k)` | Returns a non-expired entry, or an error if missing, expired, or closed. |
| `Delete(k)` | Removes an entry or returns an error if it does not exist. |
| `Stats()` | Returns the current counters as a formatted string. |
| `Close()` | Cancels background cleanup and waits for tracked work to finish. |

The implementation distinguishes missing keys (`key not found`) from expired keys (`key has expired`). Operations invoked after closure return `cache is closed`.

## Cleanup and Concurrency Considerations

Cleanup copies each shard's entries before checking expiration, avoiding holding a shard lock while calling `Get` and `Delete`. Expiration is checked during reads, so expired entries are never intentionally returned even if cleanup has not removed them.

However, cleanup currently scans a snapshot of the entire shard on each pass. Its work is therefore proportional to the number of entries, rather than bounded per pass. An entry updated during cleanup may also be deleted based on a stale expiration check. A production-ready implementation should validate the entry's current creation time or version before deleting it and use an incremental cleanup cursor or bounded batch.

The lifecycle uses a cancellation context, a wait group, and an idempotent `Close`. Concurrent shutdown and operations require additional synchronization around wait-group registration to guarantee that no operation can call `Add` after shutdown begins waiting. The current implementation does not fully establish that guarantee.

## Details

- Make the shard count and cleanup interval configurable.
- Use Go generics to support a typed public API.
- Bound cleanup work per shard and ensure eventual revisiting of entries.
- Prevent stale cleanup checks from deleting recently updated entries.
- Synchronize shutdown with operation registration and document the exact `Close` contract.
- Add tests for concurrent access, expiration, cleanup races, statistics, and shutdown.
- Run `go test -race -count=100 ./...` and add concurrent access benchmarks.

## Design Trade-off

Per-shard mutexes provide a straightforward ownership model without a global lock on ordinary entry operations. A single-owner goroutine per shard is an alternative, but it adds message-passing complexity and is unnecessary for this implementation's scope.
