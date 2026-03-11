# Lux

A fast, multi-threaded key-value store written in Rust. Drop-in Redis replacement that speaks the RESP protocol natively -- any Redis client library works out of the box.

## Why Lux?

Redis is single-threaded. Lux is not.

Lux uses a **128-shard concurrent architecture** with per-shard locking, meaning multiple cores can serve requests simultaneously. On pipelined workloads (how real applications talk to key-value stores), Lux significantly outperforms Redis.

### Benchmarks (Raspberry Pi 4B, 4 cores)

| Benchmark | Lux | Redis 7.0 | Delta |
|-----------|-----|-----------|-------|
| SET | 90,416 rps | 92,507 rps | -2% |
| GET | 90,090 rps | 92,507 rps | -3% |
| SET (pipelined) | **1,250,000 rps** | 826,446 rps | **+51%** |
| GET (pipelined) | **1,265,823 rps** | 990,099 rps | **+28%** |

*50 concurrent clients, 100K requests, pipeline depth 16*

## Features

- **60+ Redis commands** -- strings, lists, hashes, sets, pub/sub
- **RESP protocol** -- works with redis-cli, ioredis, redis-py, any Redis client
- **Multi-threaded** -- 128 shards with parking_lot RwLocks, tokio async runtime
- **Pipelining** -- batch multiple commands per round trip for maximum throughput
- **Persistence** -- automatic snapshots every 60 seconds, manual SAVE command
- **Pub/Sub** -- SUBSCRIBE, UNSUBSCRIBE, PUBLISH with broadcast channels
- **TTL support** -- EX, PX, EXPIRE, PEXPIRE, PERSIST, TTL, PTTL
- **Zero-copy where possible** -- bytes::Bytes for stored values, pre-serialized common responses

## Quick Start

```bash
cargo build --release
./target/release/lux
```

Lux starts on `0.0.0.0:6379` by default. Connect with any Redis client:

```bash
redis-cli -p 6379
> SET hello world
OK
> GET hello
"world"
```

## Supported Commands

### Strings
`SET` `GET` `SETNX` `SETEX` `PSETEX` `GETSET` `MGET` `MSET` `STRLEN` `APPEND` `INCR` `DECR` `INCRBY` `DECRBY`

### Keys
`DEL` `EXISTS` `KEYS` `SCAN` `TYPE` `RENAME` `TTL` `PTTL` `EXPIRE` `PEXPIRE` `PERSIST` `DBSIZE` `FLUSHDB` `FLUSHALL`

### Lists
`LPUSH` `RPUSH` `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX`

### Hashes
`HSET` `HMSET` `HGET` `HMGET` `HDEL` `HGETALL` `HKEYS` `HVALS` `HLEN` `HEXISTS` `HINCRBY`

### Sets
`SADD` `SREM` `SMEMBERS` `SISMEMBER` `SCARD` `SUNION` `SINTER` `SDIFF`

### Pub/Sub
`PUBLISH` `SUBSCRIBE` `UNSUBSCRIBE`

### Server
`PING` `ECHO` `INFO` `SAVE` `CONFIG` `CLIENT` `SELECT` `COMMAND`

## Architecture

```
Client connections (tokio tasks)
        |
   RESP Parser (zero-copy, pipelining)
        |
   Command Dispatch
        |
   128 Sharded Store (parking_lot RwLock per shard)
        |
   FNV Hash -> Shard Selection
```

Each shard is cache-line aligned (`#[repr(align(128))]`) to prevent false sharing between cores. Values are stored as `bytes::Bytes` (reference-counted, clone is a pointer bump). Common responses (`+OK\r\n`, `$-1\r\n`, etc.) are pre-serialized static byte slices.

## Build Optimizations

The release profile enables:
- **LTO** (link-time optimization) -- cross-crate inlining
- **Single codegen unit** -- maximum optimization opportunity
- **Strip** -- smaller binary

## License

MIT
