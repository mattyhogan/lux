use bytes::{Bytes, BytesMut};
use std::time::Duration;

use crate::pubsub::Broker;
use crate::resp;
use crate::snapshot;
use crate::store::Store;

pub enum CmdResult {
    Written,
    Subscribe { channels: Vec<String> },
    Publish { channel: String, message: String },
}

pub fn execute(
    store: &Store,
    _broker: &Broker,
    args: &[String],
    out: &mut BytesMut,
) -> CmdResult {
    if args.is_empty() {
        resp::write_error(out, "ERR no command");
        return CmdResult::Written;
    }

    let cmd = args[0].to_uppercase();
    match cmd.as_str() {
        "PING" => {
            if args.len() > 1 {
                resp::write_bulk(out, &args[1]);
            } else {
                resp::write_pong(out);
            }
        }
        "ECHO" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'echo' command");
            } else {
                resp::write_bulk(out, &args[1]);
            }
        }

        "SET" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'set' command");
                return CmdResult::Written;
            }
            let mut ttl = None;
            let mut nx = false;
            let mut xx = false;
            let mut i = 3;
            while i < args.len() {
                match args[i].to_uppercase().as_str() {
                    "EX" => {
                        if i + 1 >= args.len() {
                            resp::write_error(out, "ERR syntax error");
                            return CmdResult::Written;
                        }
                        match args[i + 1].parse::<u64>() {
                            Ok(s) => ttl = Some(Duration::from_secs(s)),
                            Err(_) => {
                                resp::write_error(out, "ERR value is not an integer or out of range");
                                return CmdResult::Written;
                            }
                        }
                        i += 2;
                    }
                    "PX" => {
                        if i + 1 >= args.len() {
                            resp::write_error(out, "ERR syntax error");
                            return CmdResult::Written;
                        }
                        match args[i + 1].parse::<u64>() {
                            Ok(ms) => ttl = Some(Duration::from_millis(ms)),
                            Err(_) => {
                                resp::write_error(out, "ERR value is not an integer or out of range");
                                return CmdResult::Written;
                            }
                        }
                        i += 2;
                    }
                    "NX" => { nx = true; i += 1; }
                    "XX" => { xx = true; i += 1; }
                    _ => {
                        resp::write_error(out, "ERR syntax error");
                        return CmdResult::Written;
                    }
                }
            }
            if nx {
                if store.set_nx(args[1].clone(), Bytes::from(args[2].clone())) {
                    resp::write_ok(out);
                } else {
                    resp::write_null(out);
                }
            } else if xx {
                if store.get(&args[1]).is_some() {
                    store.set(args[1].clone(), Bytes::from(args[2].clone()), ttl);
                    resp::write_ok(out);
                } else {
                    resp::write_null(out);
                }
            } else {
                store.set(args[1].clone(), Bytes::from(args[2].clone()), ttl);
                resp::write_ok(out);
            }
        }
        "SETNX" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'setnx' command");
                return CmdResult::Written;
            }
            resp::write_integer(out, if store.set_nx(args[1].clone(), Bytes::from(args[2].clone())) { 1 } else { 0 });
        }
        "SETEX" => {
            if args.len() < 4 {
                resp::write_error(out, "ERR wrong number of arguments for 'setex' command");
                return CmdResult::Written;
            }
            match args[2].parse::<u64>() {
                Ok(secs) => {
                    store.set(args[1].clone(), Bytes::from(args[3].clone()), Some(Duration::from_secs(secs)));
                    resp::write_ok(out);
                }
                Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
            }
        }
        "PSETEX" => {
            if args.len() < 4 {
                resp::write_error(out, "ERR wrong number of arguments for 'psetex' command");
                return CmdResult::Written;
            }
            match args[2].parse::<u64>() {
                Ok(ms) => {
                    store.set(args[1].clone(), Bytes::from(args[3].clone()), Some(Duration::from_millis(ms)));
                    resp::write_ok(out);
                }
                Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
            }
        }
        "GET" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'get' command");
                return CmdResult::Written;
            }
            resp::write_optional_bulk_raw(out, &store.get(&args[1]));
        }
        "GETSET" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'getset' command");
                return CmdResult::Written;
            }
            resp::write_optional_bulk_raw(out, &store.get_set(&args[1], Bytes::from(args[2].clone())));
        }
        "MGET" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'mget' command");
                return CmdResult::Written;
            }
            resp::write_array_header(out, args.len() - 1);
            for key in &args[1..] {
                resp::write_optional_bulk_raw(out, &store.get(key));
            }
        }
        "MSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                resp::write_error(out, "ERR wrong number of arguments for 'mset' command");
                return CmdResult::Written;
            }
            let mut i = 1;
            while i < args.len() {
                store.set(args[i].clone(), Bytes::from(args[i + 1].clone()), None);
                i += 2;
            }
            resp::write_ok(out);
        }
        "STRLEN" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'strlen' command");
                return CmdResult::Written;
            }
            resp::write_integer(out, store.strlen(&args[1]));
        }
        "DEL" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'del' command");
                return CmdResult::Written;
            }
            resp::write_integer(out, store.del(&args[1..]));
        }
        "EXISTS" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'exists' command");
                return CmdResult::Written;
            }
            resp::write_integer(out, store.exists(&args[1..]));
        }
        "INCR" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'incr' command");
                return CmdResult::Written;
            }
            match store.incr(&args[1], 1) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "DECR" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'decr' command");
                return CmdResult::Written;
            }
            match store.incr(&args[1], -1) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "INCRBY" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'incrby' command");
                return CmdResult::Written;
            }
            match args[2].parse::<i64>() {
                Ok(delta) => match store.incr(&args[1], delta) {
                    Ok(n) => resp::write_integer(out, n),
                    Err(e) => resp::write_error(out, &e),
                },
                Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
            }
        }
        "DECRBY" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'decrby' command");
                return CmdResult::Written;
            }
            match args[2].parse::<i64>() {
                Ok(delta) => match store.incr(&args[1], -delta) {
                    Ok(n) => resp::write_integer(out, n),
                    Err(e) => resp::write_error(out, &e),
                },
                Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
            }
        }
        "APPEND" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'append' command");
                return CmdResult::Written;
            }
            resp::write_integer(out, store.append(&args[1], args[2].as_bytes()));
        }
        "KEYS" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'keys' command");
                return CmdResult::Written;
            }
            let keys = store.keys(&args[1]);
            resp::write_bulk_array(out, &keys);
        }
        "SCAN" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'scan' command");
                return CmdResult::Written;
            }
            let cursor = args[1].parse::<usize>().unwrap_or(0);
            let mut pattern = "*";
            let mut count = 10usize;
            let mut i = 2;
            while i < args.len() {
                match args[i].to_uppercase().as_str() {
                    "MATCH" => {
                        if i + 1 < args.len() {
                            pattern = &args[i + 1];
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    "COUNT" => {
                        if i + 1 < args.len() {
                            count = args[i + 1].parse().unwrap_or(10);
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    _ => { i += 1; }
                }
            }
            let (next_cursor, keys) = store.scan(cursor, pattern, count);
            resp::write_array_header(out, 2);
            resp::write_bulk(out, &next_cursor.to_string());
            resp::write_bulk_array(out, &keys);
        }
        "TTL" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'ttl' command");
                return CmdResult::Written;
            }
            resp::write_integer(out, store.ttl(&args[1]));
        }
        "PTTL" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'pttl' command");
                return CmdResult::Written;
            }
            resp::write_integer(out, store.pttl(&args[1]));
        }
        "EXPIRE" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'expire' command");
                return CmdResult::Written;
            }
            match args[2].parse::<u64>() {
                Ok(secs) => resp::write_integer(out, if store.expire(&args[1], secs) { 1 } else { 0 }),
                Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
            }
        }
        "PEXPIRE" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'pexpire' command");
                return CmdResult::Written;
            }
            match args[2].parse::<u64>() {
                Ok(ms) => resp::write_integer(out, if store.pexpire(&args[1], ms) { 1 } else { 0 }),
                Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
            }
        }
        "PERSIST" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'persist' command");
                return CmdResult::Written;
            }
            resp::write_integer(out, if store.persist(&args[1]) { 1 } else { 0 });
        }
        "TYPE" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'type' command");
                return CmdResult::Written;
            }
            match store.get_entry_type(&args[1]) {
                Some(t) => resp::write_simple(out, t),
                None => resp::write_simple(out, "none"),
            }
        }
        "RENAME" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'rename' command");
                return CmdResult::Written;
            }
            match store.rename(&args[1], &args[2]) {
                Ok(()) => resp::write_ok(out),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "DBSIZE" => resp::write_integer(out, store.dbsize()),
        "FLUSHDB" | "FLUSHALL" => {
            store.flushdb();
            resp::write_ok(out);
        }

        "LPUSH" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'lpush' command");
                return CmdResult::Written;
            }
            match store.lpush(&args[1], &args[2..]) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "RPUSH" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'rpush' command");
                return CmdResult::Written;
            }
            match store.rpush(&args[1], &args[2..]) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "LPOP" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'lpop' command");
                return CmdResult::Written;
            }
            resp::write_optional_bulk_raw(out, &store.lpop(&args[1]));
        }
        "RPOP" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'rpop' command");
                return CmdResult::Written;
            }
            resp::write_optional_bulk_raw(out, &store.rpop(&args[1]));
        }
        "LLEN" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'llen' command");
                return CmdResult::Written;
            }
            match store.llen(&args[1]) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "LRANGE" => {
            if args.len() < 4 {
                resp::write_error(out, "ERR wrong number of arguments for 'lrange' command");
                return CmdResult::Written;
            }
            let start = args[2].parse::<i64>().unwrap_or(0);
            let stop = args[3].parse::<i64>().unwrap_or(-1);
            match store.lrange(&args[1], start, stop) {
                Ok(items) => resp::write_bulk_array_raw(out, &items),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "LINDEX" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'lindex' command");
                return CmdResult::Written;
            }
            let index = args[2].parse::<i64>().unwrap_or(0);
            resp::write_optional_bulk_raw(out, &store.lindex(&args[1], index));
        }

        "HSET" | "HMSET" => {
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                resp::write_error(out, &format!("ERR wrong number of arguments for '{}' command", args[0].to_lowercase()));
                return CmdResult::Written;
            }
            let pairs: Vec<(String, String)> = args[2..]
                .chunks(2)
                .map(|c| (c[0].clone(), c[1].clone()))
                .collect();
            match store.hset(&args[1], &pairs) {
                Ok(n) => {
                    if cmd == "HMSET" {
                        resp::write_ok(out);
                    } else {
                        resp::write_integer(out, n);
                    }
                }
                Err(e) => resp::write_error(out, &e),
            }
        }
        "HGET" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'hget' command");
                return CmdResult::Written;
            }
            resp::write_optional_bulk_raw(out, &store.hget(&args[1], &args[2]));
        }
        "HMGET" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'hmget' command");
                return CmdResult::Written;
            }
            let results = store.hmget(&args[1], &args[2..]);
            resp::write_array_header(out, results.len());
            for val in &results {
                resp::write_optional_bulk_raw(out, val);
            }
        }
        "HDEL" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'hdel' command");
                return CmdResult::Written;
            }
            match store.hdel(&args[1], &args[2..]) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "HGETALL" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'hgetall' command");
                return CmdResult::Written;
            }
            match store.hgetall(&args[1]) {
                Ok(pairs) => {
                    resp::write_array_header(out, pairs.len() * 2);
                    for (k, v) in &pairs {
                        resp::write_bulk(out, k);
                        resp::write_bulk_raw(out, v);
                    }
                }
                Err(e) => resp::write_error(out, &e),
            }
        }
        "HKEYS" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'hkeys' command");
                return CmdResult::Written;
            }
            match store.hkeys(&args[1]) {
                Ok(keys) => resp::write_bulk_array(out, &keys),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "HVALS" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'hvals' command");
                return CmdResult::Written;
            }
            match store.hvals(&args[1]) {
                Ok(vals) => resp::write_bulk_array_raw(out, &vals),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "HLEN" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'hlen' command");
                return CmdResult::Written;
            }
            match store.hlen(&args[1]) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "HEXISTS" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'hexists' command");
                return CmdResult::Written;
            }
            match store.hexists(&args[1], &args[2]) {
                Ok(b) => resp::write_integer(out, if b { 1 } else { 0 }),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "HINCRBY" => {
            if args.len() < 4 {
                resp::write_error(out, "ERR wrong number of arguments for 'hincrby' command");
                return CmdResult::Written;
            }
            match args[3].parse::<i64>() {
                Ok(delta) => match store.hincrby(&args[1], &args[2], delta) {
                    Ok(n) => resp::write_integer(out, n),
                    Err(e) => resp::write_error(out, &e),
                },
                Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
            }
        }

        "SADD" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'sadd' command");
                return CmdResult::Written;
            }
            match store.sadd(&args[1], &args[2..]) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "SREM" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'srem' command");
                return CmdResult::Written;
            }
            match store.srem(&args[1], &args[2..]) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "SMEMBERS" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'smembers' command");
                return CmdResult::Written;
            }
            match store.smembers(&args[1]) {
                Ok(members) => resp::write_bulk_array(out, &members),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "SISMEMBER" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'sismember' command");
                return CmdResult::Written;
            }
            match store.sismember(&args[1], &args[2]) {
                Ok(b) => resp::write_integer(out, if b { 1 } else { 0 }),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "SCARD" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'scard' command");
                return CmdResult::Written;
            }
            match store.scard(&args[1]) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "SUNION" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'sunion' command");
                return CmdResult::Written;
            }
            match store.sunion(&args[1..]) {
                Ok(members) => resp::write_bulk_array(out, &members),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "SINTER" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'sinter' command");
                return CmdResult::Written;
            }
            match store.sinter(&args[1..]) {
                Ok(members) => resp::write_bulk_array(out, &members),
                Err(e) => resp::write_error(out, &e),
            }
        }
        "SDIFF" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'sdiff' command");
                return CmdResult::Written;
            }
            match store.sdiff(&args[1..]) {
                Ok(members) => resp::write_bulk_array(out, &members),
                Err(e) => resp::write_error(out, &e),
            }
        }

        "SAVE" => match snapshot::save(store) {
            Ok(n) => resp::write_simple(out, &format!("OK ({n} keys saved)")),
            Err(e) => resp::write_error(out, &format!("ERR snapshot failed: {e}")),
        },
        "INFO" => {
            let section = if args.len() > 1 {
                args[1].to_lowercase()
            } else {
                "all".to_string()
            };
            let info = build_info(store, &section);
            resp::write_bulk(out, &info);
        }
        "CONFIG" => {
            if args.len() > 1 && args[1].to_uppercase() == "GET" {
                resp::write_array_header(out, 0);
            } else {
                resp::write_ok(out);
            }
        }
        "CLIENT" => resp::write_ok(out),
        "SELECT" => resp::write_ok(out),
        "COMMAND" => {
            if args.len() > 1 && args[1].to_uppercase() == "DOCS" {
                resp::write_array_header(out, 0);
            } else {
                resp::write_ok(out);
            }
        }

        "PUBLISH" => {
            if args.len() < 3 {
                resp::write_error(out, "ERR wrong number of arguments for 'publish' command");
                return CmdResult::Written;
            }
            return CmdResult::Publish {
                channel: args[1].clone(),
                message: args[2].clone(),
            };
        }
        "SUBSCRIBE" => {
            if args.len() < 2 {
                resp::write_error(out, "ERR wrong number of arguments for 'subscribe' command");
                return CmdResult::Written;
            }
            return CmdResult::Subscribe {
                channels: args[1..].to_vec(),
            };
        }

        _ => resp::write_error(out, &format!("ERR unknown command '{}'", args[0])),
    }
    CmdResult::Written
}

fn build_info(store: &Store, _section: &str) -> String {
    format!(
        "# Server\r\nlux_version:{}\r\nshards:128\r\n\r\n# Keyspace\r\nkeys:{}\r\n",
        env!("CARGO_PKG_VERSION"),
        store.dbsize()
    )
}
