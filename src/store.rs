use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

const NUM_SHARDS: usize = 128;

pub enum StoreValue {
    Str(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<String, Bytes>),
    Set(HashSet<String>),
}

impl StoreValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            StoreValue::Str(_) => "string",
            StoreValue::List(_) => "list",
            StoreValue::Hash(_) => "hash",
            StoreValue::Set(_) => "set",
        }
    }
}

pub struct Entry {
    pub value: StoreValue,
    pub expires_at: Option<Instant>,
}

impl Entry {
    #[inline(always)]
    fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |exp| Instant::now() > exp)
    }
}

#[repr(align(128))]
struct Shard {
    data: HashMap<String, Entry>,
}

pub struct Store {
    shards: Box<[RwLock<Shard>]>,
}

#[inline(always)]
fn fx_hash(bytes: &[u8]) -> usize {
    let mut hash: usize = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= b as usize;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

impl Store {
    pub fn new() -> Self {
        let shards: Vec<RwLock<Shard>> = (0..NUM_SHARDS)
            .map(|_| {
                RwLock::new(Shard {
                    data: HashMap::new(),
                })
            })
            .collect();
        Self {
            shards: shards.into_boxed_slice(),
        }
    }

    #[inline(always)]
    fn shard_index(&self, key: &str) -> usize {
        fx_hash(key.as_bytes()) % NUM_SHARDS
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Str(s) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn get_entry_type(&self, key: &str) -> Option<&'static str> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => Some(entry.value.type_name()),
            _ => None,
        }
    }

    pub fn set(&self, key: String, value: Bytes, ttl: Option<Duration>) {
        let idx = self.shard_index(&key);
        let mut shard = self.shards[idx].write();
        let expires_at = ttl.map(|d| Instant::now() + d);
        shard.data.insert(
            key,
            Entry {
                value: StoreValue::Str(value),
                expires_at,
            },
        );
    }

    pub fn set_nx(&self, key: String, value: Bytes) -> bool {
        let idx = self.shard_index(&key);
        let mut shard = self.shards[idx].write();
        if let Some(entry) = shard.data.get(&key) {
            if !entry.is_expired() {
                return false;
            }
        }
        shard.data.insert(
            key,
            Entry {
                value: StoreValue::Str(value),
                expires_at: None,
            },
        );
        true
    }

    pub fn get_set(&self, key: &str, value: Bytes) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let old = shard.data.get(key).and_then(|e| {
            if e.is_expired() {
                None
            } else {
                match &e.value {
                    StoreValue::Str(s) => Some(s.clone()),
                    _ => None,
                }
            }
        });
        shard.data.insert(
            key.to_string(),
            Entry {
                value: StoreValue::Str(value),
                expires_at: None,
            },
        );
        old
    }

    pub fn strlen(&self, key: &str) -> i64 {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Str(s) => s.len() as i64,
                _ => 0,
            },
            _ => 0,
        }
    }

    pub fn del(&self, keys: &[String]) -> i64 {
        let mut count = 0i64;
        for key in keys {
            let idx = self.shard_index(key);
            let mut shard = self.shards[idx].write();
            if shard.data.remove(key).is_some() {
                count += 1;
            }
        }
        count
    }

    pub fn exists(&self, keys: &[String]) -> i64 {
        let mut count = 0i64;
        for key in keys {
            let idx = self.shard_index(key);
            let shard = self.shards[idx].read();
            if let Some(entry) = shard.data.get(key.as_str()) {
                if !entry.is_expired() {
                    count += 1;
                }
            }
        }
        count
    }

    pub fn incr(&self, key: &str, delta: i64) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let (current, expires_at) = match shard.data.get(key) {
            Some(e) if !e.is_expired() => match &e.value {
                StoreValue::Str(s) => {
                    let s = std::str::from_utf8(s).map_err(|_| {
                        "ERR value is not an integer or out of range".to_string()
                    })?;
                    let n = s.parse::<i64>().map_err(|_| {
                        "ERR value is not an integer or out of range".to_string()
                    })?;
                    (n, e.expires_at)
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            },
            _ => (0, None),
        };
        let new_val = current + delta;
        shard.data.insert(
            key.to_string(),
            Entry {
                value: StoreValue::Str(Bytes::from(new_val.to_string())),
                expires_at,
            },
        );
        Ok(new_val)
    }

    pub fn append(&self, key: &str, value: &[u8]) -> i64 {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        if let Some(entry) = shard.data.get_mut(key) {
            if !entry.is_expired() {
                if let StoreValue::Str(s) = &entry.value {
                    let mut new_val = Vec::with_capacity(s.len() + value.len());
                    new_val.extend_from_slice(s);
                    new_val.extend_from_slice(value);
                    let len = new_val.len() as i64;
                    entry.value = StoreValue::Str(Bytes::from(new_val));
                    return len;
                }
            }
        }
        let val = Bytes::copy_from_slice(value);
        let len = val.len() as i64;
        shard.data.insert(
            key.to_string(),
            Entry {
                value: StoreValue::Str(val),
                expires_at: None,
            },
        );
        len
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let now = Instant::now();
        let matcher = GlobMatcher::new(pattern);
        let mut result = Vec::new();
        for shard in self.shards.iter() {
            let shard = shard.read();
            for (k, e) in shard.data.iter() {
                if e.expires_at.map_or(true, |exp| now < exp) && matcher.matches(k) {
                    result.push(k.clone());
                }
            }
        }
        result
    }

    pub fn scan(&self, cursor: usize, pattern: &str, count: usize) -> (usize, Vec<String>) {
        let all_keys = self.keys(pattern);
        let start = cursor.min(all_keys.len());
        let end = (start + count).min(all_keys.len());
        let next_cursor = if end >= all_keys.len() { 0 } else { end };
        (next_cursor, all_keys[start..end].to_vec())
    }

    pub fn ttl(&self, key: &str) -> i64 {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            None => -2,
            Some(entry) => match entry.expires_at {
                None => -1,
                Some(exp) => {
                    let now = Instant::now();
                    if now > exp { -2 } else { exp.duration_since(now).as_secs() as i64 }
                }
            },
        }
    }

    pub fn pttl(&self, key: &str) -> i64 {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            None => -2,
            Some(entry) => match entry.expires_at {
                None => -1,
                Some(exp) => {
                    let now = Instant::now();
                    if now > exp { -2 } else { exp.duration_since(now).as_millis() as i64 }
                }
            },
        }
    }

    pub fn expire(&self, key: &str, seconds: u64) -> bool {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        if let Some(entry) = shard.data.get_mut(key) {
            if !entry.is_expired() {
                entry.expires_at = Some(Instant::now() + Duration::from_secs(seconds));
                return true;
            }
        }
        false
    }

    pub fn pexpire(&self, key: &str, millis: u64) -> bool {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        if let Some(entry) = shard.data.get_mut(key) {
            if !entry.is_expired() {
                entry.expires_at = Some(Instant::now() + Duration::from_millis(millis));
                return true;
            }
        }
        false
    }

    pub fn persist(&self, key: &str) -> bool {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        if let Some(entry) = shard.data.get_mut(key) {
            if !entry.is_expired() && entry.expires_at.is_some() {
                entry.expires_at = None;
                return true;
            }
        }
        false
    }

    pub fn rename(&self, key: &str, new_key: &str) -> Result<(), String> {
        let old_idx = self.shard_index(key);
        let entry = {
            let mut shard = self.shards[old_idx].write();
            match shard.data.remove(key) {
                Some(e) if !e.is_expired() => e,
                _ => return Err("ERR no such key".to_string()),
            }
        };
        let new_idx = self.shard_index(new_key);
        let mut shard = self.shards[new_idx].write();
        shard.data.insert(new_key.to_string(), entry);
        Ok(())
    }

    pub fn dbsize(&self) -> i64 {
        let now = Instant::now();
        let mut total = 0i64;
        for shard in self.shards.iter() {
            let shard = shard.read();
            total += shard.data.values().filter(|e| e.expires_at.map_or(true, |exp| now < exp)).count() as i64;
        }
        total
    }

    pub fn flushdb(&self) {
        for shard in self.shards.iter() {
            let mut shard = shard.write();
            shard.data.clear();
        }
    }

    pub fn lpush(&self, key: &str, values: &[String]) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let entry = shard.data.entry(key.to_string()).or_insert_with(|| Entry {
            value: StoreValue::List(VecDeque::new()),
            expires_at: None,
        });
        match &mut entry.value {
            StoreValue::List(list) => {
                for v in values { list.push_front(Bytes::from(v.clone())); }
                Ok(list.len() as i64)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn rpush(&self, key: &str, values: &[String]) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let entry = shard.data.entry(key.to_string()).or_insert_with(|| Entry {
            value: StoreValue::List(VecDeque::new()),
            expires_at: None,
        });
        match &mut entry.value {
            StoreValue::List(list) => {
                for v in values { list.push_back(Bytes::from(v.clone())); }
                Ok(list.len() as i64)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn lpop(&self, key: &str) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key) {
            Some(entry) if !entry.is_expired() => match &mut entry.value {
                StoreValue::List(list) => list.pop_front(),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn rpop(&self, key: &str) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key) {
            Some(entry) if !entry.is_expired() => match &mut entry.value {
                StoreValue::List(list) => list.pop_back(),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn llen(&self, key: &str) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::List(list) => Ok(list.len() as i64),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(0),
        }
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::List(list) => {
                    let len = list.len() as i64;
                    let s = if start < 0 { (len + start).max(0) as usize } else { start.min(len) as usize };
                    let e = if stop < 0 { (len + stop + 1).max(0) as usize } else { (stop + 1).min(len) as usize };
                    if s >= e { Ok(vec![]) } else { Ok(list.iter().skip(s).take(e - s).cloned().collect()) }
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(vec![]),
        }
    }

    pub fn lindex(&self, key: &str, index: i64) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::List(list) => {
                    let i = if index < 0 { (list.len() as i64 + index) as usize } else { index as usize };
                    list.get(i).cloned()
                }
                _ => None,
            },
            _ => None,
        }
    }

    pub fn hset(&self, key: &str, pairs: &[(String, String)]) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let entry = shard.data.entry(key.to_string()).or_insert_with(|| Entry {
            value: StoreValue::Hash(HashMap::new()),
            expires_at: None,
        });
        match &mut entry.value {
            StoreValue::Hash(map) => {
                let mut added = 0i64;
                for (field, value) in pairs {
                    if map.insert(field.clone(), Bytes::from(value.clone())).is_none() { added += 1; }
                }
                Ok(added)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Hash(map) => map.get(field).cloned(),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn hmget(&self, key: &str, fields: &[String]) -> Vec<Option<Bytes>> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Hash(map) => fields.iter().map(|f| map.get(f).cloned()).collect(),
                _ => fields.iter().map(|_| None).collect(),
            },
            _ => fields.iter().map(|_| None).collect(),
        }
    }

    pub fn hdel(&self, key: &str, fields: &[String]) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key) {
            Some(entry) if !entry.is_expired() => match &mut entry.value {
                StoreValue::Hash(map) => {
                    let mut removed = 0i64;
                    for f in fields { if map.remove(f).is_some() { removed += 1; } }
                    Ok(removed)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(0),
        }
    }

    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, Bytes)>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Hash(map) => Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(vec![]),
        }
    }

    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Hash(map) => Ok(map.keys().cloned().collect()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(vec![]),
        }
    }

    pub fn hvals(&self, key: &str) -> Result<Vec<Bytes>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Hash(map) => Ok(map.values().cloned().collect()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(vec![]),
        }
    }

    pub fn hlen(&self, key: &str) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Hash(map) => Ok(map.len() as i64),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(0),
        }
    }

    pub fn hexists(&self, key: &str, field: &str) -> Result<bool, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Hash(map) => Ok(map.contains_key(field)),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(false),
        }
    }

    pub fn hincrby(&self, key: &str, field: &str, delta: i64) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let entry = shard.data.entry(key.to_string()).or_insert_with(|| Entry {
            value: StoreValue::Hash(HashMap::new()),
            expires_at: None,
        });
        match &mut entry.value {
            StoreValue::Hash(map) => {
                let current: i64 = map.get(field)
                    .map(|v| std::str::from_utf8(v).ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .ok_or_else(|| "ERR hash value is not an integer".to_string()))
                    .transpose()?
                    .unwrap_or(0);
                let new_val = current + delta;
                map.insert(field.to_string(), Bytes::from(new_val.to_string()));
                Ok(new_val)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn sadd(&self, key: &str, members: &[String]) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let entry = shard.data.entry(key.to_string()).or_insert_with(|| Entry {
            value: StoreValue::Set(HashSet::new()),
            expires_at: None,
        });
        match &mut entry.value {
            StoreValue::Set(set) => {
                let mut added = 0i64;
                for m in members { if set.insert(m.clone()) { added += 1; } }
                Ok(added)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn srem(&self, key: &str, members: &[String]) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key) {
            Some(entry) if !entry.is_expired() => match &mut entry.value {
                StoreValue::Set(set) => {
                    let mut removed = 0i64;
                    for m in members { if set.remove(m) { removed += 1; } }
                    Ok(removed)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(0),
        }
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<String>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Set(set) => Ok(set.iter().cloned().collect()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(vec![]),
        }
    }

    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Set(set) => Ok(set.contains(member)),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(false),
        }
    }

    pub fn scard(&self, key: &str) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Set(set) => Ok(set.len() as i64),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(0),
        }
    }

    fn collect_set(&self, key: &str) -> Result<HashSet<String>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                StoreValue::Set(set) => Ok(set.clone()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(HashSet::new()),
        }
    }

    pub fn sunion(&self, keys: &[String]) -> Result<Vec<String>, String> {
        let mut result = HashSet::new();
        for key in keys { result.extend(self.collect_set(key)?); }
        Ok(result.into_iter().collect())
    }

    pub fn sinter(&self, keys: &[String]) -> Result<Vec<String>, String> {
        if keys.is_empty() { return Ok(vec![]); }
        let mut result = self.collect_set(&keys[0])?;
        for key in &keys[1..] { let set = self.collect_set(key)?; result.retain(|m| set.contains(m)); }
        Ok(result.into_iter().collect())
    }

    pub fn sdiff(&self, keys: &[String]) -> Result<Vec<String>, String> {
        if keys.is_empty() { return Ok(vec![]); }
        let mut result = self.collect_set(&keys[0])?;
        for key in &keys[1..] { let set = self.collect_set(key)?; result.retain(|m| !set.contains(m)); }
        Ok(result.into_iter().collect())
    }

    pub fn dump_all(&self) -> Vec<DumpEntry> {
        let now = Instant::now();
        let mut entries = Vec::new();
        for shard in self.shards.iter() {
            let shard = shard.read();
            for (key, entry) in shard.data.iter() {
                if entry.is_expired() { continue; }
                let ttl_ms = entry.expires_at.map(|exp| exp.duration_since(now).as_millis() as i64).unwrap_or(0);
                entries.push(DumpEntry {
                    key: key.clone(),
                    value: match &entry.value {
                        StoreValue::Str(s) => DumpValue::Str(String::from_utf8_lossy(s).into_owned()),
                        StoreValue::List(l) => DumpValue::List(l.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect()),
                        StoreValue::Hash(h) => DumpValue::Hash(h.iter().map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into_owned())).collect()),
                        StoreValue::Set(s) => DumpValue::Set(s.iter().cloned().collect()),
                    },
                    ttl_ms,
                });
            }
        }
        entries
    }

    pub fn load_entry(&self, key: String, value: DumpValue, ttl: Option<Duration>) {
        let idx = self.shard_index(&key);
        let mut shard = self.shards[idx].write();
        let store_value = match value {
            DumpValue::Str(s) => StoreValue::Str(Bytes::from(s)),
            DumpValue::List(l) => StoreValue::List(l.into_iter().map(Bytes::from).collect()),
            DumpValue::Hash(h) => StoreValue::Hash(h.into_iter().map(|(k, v)| (k, Bytes::from(v))).collect()),
            DumpValue::Set(s) => StoreValue::Set(s.into_iter().collect()),
        };
        let expires_at = ttl.map(|d| Instant::now() + d);
        shard.data.insert(key, Entry { value: store_value, expires_at });
    }
}

pub enum DumpValue {
    Str(String),
    List(Vec<String>),
    Hash(Vec<(String, String)>),
    Set(Vec<String>),
}

pub struct DumpEntry {
    pub key: String,
    pub value: DumpValue,
    pub ttl_ms: i64,
}

struct GlobMatcher {
    pattern: Vec<char>,
}

impl GlobMatcher {
    fn new(pattern: &str) -> Self {
        Self { pattern: pattern.chars().collect() }
    }

    fn matches(&self, s: &str) -> bool {
        if self.pattern.len() == 1 && self.pattern[0] == '*' { return true; }
        let s: Vec<char> = s.chars().collect();
        Self::do_match(&self.pattern, &s, 0, 0)
    }

    fn do_match(pattern: &[char], s: &[char], pi: usize, si: usize) -> bool {
        if pi == pattern.len() && si == s.len() { return true; }
        if pi == pattern.len() { return false; }
        if pattern[pi] == '*' {
            for i in si..=s.len() { if Self::do_match(pattern, s, pi + 1, i) { return true; } }
            return false;
        }
        if si == s.len() { return false; }
        if pattern[pi] == '?' || pattern[pi] == s[si] { return Self::do_match(pattern, s, pi + 1, si + 1); }
        false
    }
}
