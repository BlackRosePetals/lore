// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The keyspace: each key's payload and hash, derived from the store's seed, and the variants
//! (partition and context) a key is stored under.
//!
//! A payload starts with a header naming its key and length, and the rest is derived from the
//! key, so a payload read back is checked byte for byte and a wrong one names whose it is. With
//! fake hashes, which the default, a hash is built from its key rather than hashed from the
//! payload: the store never checks a hash outside `verify`, and building them lets a run pick the
//! groups and buckets its keys land in. Real hashes leave the placement to the hash function, and
//! let `verify` hash the payloads.
//!
//! With `lists`, every sixteenth key is a fragment list instead: its payload is the references to
//! two to four plain keys, its children, and its content theirs. The first child is one of the
//! fifteen keys after it, so that no two lists hold the same references.

use std::fmt::Write as _;
use std::path::Path;

use bytes::Bytes;
use lore_storage::Address;
use lore_storage::Context;
use lore_storage::FRAGMENT_SIZE_THRESHOLD;
use lore_storage::Fragment;
use lore_storage::FragmentFlags;
use lore_storage::FragmentReference;
use lore_storage::Hash;
use lore_storage::ImmutableStoreSettings;
use lore_storage::Partition;

use crate::args::Args;

pub const MAGIC: &[u8; 4] = b"TORT";
pub const HEADER: usize = 16;
pub const CONFIG_FILE: &str = "stress.cfg";

pub fn splitmix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A small deterministic generator, so a run is repeatable from its seed.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(splitmix(seed))
    }

    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        splitmix(self.0)
    }

    pub fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 { 0 } else { self.next() % bound }
    }

    pub fn chance(&mut self, probability: f64) -> bool {
        ((self.next() >> 11) as f64 / (1u64 << 53) as f64) < probability
    }
}

/// What a store directory holds and how it is opened, fixed when the directory is first used so
/// that every later `work` and `verify` of it agrees.
#[derive(Clone, Debug)]
pub struct Config {
    pub seed: u64,
    pub keys: u64,
    /// The groups keys land in; empty for every group.
    pub groups: Vec<u8>,
    pub partitions: u32,
    pub contexts: u32,
    /// How many of the partitions, the last ones, store their payloads durable, which a client
    /// store may evict.
    pub evictable_partitions: u32,
    pub max_payload: usize,
    pub real_hash: bool,
    /// Server-shaped settings: nothing protected from eviction, every partition isolated, groups
    /// at 256 buckets from the start.
    pub server: bool,
    pub fan_out_threshold: usize,
    pub initial_level: usize,
    /// Whether every sixteenth key is a fragment list over other keys.
    pub lists: bool,
}

impl Config {
    /// The configuration the command line gives, with defaults for what it leaves out.
    pub fn from_args(args: &Args) -> Config {
        let groups = match args.get("groups") {
            Some("all") => vec![],
            Some(list) if list.contains(',') => list
                .split(',')
                .filter_map(|group| u8::from_str_radix(group.trim(), 16).ok())
                .collect(),
            other => {
                let count = other.and_then(|count| count.parse().ok()).unwrap_or(4u32);
                (0..count).map(|index| (index * 67 % 256) as u8).collect()
            }
        };
        let server = args.flag("server");
        Config {
            seed: args.number("seed", 1),
            keys: args.number("keys", 4000),
            groups,
            partitions: args.number("partitions", 3).clamp(1, 16) as u32,
            contexts: args.number("contexts", 2).clamp(1, 16) as u32,
            evictable_partitions: args.number("evictable-partitions", 1) as u32,
            max_payload: (args.number("max-payload", FRAGMENT_SIZE_THRESHOLD as u64) as usize)
                .clamp(HEADER + 1, FRAGMENT_SIZE_THRESHOLD),
            real_hash: args.flag("real-hash"),
            server,
            fan_out_threshold: args.number("fan-out-threshold", 24).max(1) as usize,
            initial_level: args.number("initial-level", if server { 256 } else { 1 }) as usize,
            lists: args.flag("lists"),
        }
    }

    /// The configuration `dir` was first used with, or the one `args` give, written to `dir` when
    /// it has none.
    pub fn load_or_create(dir: &Path, args: &Args) -> std::io::Result<Config> {
        let path = dir.join(CONFIG_FILE);
        if let Ok(text) = std::fs::read_to_string(&path) {
            return Ok(Config::parse(&text));
        }
        std::fs::create_dir_all(dir)?;
        let config = Config::from_args(args);
        std::fs::write(&path, config.serialize())?;
        Ok(config)
    }

    pub fn load(dir: &Path) -> std::io::Result<Config> {
        Ok(Config::parse(&std::fs::read_to_string(
            dir.join(CONFIG_FILE),
        )?))
    }

    pub fn serialize(&self) -> String {
        let mut text = String::new();
        let groups: Vec<String> = self.groups.iter().map(|g| format!("{g:02x}")).collect();
        let _ = writeln!(text, "seed={}", self.seed);
        let _ = writeln!(text, "keys={}", self.keys);
        let _ = writeln!(text, "groups={}", groups.join(","));
        let _ = writeln!(text, "partitions={}", self.partitions);
        let _ = writeln!(text, "contexts={}", self.contexts);
        let _ = writeln!(text, "evictable_partitions={}", self.evictable_partitions);
        let _ = writeln!(text, "max_payload={}", self.max_payload);
        let _ = writeln!(text, "real_hash={}", self.real_hash);
        let _ = writeln!(text, "server={}", self.server);
        let _ = writeln!(text, "fan_out_threshold={}", self.fan_out_threshold);
        let _ = writeln!(text, "initial_level={}", self.initial_level);
        let _ = writeln!(text, "lists={}", self.lists);
        text
    }

    fn parse(text: &str) -> Config {
        let mut config = Config::from_args(&Args::parse(std::iter::empty()));
        for line in text.lines() {
            let Some((name, value)) = line.split_once('=') else {
                continue;
            };
            match name {
                "seed" => config.seed = value.parse().unwrap_or(config.seed),
                "keys" => config.keys = value.parse().unwrap_or(config.keys),
                "groups" => {
                    config.groups = value
                        .split(',')
                        .filter_map(|group| u8::from_str_radix(group, 16).ok())
                        .collect();
                }
                "partitions" => config.partitions = value.parse().unwrap_or(config.partitions),
                "contexts" => config.contexts = value.parse().unwrap_or(config.contexts),
                "evictable_partitions" => {
                    config.evictable_partitions =
                        value.parse().unwrap_or(config.evictable_partitions);
                }
                "max_payload" => config.max_payload = value.parse().unwrap_or(config.max_payload),
                "real_hash" => config.real_hash = value == "true",
                "server" => config.server = value == "true",
                "fan_out_threshold" => {
                    config.fan_out_threshold = value.parse().unwrap_or(config.fan_out_threshold);
                }
                "initial_level" => {
                    config.initial_level = value.parse().unwrap_or(config.initial_level);
                }
                "lists" => config.lists = value == "true",
                _ => {}
            }
        }
        config
    }

    /// The settings the store is opened with. `flush_background` and `flush_delay_seconds` vary
    /// per run, so the caller passes them.
    pub fn settings(
        &self,
        flush_background: bool,
        flush_delay_seconds: u64,
    ) -> ImmutableStoreSettings {
        ImmutableStoreSettings {
            protect_local_fragment: !self.server,
            implicit_durable_stored: self.server,
            isolate_partitions: self.server,
            flush_background,
            flush_delay_seconds,
            initial_fan_out_level: self.initial_level,
            fan_out_threshold: self.fan_out_threshold,
            ..ImmutableStoreSettings::default()
        }
    }

    /// Whether `key` is a fragment list, see the module documentation.
    pub fn is_list(&self, key: u64) -> bool {
        self.lists && key.is_multiple_of(16) && key + 16 < self.keys
    }

    /// The plain keys list `key` references, in order.
    pub fn children(&self, key: u64) -> Vec<u64> {
        // A child holds at least as many bytes as its reference, which a list may not exceed.
        let fits = |child: u64| !self.is_list(child) && self.plain_len(child) >= 64;
        let first = (key + 1..key + 16)
            .find(|child| fits(*child))
            .unwrap_or(key + 1);
        let mut children = vec![first];
        let count = 2 + splitmix(self.seed ^ key ^ 0x11C7) % 3;
        let mut attempt = 0u64;
        while (children.len() as u64) < count && attempt < 64 {
            attempt += 1;
            let child =
                splitmix(self.seed ^ key.wrapping_mul(0x9E37) ^ (attempt << 32)) % self.keys;
            if fits(child) && !children.contains(&child) {
                children.push(child);
            }
        }
        children
    }

    /// The length of the bytes stored for `key`: its payload, or a list's references.
    pub fn payload_len(&self, key: u64) -> usize {
        if self.is_list(key) {
            return self.children(key).len() * size_of::<FragmentReference>();
        }
        self.plain_len(key)
    }

    /// The length of the content `key` describes: its payload, or a list's children together.
    pub fn content_len(&self, key: u64) -> u64 {
        if self.is_list(key) {
            return self
                .children(key)
                .iter()
                .map(|child| self.plain_len(*child) as u64)
                .sum();
        }
        self.plain_len(key) as u64
    }

    fn plain_len(&self, key: u64) -> usize {
        let roll = splitmix(self.seed ^ key.wrapping_mul(0x2545_F491_4F6C_DD1D));
        let size = splitmix(roll);
        let max = self.max_payload;
        let len = match roll % 100 {
            0..70 => 32 + (size % 4064) as usize,
            70..95 => 4096 + (size % 61440) as usize,
            _ => 65536 + (size % (max.saturating_sub(65536).max(1)) as u64) as usize,
        };
        len.clamp(HEADER, max)
    }

    pub fn payload(&self, key: u64) -> Bytes {
        if self.is_list(key) {
            let mut references = Vec::new();
            let mut offset = 0u64;
            for child in self.children(key) {
                let reference = FragmentReference {
                    hash: self.hash(child),
                    offset_content: offset,
                };
                references.extend_from_slice(zerocopy::IntoBytes::as_bytes(&reference));
                offset += self.plain_len(child) as u64;
            }
            return Bytes::from(references);
        }
        let len = self.plain_len(key);
        let mut payload = vec![0u8; len];
        payload[..4].copy_from_slice(MAGIC);
        payload[4..12].copy_from_slice(&key.to_le_bytes());
        payload[12..16].copy_from_slice(&(len as u32).to_le_bytes());
        let base = splitmix(self.seed ^ (key << 20) ^ 0xB0D7);
        for (index, chunk) in payload[HEADER..].chunks_mut(8).enumerate() {
            let bytes = splitmix(base.wrapping_add(index as u64)).to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
        Bytes::from(payload)
    }

    pub fn group_of(&self, key: u64) -> u8 {
        if self.groups.is_empty() {
            (splitmix(self.seed ^ key ^ 0x6A09) >> 56) as u8
        } else {
            self.groups[(key % self.groups.len() as u64) as usize]
        }
    }

    pub fn hash(&self, key: u64) -> Hash {
        if self.real_hash {
            return lore_storage::hash::hash_slice(&self.payload(key));
        }
        let mut hash = [0u8; 32];
        hash[0] = self.group_of(key);
        hash[1] = (splitmix(self.seed ^ key ^ 0x0B0C) >> 56) as u8;
        hash[2..10].copy_from_slice(&key.to_le_bytes());
        let check = splitmix(self.seed.wrapping_mul(31).wrapping_add(key));
        for (index, byte) in hash[10..].iter_mut().enumerate() {
            *byte = (splitmix(check.wrapping_add(index as u64)) >> 24) as u8;
        }
        Hash::from(hash)
    }

    /// The key a fake hash was built from, when it is one this keyspace builds.
    pub fn key_of_fake(&self, hash: &Hash) -> Option<u64> {
        let key = u64::from_le_bytes(hash.data()[2..10].try_into().ok()?);
        (key < self.keys && self.hash(key) == *hash).then_some(key)
    }

    pub fn variants(&self) -> u32 {
        self.partitions * self.contexts
    }

    pub fn partition(&self, variant: u32) -> Partition {
        Partition::from([0x40 + (variant / self.contexts) as u8; 16])
    }

    pub fn context(&self, variant: u32) -> Context {
        match variant % self.contexts {
            0 => Context::from([0u8; 16]),
            context => Context::from([0xC0 + context as u8; 16]),
        }
    }

    pub fn address(&self, key: u64, variant: u32) -> Address {
        Address {
            hash: self.hash(key),
            context: self.context(variant),
        }
    }

    /// The variant stored under `partition` and `context`, when they are one of the keyspace's.
    pub fn variant_of(&self, partition: &Partition, context: &Context) -> Option<u32> {
        (0..self.variants()).find(|variant| {
            self.partition(*variant) == *partition && self.context(*variant) == *context
        })
    }

    /// Whether eviction may take the entries of `variant`, whose payloads are stored durable.
    pub fn evictable(&self, variant: u32) -> bool {
        self.server
            || variant / self.contexts >= self.partitions.saturating_sub(self.evictable_partitions)
    }

    pub fn fragment(&self, key: u64, variant: u32) -> Fragment {
        let mut flags = FragmentFlags::PayloadStoredLocal.bits();
        if self.evictable(variant) && !self.server {
            flags |= FragmentFlags::PayloadStoredDurable.bits();
        }
        if self.is_list(key) {
            flags |= FragmentFlags::PayloadFragmented.bits();
        }
        Fragment {
            flags,
            size_payload: self.payload_len(key) as u32,
            size_content: self.content_len(key),
        }
    }

    /// What `bytes`, read back for `key`, are.
    pub fn classify(&self, key: u64, bytes: &[u8]) -> Served {
        if bytes == self.payload(key).as_ref() {
            return Served::Correct;
        }
        if bytes.iter().all(|byte| *byte == 0) {
            return Served::Zeros;
        }
        let expected = self.payload(key);
        if bytes.len() == expected.len()
            && bytes
                .iter()
                .zip(expected.iter())
                .all(|(byte, wanted)| *byte == 0 || byte == wanted)
        {
            return Served::PartlyZeroed;
        }
        if bytes.len() >= HEADER && &bytes[..4] == MAGIC {
            let other = u64::from_le_bytes(bytes[4..12].try_into().unwrap_or_default());
            if other != key && other < self.keys && bytes == self.payload(other).as_ref() {
                return Served::OtherKey(other);
            }
        }
        // Where the bytes were taken from inside another payload, the header is elsewhere.
        if let Some(position) = bytes.windows(4).position(|window| window == MAGIC)
            && position + HEADER <= bytes.len()
        {
            let other = u64::from_le_bytes(
                bytes[position + 4..position + 12]
                    .try_into()
                    .unwrap_or_default(),
            );
            return Served::Shifted { other, position };
        }
        Served::Garbage
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Served {
    Correct,
    Zeros,
    /// The payload with some of its bytes zeroed, as an obliterate's zeroing lands across a read.
    PartlyZeroed,
    /// The whole payload of another key.
    OtherKey(u64),
    /// Bytes holding the start of another key's payload at `position`, as when a location points
    /// into bytes written later over the range it named.
    Shifted {
        other: u64,
        position: usize,
    },
    Garbage,
}

impl std::fmt::Display for Served {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Served::Correct => write!(f, "correct"),
            Served::Zeros => write!(f, "zeros"),
            Served::PartlyZeroed => write!(f, "partly-zeroed"),
            Served::OtherKey(other) => write!(f, "payload-of-key-{other}"),
            Served::Shifted { other, position } => {
                write!(f, "bytes-holding-key-{other}-at-{position}")
            }
            Served::Garbage => write!(f, "garbage"),
        }
    }
}
