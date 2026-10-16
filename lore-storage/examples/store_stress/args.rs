// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Command line arguments: positionals, `--name value` or `--name=value` options, and bare
//! `--flag`s.

use std::collections::HashMap;
use std::path::PathBuf;

pub struct Args {
    pub positional: Vec<String>,
    options: HashMap<String, String>,
}

impl Args {
    pub fn parse(args: impl Iterator<Item = String>) -> Args {
        let mut positional = vec![];
        let mut options = HashMap::new();
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            let Some(name) = arg.strip_prefix("--") else {
                positional.push(arg);
                continue;
            };
            if let Some((name, value)) = name.split_once('=') {
                options.insert(name.to_string(), value.to_string());
            } else if args.peek().is_some_and(|next| !next.starts_with("--")) {
                options.insert(name.to_string(), args.next().unwrap_or_default());
            } else {
                options.insert(name.to_string(), "true".to_string());
            }
        }
        Args {
            positional,
            options,
        }
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.options.get(name).map(String::as_str)
    }

    pub fn number(&self, name: &str, default: u64) -> u64 {
        self.get(name).and_then(parse_number).unwrap_or(default)
    }

    pub fn float(&self, name: &str, default: f64) -> f64 {
        self.get(name)
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    pub fn flag(&self, name: &str) -> bool {
        self.get(name)
            .is_some_and(|value| matches!(value, "true" | "1" | "yes"))
    }

    pub fn string(&self, name: &str, default: &str) -> String {
        self.get(name).unwrap_or(default).to_string()
    }

    /// The store directory, the positional after the command.
    pub fn dir(&self) -> Option<PathBuf> {
        self.positional.get(1).map(PathBuf::from)
    }
}

/// A count, with an optional `k`, `m` or `g` suffix, binary for sizes.
pub fn parse_number(value: &str) -> Option<u64> {
    let value = value.trim();
    let (digits, scale) = match value.chars().last()? {
        'k' | 'K' => (&value[..value.len() - 1], 1024),
        'm' | 'M' => (&value[..value.len() - 1], 1024 * 1024),
        'g' | 'G' => (&value[..value.len() - 1], 1024 * 1024 * 1024),
        _ => (value, 1),
    };
    digits.parse::<u64>().ok().map(|number| number * scale)
}
