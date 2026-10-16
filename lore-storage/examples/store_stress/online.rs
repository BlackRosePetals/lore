// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! What each key must hold under each variant while a run goes on: the journal's expectations at
//! the start, then every operation the run makes, so that a read can be held to the store's state
//! at once instead of after a flush and a reopen. Only variants eviction may not take are tracked,
//! and a read is checked only when no operation on its key and variant ran alongside it.

use std::collections::HashMap;

use parking_lot::Mutex;

use crate::journal::Expect;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum State {
    #[default]
    Unknown,
    /// An entry, holding its payload when `payload`.
    Present {
        payload: bool,
    },
    Obliterated,
}

impl From<Expect> for State {
    fn from(expect: Expect) -> State {
        match expect {
            Expect::Present { payload } => State::Present { payload },
            Expect::Obliterated => State::Obliterated,
            Expect::Unknown => State::Unknown,
        }
    }
}

/// What an operation does to the key and variant it names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    /// A put with the payload.
    Store,
    /// A put without the payload.
    Describe,
    /// A copy to this key and variant.
    Copy,
    /// An obliterate of this key and variant.
    Obliterate,
    /// The obliterate of a fragment that obliterating its list may carry out.
    ObliterateChild,
}

/// How an operation ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Done,
    /// Failed, having found nothing to act on: a copy without a source.
    NotFound,
    Failed,
    /// Dropped before it finished.
    Cancelled,
}

#[derive(Default)]
struct Slot {
    running: u32,
    /// Changed at each start and end of an operation on the slot, so that a read can tell that
    /// none ran alongside it.
    version: u64,
    /// Whether operations overlapped since the slot last had none running, which leaves the order
    /// the store applied them in unknown.
    overlapped: bool,
    /// Whether an obliterate ever ran on the slot, after which a put without the payload may meet
    /// a tombstone, which it leaves as it is.
    obliterated_ever: bool,
    state: State,
}

pub struct Model {
    slots: Mutex<HashMap<(u64, u32), Slot>>,
    active: bool,
}

impl Model {
    pub fn new(active: bool) -> Model {
        Model {
            slots: Mutex::new(HashMap::new()),
            active,
        }
    }

    /// Start from what the journal settled for `slot`.
    pub fn seed(&self, slot: (u64, u32), state: State, obliterated_ever: bool) {
        if !self.active {
            return;
        }
        let mut slots = self.slots.lock();
        let entry = slots.entry(slot).or_default();
        entry.state = state;
        entry.obliterated_ever = obliterated_ever;
    }

    /// Record an operation on `slot` starting, `tracked` when the slot is one the model follows.
    pub fn begin(&self, slot: (u64, u32), effect: Effect, tracked: bool) -> Running<'_> {
        if self.active && tracked {
            let mut slots = self.slots.lock();
            let entry = slots.entry(slot).or_default();
            entry.version += 1;
            if entry.running > 0 {
                entry.overlapped = true;
            }
            entry.running += 1;
            if matches!(effect, Effect::Obliterate | Effect::ObliterateChild) {
                entry.obliterated_ever = true;
            }
        }
        Running {
            model: self,
            slot,
            effect,
            tracked: self.active && tracked,
            finished: false,
        }
    }

    fn end(&self, slot: (u64, u32), effect: Effect, outcome: Outcome) {
        let mut slots = self.slots.lock();
        let entry = slots.entry(slot).or_default();
        entry.version += 1;
        entry.running = entry.running.saturating_sub(1);
        entry.state = if entry.overlapped {
            State::Unknown
        } else {
            next(entry.state, entry.obliterated_ever, effect, outcome)
        };
        if entry.running == 0 {
            entry.overlapped = false;
        }
    }

    /// What `slot` holds, with its version for [`Model::still`], when it is known and no
    /// operation on it is running.
    pub fn settled(&self, slot: (u64, u32)) -> Option<(State, u64)> {
        if !self.active {
            return None;
        }
        let slots = self.slots.lock();
        let entry = slots.get(&slot)?;
        (entry.running == 0 && entry.state != State::Unknown)
            .then_some((entry.state, entry.version))
    }

    /// Whether no operation on `slot` started or ended since it was at `version`.
    pub fn still(&self, slot: (u64, u32), version: u64) -> bool {
        self.slots
            .lock()
            .get(&slot)
            .is_some_and(|entry| entry.version == version && entry.running == 0)
    }
}

/// What a slot holds after an operation on it, run alone, ended as `outcome`.
fn next(state: State, obliterated_ever: bool, effect: Effect, outcome: Outcome) -> State {
    match (effect, outcome) {
        (Effect::Store, Outcome::Done) => State::Present { payload: true },
        // A put without the payload leaves a tombstone as it is, and keeps a payload stored.
        (Effect::Describe, Outcome::Done) => match state {
            State::Present { payload } => State::Present { payload },
            State::Unknown if !obliterated_ever => State::Present { payload: false },
            _ => State::Unknown,
        },
        // A copy takes its source's payload, which may not be held here, and revives a tombstone.
        (Effect::Copy, Outcome::Done) => State::Present { payload: false },
        (Effect::Copy, Outcome::NotFound) => state,
        // A put or copy that failed or was dropped leaves an entry, but may have reached a
        // tombstone.
        (Effect::Store | Effect::Describe | Effect::Copy, _) => match state {
            State::Obliterated => State::Unknown,
            state => state,
        },
        (Effect::Obliterate, Outcome::Done) => State::Obliterated,
        (Effect::Obliterate | Effect::ObliterateChild, _) => State::Unknown,
    }
}

/// An operation in progress on a slot, which records how it ended when finished, or when dropped
/// unfinished, as a cancelled operation is.
pub struct Running<'a> {
    model: &'a Model,
    slot: (u64, u32),
    effect: Effect,
    tracked: bool,
    finished: bool,
}

impl Running<'_> {
    pub fn finish(mut self, outcome: Outcome) {
        self.finished = true;
        if self.tracked {
            self.model.end(self.slot, self.effect, outcome);
        }
    }
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        if !self.finished && self.tracked {
            self.model.end(self.slot, self.effect, Outcome::Cancelled);
        }
    }
}
