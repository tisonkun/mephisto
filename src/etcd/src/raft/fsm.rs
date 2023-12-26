// Copyright 2023 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use mephisto_raft::{
    eraftpb::{Entry, EntryType},
    ReadState,
};
use prost::encoding::decode_varint;
use tokio::sync::oneshot;
use tracing::debug;

#[derive(Debug, Default)]
pub struct FSM {
    applied_index: u64,
    applies: BTreeMap<u64, oneshot::Sender<Vec<u8>>>,
    read_indices: BTreeMap<u64, oneshot::Sender<()>>,
    read_waiters: BTreeMap<u64, Vec<oneshot::Sender<()>>>,
}

impl FSM {
    pub fn register_apply(&mut self, id: u64, resp: oneshot::Sender<Vec<u8>>) {
        let prev = self.applies.insert(id, resp);
        debug_assert!(prev.is_none(), "duplicate register apply (id: {id})");
    }

    pub fn register_read_index(&mut self, id: u64, resp: oneshot::Sender<()>) {
        let prev = self.read_indices.insert(id, resp);
        debug_assert!(prev.is_none(), "duplicate register read_index (id: {id})");
    }

    pub fn apply(&mut self, ent: Entry) {
        assert!(
            self.applied_index < ent.index,
            "raft index backward (current: {}, incoming: {})",
            self.applied_index,
            ent.index
        );
        self.applied_index = ent.index;

        if ent.data.is_empty() {
            // empty entry on leader elected
            return;
        }

        // currently only normal entry
        assert_eq!(ent.entry_type(), EntryType::EntryNormal);

        // resp apply
        let id = decode_varint(&mut ent.context.as_slice()).expect("malformed context");
        let rx = self.applies.remove(&id).expect("resp channel absent");
        debug!("notify request {id} has been committed");
        rx.send(ent.data).expect("resp channel has been closed");

        // resp read
        while let Some(read_waiter) = self.read_waiters.first_entry() {
            let read_index = *read_waiter.key();
            if read_index > self.applied_index {
                break;
            }
            let waiters = read_waiter.remove();
            for waiter in waiters {
                waiter
                    .send(())
                    .unwrap_or_else(|_| debug!("resp channel has been closed"));
            }
        }
    }

    pub fn read_index(&mut self, state: ReadState) {
        let id = decode_varint(&mut state.request_ctx.as_slice()).expect("malformed context");
        let rx = self.read_indices.remove(&id).expect("resp channel absent");
        if state.index <= self.applied_index {
            rx.send(())
                .unwrap_or_else(|_| debug!("resp channel has been closed"));
        } else {
            self.read_waiters.entry(state.index).or_default().push(rx);
        }
    }
}
