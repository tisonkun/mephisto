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

use std::{
    collections::BTreeMap,
    thread,
    time::{Duration, Instant},
};

use crossbeam::channel::{Receiver, Select, Sender, TryRecvError};
use mephisto_raft::{
    eraftpb,
    eraftpb::{Entry, EntryType, Message},
    fatal,
    storage::MemStorage,
    Config, Peer, RawNode, StateRole,
};
use prost::encoding::{decode_varint, encode_varint, encoded_len_varint};
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tracing::{debug, error, error_span, info};

use crate::raft::ApiProposeMessage;

pub struct RaftNode {
    id: u64,

    node: RawNode<MemStorage>,
    state: StateRole,

    rx_inbound: Receiver<Message>,
    tx_outbound: UnboundedSender<Message>,

    tx_shutdown: Sender<()>,
    rx_shutdown: Receiver<()>,
    tick: Receiver<Instant>,

    tx_api: Sender<ApiProposeMessage>,
    rx_api: Receiver<ApiProposeMessage>,
    responses: BTreeMap<u64, oneshot::Sender<Vec<u8>>>,
}

impl RaftNode {
    pub fn new(
        this: Peer,
        peers: Vec<Peer>,
        rx_inbound: Receiver<Message>,
        tx_outbound: UnboundedSender<Message>,
    ) -> mephisto_raft::Result<RaftNode> {
        let id = this.id;

        let config = {
            let mut config = Config::new(id);
            config.pre_vote = true;
            config.priority = (1 << id) as i64;
            config.election_tick = 10;
            config.heartbeat_tick = 1;
            config.max_size_per_msg = 1024 * 1024 * 1024;
            config.validate()?;
            config
        };

        let voters = peers.iter().map(|p| p.id).collect::<Vec<_>>();
        let storage = MemStorage::new_with_conf_state((voters, vec![]));
        storage.wl().mut_hard_state().term = 1;

        let (tx_shutdown, rx_shutdown) = crossbeam::channel::bounded(1);
        let (tx_api, rx_api) = crossbeam::channel::unbounded();
        let tick = crossbeam::channel::tick(Duration::from_millis(100));

        Ok(RaftNode {
            id,
            node: RawNode::new(&config, storage)?,
            state: StateRole::Follower,
            rx_inbound,
            tx_outbound,
            tx_shutdown,
            rx_shutdown,
            tick,
            tx_api,
            rx_api,
            responses: BTreeMap::new(),
        })
    }

    pub fn tx_shutdown(&self) -> Sender<()> {
        self.tx_shutdown.clone()
    }

    pub fn tx_api(&self) -> Sender<ApiProposeMessage> {
        self.tx_api.clone()
    }

    pub fn run(self) {
        thread::spawn(move || {
            error_span!("RaftNode", id = self.id).in_scope(move || match self.do_run() {
                Ok(()) => info!("RaftNode shutdown normally"),
                Err(err) => error!(?err, "RaftNode shutdown improperly"),
            })
        });
    }

    fn do_run(mut self) -> anyhow::Result<()> {
        loop {
            // waiting until anyone of the inputs available
            let mut select = Select::new();
            select.recv(&self.rx_shutdown);
            select.recv(&self.rx_inbound);
            select.recv(&self.rx_api);
            select.recv(&self.tick);
            select.ready();

            // stop if shutting down
            if !matches!(self.rx_shutdown.try_recv(), Err(TryRecvError::Empty)) {
                return Ok(());
            }

            if self.tick.try_recv().is_ok() {
                self.node.tick();
            }

            for msg in self.rx_inbound.try_iter() {
                if mephisto_raft::raw_node::is_local_msg(msg.msg_type()) {
                    self.node.raft.step(msg)?;
                } else {
                    self.node.step(msg)?;
                }
            }

            for ApiProposeMessage { id, data, resp } in self.rx_api.try_iter() {
                let mut context = Vec::with_capacity(encoded_len_varint(id));
                encode_varint(id, &mut context);
                self.node.propose(context, data)?;
                self.responses.insert(id, resp);
            }

            self.on_ready()?;
        }
    }

    fn on_ready(&mut self) -> anyhow::Result<()> {
        if !self.node.has_ready() {
            return Ok(());
        }

        let mut ready = self.node.ready();

        if let Some(ss) = ready.ss() {
            if ss.raft_state != self.state {
                info!(
                    "changing raft node role from {:?} to {:?}",
                    self.state, ss.raft_state
                );
                self.state = ss.raft_state;
            }
        }

        for msg in ready.take_messages() {
            self.handle_message(msg, false);
        }

        for ent in ready.take_committed_entries() {
            self.handle_committed_entry(ent);
        }

        if !ready.entries().is_empty() {
            self.node.mut_store().wl().append(ready.entries())?;
        }

        if let Some(hs) = ready.hs() {
            self.node.mut_store().wl().set_hard_state(hs.clone());
        }

        for msg in ready.take_persisted_messages() {
            self.handle_message(msg, true);
        }

        let mut light_ready = self.node.advance(ready);

        for msg in light_ready.take_messages() {
            self.handle_message(msg, false);
        }

        for ent in light_ready.take_committed_entries() {
            self.handle_committed_entry(ent);
        }

        self.node.advance_apply();
        Ok(())
    }

    fn handle_committed_entry(&mut self, ent: Entry) {
        if ent.data.is_empty() {
            // empty entry on leader elected
            return;
        }

        // currently only normal entry
        assert_eq!(ent.entry_type(), EntryType::EntryNormal);
        let id = decode_varint(&mut ent.context.as_slice()).expect("malformed context");
        let rx = self.responses.remove(&id).expect("response channel absent");

        debug!("notify request {id} has been committed");
        rx.send(ent.data).expect("response channel has been closed");
    }

    fn handle_message(&self, msg: Message, _is_persisted_msg: bool) {
        match msg.msg_type() {
            eraftpb::MessageType::MsgAppend
            | eraftpb::MessageType::MsgRequestPreVote
            | eraftpb::MessageType::MsgAppendResponse
            | eraftpb::MessageType::MsgRequestPreVoteResponse
            | eraftpb::MessageType::MsgRequestVote
            | eraftpb::MessageType::MsgRequestVoteResponse
            | eraftpb::MessageType::MsgHeartbeat
            | eraftpb::MessageType::MsgHeartbeatResponse => {
                assert_ne!(
                    msg.to, self.id,
                    "cannot handle message send to self {msg:?}"
                );
                self.tx_outbound
                    .send(msg)
                    .expect("outbound channel has been closed");
            }
            _ => fatal!("unimplemented {msg:?}"),
        }
    }
}
