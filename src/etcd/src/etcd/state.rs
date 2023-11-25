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

use std::path::PathBuf;

use bytes::Bytes;
use prost::Message;

use crate::etcd::{
    backend::RocksDBBackend,
    mvcc::{key_index::IndexGet, revision::Revision, tree_index::TreeIndex},
    pb::mvccpb::KeyValue,
};

pub struct EtcdState {
    current_rev: u64,

    ti: TreeIndex,
    b: RocksDBBackend,
}

pub struct StateRange {
    pub kvs: Vec<KeyValue>,
    pub total: u64,
}

impl EtcdState {
    pub fn new(path: PathBuf) -> Self {
        Self {
            current_rev: 0,
            ti: TreeIndex::default(),
            b: RocksDBBackend::new(path),
        }
    }

    pub fn put(&mut self, key: Bytes, value: Bytes) -> u64 {
        let rev = {
            self.current_rev += 1;
            Revision::new0(self.current_rev)
        };

        let (c, ver) = if let Ok(IndexGet {
            created, version, ..
        }) = self.ti.get(key.clone(), rev.main)
        {
            // if the key exists before, use its previous created
            (created, version + 1)
        } else {
            (rev, 1)
        };

        let rev_bytes = rev.to_bytes();
        let kv = KeyValue {
            key: key.to_vec(),
            value: value.into(),
            create_revision: c.main as i64,
            mod_revision: rev.sub as i64,
            version: ver as i64,
            lease: 0,
        }
        .encode_to_vec();

        self.b
            .unsafe_put(rev_bytes.into(), kv.into())
            .unwrap_or_else(|_| todo!("handle Backend error"));
        self.ti.put(key, rev);

        rev.main
    }

    pub fn range(&self, key: Bytes, end: Option<Bytes>) -> StateRange {
        let rev = self.current_rev;

        let (revs, total) = self.ti.revisions(key, end, rev, 0);

        let mut kvs = vec![];
        for rev in revs.into_iter() {
            let rev_bytes = rev.to_bytes();
            let vs = self
                .b
                .unsafe_range(rev_bytes.into(), None, 0)
                .unwrap_or_else(|_| todo!("handle Backend error"))
                .kvs
                .into_iter()
                .map(|(_, v)| v)
                .collect::<Vec<_>>();
            assert_eq!(1, vs.len(), "range failed to find revision pair");
            let v = unsafe { vs.into_iter().next().unwrap_unchecked() };
            kvs.push(KeyValue::decode(v).expect("failed to decode mvccpb.KeyValue"));
        }
        StateRange { kvs, total }
    }
}
