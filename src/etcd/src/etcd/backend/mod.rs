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
use rocksdb::{Direction, IteratorMode, DB};

pub type RocksDBResult<T> = Result<T, rocksdb::Error>;

pub struct RocksDBBackend {
    db: DB,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BackendRange {
    pub kvs: Vec<(Bytes, Bytes)>,
}

// TODO (@tisonkun) separate readTx and batchTx
impl RocksDBBackend {
    pub fn new(path: PathBuf) -> Self {
        let db = DB::open_default(path).expect("cannot open rocksdb");
        Self { db }
    }

    pub fn unsafe_put(&mut self, key: Bytes, value: Bytes) -> RocksDBResult<()> {
        self.db.put(key, value)
    }

    pub fn unsafe_range(
        &self,
        key: Bytes,
        end: Option<Bytes>,
        limit: usize,
    ) -> RocksDBResult<BackendRange> {
        #[allow(clippy::type_complexity)]
        let (bound, pred): (usize, Box<dyn Fn(&Bytes) -> bool>) = match end {
            None => (1, Box::new(|k: &Bytes| k == &key)),
            Some(end) => {
                let bound = if limit > 0 { limit } else { usize::MAX };
                (bound, Box::new(move |k: &Bytes| k < &end))
            }
        };

        let it = self
            .db
            .iterator(IteratorMode::From(&key, Direction::Forward));

        let mut kvs = vec![];
        for item in it {
            let (k, v) = item?;
            let (k, v) = (Bytes::from(k), Bytes::from(v));
            if !pred(&k) {
                break;
            }
            kvs.push((k, v));
            if kvs.len() >= bound {
                break;
            }
        }
        Ok(BackendRange { kvs })
    }
}
