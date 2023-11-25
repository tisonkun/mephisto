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

use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::etcd::{
    make_gte_range,
    pb::etcdserverpb::{
        kv_server::Kv, CompactionRequest, CompactionResponse, DeleteRangeRequest,
        DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse, TxnRequest,
        TxnResponse,
    },
    state::{EtcdState, StateRange},
};

pub struct KvService {
    pub state: Arc<Mutex<EtcdState>>,
}

#[tonic::async_trait]
impl Kv for KvService {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let RangeRequest { key, range_end, .. } = request.into_inner();
        let state = self.state.lock().await;
        let StateRange { kvs, total } = state.range(key.into(), make_gte_range(range_end));
        let more = total > kvs.len() as u64;
        Ok(Response::new(RangeResponse {
            header: None,
            kvs,
            more,
            count: total as i64,
        }))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let PutRequest { key, value, .. } = request.into_inner();
        let mut state = self.state.lock().await;
        state.put(key.into(), value.into());
        Ok(Response::new(PutResponse {
            header: None,
            prev_kv: None,
        }))
    }

    async fn delete_range(
        &self,
        _request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        unimplemented!("delete_range")
    }

    async fn txn(&self, _request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        unimplemented!("txn")
    }

    async fn compact(
        &self,
        _request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        unimplemented!("compact")
    }
}
