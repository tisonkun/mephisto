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

use tonic::{Request, Response, Status};

use crate::etcd::pb::etcdserverpb::{
    kv_server::Kv, CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse,
    PutRequest, PutResponse, RangeRequest, RangeResponse, TxnRequest, TxnResponse,
};

pub struct KvService;

#[tonic::async_trait]
impl Kv for KvService {
    async fn range(
        &self,
        _request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        unimplemented!("range")
    }

    async fn put(&self, _request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        unimplemented!("put")
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
