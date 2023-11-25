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

use std::{env::temp_dir, sync::Arc};

use mephistio_etcd::etcd::{
    pb::etcdserverpb::kv_server::KvServer, service::KvService, state::EtcdState,
};
use tokio::sync::Mutex;
use tonic::transport::Server;

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let state = EtcdState::new(dbg!(temp_dir()));
    let service = KvService {
        state: Arc::new(Mutex::new(state)),
    };

    runtime.block_on(async move {
        let addr = "127.0.0.1:2379".parse()?;
        Server::builder()
            .add_service(KvServer::new(service))
            .serve(addr)
            .await?;
        Ok(())
    })
}
