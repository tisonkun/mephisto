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

use std::{collections::HashMap, time::Duration};

use anyhow::anyhow;
use crossbeam::channel::{Receiver, Sender};
use mephisto_raft::{
    eraftpb::{
        raft_message_service_client::RaftMessageServiceClient,
        raft_message_service_server::{RaftMessageService, RaftMessageServiceServer},
        Message,
    },
    Peer,
};
use tokio::{
    runtime::Runtime,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    time::sleep,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{
    codegen::tokio_stream::StreamExt, transport::Server, Request, Response, Status, Streaming,
};
use tracing::{error, error_span, info, Instrument};

pub struct RaftService {
    tx_inbound: Sender<Message>,
}

#[tonic::async_trait]
impl RaftMessageService for RaftService {
    async fn unordered(
        &self,
        request: Request<Streaming<Message>>,
    ) -> Result<Response<()>, Status> {
        let mut req = request.into_inner();
        while let Some(msg) = req.next().await {
            if let Err(err) = self.tx_inbound.send(msg?) {
                error!(?err, "broken pipe (tx_inbound)");
            }
        }
        Ok(Response::new(()))
    }
}

pub fn start_raft_service(
    this: Peer,
    peers: Vec<Peer>,
) -> std::io::Result<(Runtime, UnboundedSender<Message>, Receiver<Message>)> {
    let (tx_inbound, rx_inbound) = crossbeam::channel::unbounded();
    let (tx_outbound, rx_outbound) = tokio::sync::mpsc::unbounded_channel();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    // - inbound
    {
        async fn run_server(this: Peer, service: RaftService) -> anyhow::Result<()> {
            let addr = this.address.parse()?;
            let svc = RaftMessageServiceServer::new(service);
            let srv = Server::builder().add_service(svc);
            Ok(srv.serve(addr).await?)
        }
        let service = RaftService { tx_inbound };
        let this = this.clone();
        let span = error_span!("RaftService", id = this.id);
        runtime.spawn(
            async move {
                match run_server(this, service).await {
                    Ok(()) => info!("RaftService shutdown normally"),
                    Err(err) => error!(?err, "RaftService shutdown improperly"),
                }
            }
            .instrument(span),
        );
    }

    // - outbound
    {
        async fn run_client(
            this: Peer,
            peers: Vec<Peer>,
            mut rx_outbound: UnboundedReceiver<Message>,
        ) -> anyhow::Result<()> {
            let mut tx_outbounds = HashMap::new();

            for peer in peers {
                if peer.id != this.id {
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                    let addr = format!("http://{}", peer.address);
                    let request = Request::new(UnboundedReceiverStream::new(rx));
                    let mut client = RaftMessageServiceClient::connect(addr).await?;
                    tokio::spawn(async move {
                        // FIXME (@tisonkun) handle exception and propagate
                        let _ = client.unordered(request).await;
                    });
                    tx_outbounds.insert(peer.id, tx);
                }
            }

            while let Some(msg) = rx_outbound.recv().await {
                let tx = tx_outbounds
                    .get_mut(&msg.to)
                    .ok_or_else(|| anyhow!("channel not found ({}->{})", this.id, msg.to))?;
                tx.send(msg)?;
            }

            Ok(())
        }

        let span = error_span!("RaftService", id = this.id);
        runtime.spawn(
            async move {
                // FIXME (@tisonkun) retry connect
                sleep(Duration::from_secs(1)).await;
                match run_client(this, peers, rx_outbound).await {
                    Ok(()) => info!("RaftService shutdown normally"),
                    Err(err) => error!(?err, "RaftService shutdown improperly"),
                }
            }
            .instrument(span),
        );
    }

    Ok((runtime, tx_outbound, rx_inbound))
}
