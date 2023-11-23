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

fn main() -> std::io::Result<()> {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    let proto_root = "protos";
    println!("cargo:rerun-if-changed={proto_root}");

    tonic_build::configure().compile(
        &[
            "protos/auth.proto",
            "protos/kv.proto",
            "protos/rpc.proto",
            "protos/v3election.proto",
            "protos/v3lock.proto",
        ],
        &[proto_root],
    )
}
