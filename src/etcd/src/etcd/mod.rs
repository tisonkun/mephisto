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

use bytes::Bytes;

pub mod backend;
pub mod id;
pub mod mvcc;
pub mod pb;
pub mod service;
pub mod state;

// Determine if the range_end is a >= range. This works around etcd's workaround to fight with
// grpc-go encodes empty byte string as nil, but tonic encodes empty byte string as vec![].
// If it is a GTE range, then Some(Bytes::new()) is returned to indicate the empty byte
// string (vs None being no byte string).
fn make_gte_range(range_end: Vec<u8>) -> Option<Bytes> {
    if range_end.is_empty() {
        None
    } else if range_end.len() == 1 && range_end[0] == 0 {
        Some(Bytes::new())
    } else {
        Some(Bytes::from(range_end))
    }
}
