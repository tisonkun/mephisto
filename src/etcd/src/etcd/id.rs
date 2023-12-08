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

use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};

const COUNT_LEN: usize = 8;
const TIMESTAMP_LEN: usize = 5 * 8;
const SUFFIX_LEN: usize = TIMESTAMP_LEN + COUNT_LEN;

/// IdGen generates unique identifiers based on counters, timestamps, and
/// a node member ID.
///
/// The initial id is in this format:
/// High order 2 bytes are from memberID, next 5 bytes are from timestamp,
/// and low order one byte is a counter.
/// | prefix   | suffix              |
/// | 2 bytes  | 5 bytes   | 1 byte  |
/// | memberID | timestamp | cnt     |
///
/// The timestamp 5 bytes is different when the machine is restart
/// after 1 ms and before 35 years.
///
/// It increases suffix to generate the next id.
/// The count field may overflow to timestamp field, which is intentional.
/// It helps to extend the event window to 2^56. This doesn't break that
/// id generated after restart is unique because etcd throughput is <<
/// 256req/ms(250k reqs/second).
#[derive(Debug)]
pub struct IdGen {
    // high order 2 bytes
    prefix: u64,
    // low order 6 bytes
    suffix: AtomicU64,
}

impl IdGen {
    pub fn new(member_id: u64, now: DateTime<Utc>) -> IdGen {
        assert!(
            member_id <= u16::MAX as u64,
            "member_id({member_id}) overflows u16"
        );
        let prefix = member_id << SUFFIX_LEN;
        let unix_milli = (now - DateTime::UNIX_EPOCH).num_milliseconds() as u64;
        let suffix = AtomicU64::new(lowbit(unix_milli, TIMESTAMP_LEN) << COUNT_LEN);
        IdGen { prefix, suffix }
    }

    /// `next` generates a id that is unique.
    pub fn next(&self) -> u64 {
        let suffix = self.suffix.fetch_add(1, Ordering::SeqCst) + 1;
        self.prefix | lowbit(suffix, SUFFIX_LEN)
    }
}

fn lowbit(x: u64, n: usize) -> u64 {
    x & (u64::MAX >> (64 - n))
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, Utc};

    use crate::etcd::id::IdGen;

    #[test]
    fn test_new_generator() {
        let g = IdGen::new(0x12, DateTime::default() + Duration::milliseconds(0x3456));
        assert_eq!(0x12000000345601, g.next());
    }

    #[test]
    fn test_new_generator_unique() {
        let g = IdGen::new(0, DateTime::UNIX_EPOCH);
        let id = g.next();
        let g1 = IdGen::new(1, DateTime::UNIX_EPOCH);
        assert_ne!(id, g1.next());
        let g2 = IdGen::new(0, Utc::now());
        assert_ne!(id, g2.next());
    }

    #[test]
    fn test_next() {
        let g = IdGen::new(0x12, DateTime::default() + Duration::milliseconds(0x3456));
        for i in 0..1000 {
            assert_eq!(0x12000000345601 + i, g.next());
        }
    }
}
