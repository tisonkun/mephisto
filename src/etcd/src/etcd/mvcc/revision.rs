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

use derive_new::new;

/// `REV_BYTES_LEN` is the byte length of a normal revision.
/// First 8 bytes is the revision.main in big-endian format. The 9th byte
/// is a '_'. The last 8 bytes is the revision.sub in big-endian format.
const REV_BYTES_LEN: usize = 8 + 1 + 8;
/// `MARKED_REV_BYTES_LEN` is the byte length of marked revision.
/// The first `REV_BYTES_LEN` bytes represents a normal revision. The last
/// one byte is the mark.
const MARKED_REV_BYTES_LEN: usize = REV_BYTES_LEN + 1;
const MARK_TOMBSTONE: u8 = b't';

pub fn is_tombstone(bs: &[u8]) -> bool {
    bs.len() == MARKED_REV_BYTES_LEN && bs[MARKED_REV_BYTES_LEN - 1] == MARK_TOMBSTONE
}

/// A revision indicates modification of the key-value space.
/// The set of changes that share same main revision changes the key-value space atomically.
#[derive(new, Debug, Default, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Revision {
    /// `main` is the main revision of a set of changes that happen atomically.
    pub main: u64,

    /// `sub` is the sub revision of a change in a set of changes that happen
    /// atomically. Each change has different increasing sub revision in that
    /// set.
    pub sub: u64,
}

impl Revision {
    pub fn main(main: u64) -> Self {
        Self { main, sub: 0 }
    }

    pub fn from_bytes(bs: &[u8]) -> Self {
        assert_eq!(bs.len(), REV_BYTES_LEN);
        // SAFETY: length checked above
        let main = unsafe { u64::from_be_bytes(bs[0..8].try_into().unwrap_unchecked()) };
        let sub = unsafe { u64::from_be_bytes(bs[9..].try_into().unwrap_unchecked()) };
        Self { main, sub }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bs = Vec::with_capacity(MARKED_REV_BYTES_LEN);
        bs.extend(self.main.to_be_bytes());
        bs.push(b'_');
        bs.extend(self.sub.to_be_bytes());
        debug_assert_eq!(bs.len(), REV_BYTES_LEN);
        bs
    }

    pub fn to_tombstone_bytes(&self) -> Vec<u8> {
        let mut bs = self.to_bytes();
        bs.push(MARK_TOMBSTONE);
        debug_assert_eq!(bs.len(), MARKED_REV_BYTES_LEN);
        bs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_revisions() {
        let revisions = [
            Revision::default(),
            Revision::new(1, 0),
            Revision::new(1, 1),
            Revision::new(2, 0),
            Revision::new(u64::MAX, u64::MAX),
        ];

        for i in 0..revisions.len() - 1 {
            assert!(revisions[i] < revisions[i + 1])
        }
    }

    #[test]
    fn test_revision_codec() {
        let rev = Revision { main: 42, sub: 21 };
        assert!(!is_tombstone(rev.to_bytes().as_slice()));
        assert!(is_tombstone(rev.to_tombstone_bytes().as_slice()));
        assert_eq!(rev, Revision::from_bytes(rev.to_bytes().as_slice()));
    }
}
