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

use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

use bytes::Bytes;
use derive_new::new;

use crate::etcd::mvcc::revision::Revision;

#[derive(Debug, Default, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct RevisionNotFound;

impl Display for RevisionNotFound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "mvcc: revision not found")
    }
}

impl Error for RevisionNotFound {}

/// KeyIndex stores the revisions of a key in the backend. Each keyIndex has at least one key
/// generation. Each generation might have several key versions.
///
/// Tombstone on a key appends a tombstone version at the end of the current generation and creates
/// a new empty generation. Each version of a key has an index pointing to the backend.
#[derive(Debug)]
pub struct KeyIndex {
    key: Bytes,
    /// the main rev of the last modification
    modified: Revision,
    generations: Vec<Generation>,
}

#[derive(new, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct IndexGet {
    pub modified: Revision,
    pub created: Revision,
    pub version: u64,
}

impl KeyIndex {
    pub fn new(key: Bytes, main: u64, sub: u64) -> Self {
        let mut ki = Self {
            key,
            modified: Revision::default(),
            generations: vec![],
        };
        ki.put(main, sub);
        ki
    }

    pub fn put(&mut self, main: u64, sub: u64) {
        let rev = Revision::new(main, sub);
        assert!(
            rev > self.modified,
            "'put' with an unexpected smaller revision (given: {rev:?}, modified: {:?})",
            self.modified
        );
        if self.generations.is_empty() {
            self.generations.push(Generation::default());
        }
        // SAFETY: must nonempty
        let g = unsafe { self.generations.last_mut().unwrap_unchecked() };
        if g.revs.is_empty() {
            // create a new key
            g.created = rev;
        }
        g.revs.push(rev);
        g.version += 1;
        self.modified = rev;
    }

    /// `tombstone` puts a revision, pointing to a tombstone, to the keyIndex.
    ///
    /// It also creates a new empty generation in the keyIndex. It returns `Err(RevisionNotFound)`
    /// when tombstone on an empty generation.
    pub fn tombstone(&mut self, main: u64, sub: u64) -> Result<(), RevisionNotFound> {
        assert!(
            !self.is_empty(),
            "'tombstone' got an unexpected empty keyIndex (key: {:?})",
            self.key
        );
        // SAFETY: must nonempty
        let g = unsafe { self.generations.last_mut().unwrap_unchecked() };
        if g.is_empty() {
            Err(RevisionNotFound)
        } else {
            self.put(main, sub);
            self.generations.push(Generation::default());
            Ok(())
        }
    }

    pub fn get(&self, at_rev: u64) -> Result<IndexGet, RevisionNotFound> {
        assert!(
            !self.is_empty(),
            "'get' got an unexpected empty keyIndex (key: {:?})",
            self.key
        );

        let g = self.find_generation(at_rev).ok_or(RevisionNotFound)?;
        match g.walk(|rev| rev.main > at_rev) {
            None => Err(RevisionNotFound),
            Some(n) => Ok(IndexGet {
                modified: g.revs[n],
                created: g.created,
                version: g.version - ((g.revs.len() - n - 1) as u64),
            }),
        }
    }

    pub fn is_empty(&self) -> bool {
        debug_assert!(
            !self.generations.is_empty(),
            "KeyIndex is always created with the initial revision",
        );
        self.generations.len() == 1 && self.generations[0].is_empty()
    }

    fn find_generation(&self, rev: u64) -> Option<&Generation> {
        let lastg = (self.generations.len() - 1) as i64;
        let mut cg = lastg;
        while cg >= 0 {
            if self.generations[cg as usize].revs.is_empty() {
                cg -= 1;
                continue;
            }
            let g = &self.generations[cg as usize];
            if cg != lastg {
                let tomb = g.revs[g.revs.len() - 1].main;
                if tomb <= rev {
                    return None;
                }
            }
            if g.revs[0].main <= rev {
                return Some(g);
            }
            cg -= 1;
        }
        None
    }
}

/// `Generation` contains multiple revisions of a key.
#[derive(new, Debug, Default, Eq, PartialEq)]
pub struct Generation {
    version: u64,
    created: Revision,
    revs: Vec<Revision>,
}

impl Generation {
    pub fn is_empty(&self) -> bool {
        self.revs.is_empty()
    }

    /// `walk` walks through the revisions in the generation in descending order.
    /// It passes the revision to the given function.
    ///
    /// `walk` returns until:
    ///     1. it finishes walking all pairs
    ///     2. the function returns false.
    ///
    /// `walk` returns the position at where it stopped. If it stopped after finishing walking,
    /// `None` will be returned.
    pub fn walk<F>(&self, f: F) -> Option<usize>
    where
        F: Fn(Revision) -> bool,
    {
        let len = self.revs.len();
        for i in 0..len {
            let idx = len - i - 1;
            if !f(self.revs[idx]) {
                return Some(idx);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generation_walk() {
        let g = Generation::new(
            3,
            Revision::new0(2),
            vec![Revision::new0(2), Revision::new0(4), Revision::new0(6)],
        );

        struct C(fn(Revision) -> bool, Option<usize>);
        let cases = vec![
            C(|rev| rev.main >= 7, Some(2)),
            C(|rev| rev.main >= 6, Some(1)),
            C(|rev| rev.main >= 5, Some(1)),
            C(|rev| rev.main >= 4, Some(0)),
            C(|rev| rev.main >= 3, Some(0)),
            C(|rev| rev.main >= 2, None),
        ];

        for case in cases {
            assert_eq!(case.1, g.walk(case.0));
        }
    }

    #[test]
    fn test_find_generation() {
        let ki = new_test_key_index();

        let g0 = &ki.generations[0];
        let g1 = &ki.generations[1];

        assert_eq!(ki.find_generation(0), None);
        assert_eq!(ki.find_generation(1), None);
        assert_eq!(ki.find_generation(2), Some(g0));
        assert_eq!(ki.find_generation(3), Some(g0));
        assert_eq!(ki.find_generation(4), Some(g0));
        assert_eq!(ki.find_generation(5), Some(g0));
        assert_eq!(ki.find_generation(6), None);
        assert_eq!(ki.find_generation(7), None);
        assert_eq!(ki.find_generation(8), Some(g1));
        assert_eq!(ki.find_generation(9), Some(g1));
        assert_eq!(ki.find_generation(10), Some(g1));
        assert_eq!(ki.find_generation(11), Some(g1));
        assert_eq!(ki.find_generation(12), None);
        assert_eq!(ki.find_generation(13), None);
    }

    fn new_test_key_index() -> KeyIndex {
        // key: "foo"
        // modified: 16
        // generations:
        //    {empty}
        //    {{14, 0}[1], {14, 1}[2], {16, 0}(t)[3]}
        //    {{8, 0}[1], {10, 0}[2], {12, 0}(t)[3]}
        //    {{2, 0}[1], {4, 0}[2], {6, 0}(t)[3]}

        let mut ki = KeyIndex::new("foo".into(), 2, 0);
        ki.put(4, 0);
        ki.tombstone(6, 0).unwrap();
        ki.put(8, 0);
        ki.put(10, 0);
        ki.tombstone(12, 0).unwrap();
        ki.put(14, 0);
        ki.put(14, 1);
        ki.tombstone(16, 0).unwrap();
        ki
    }
}
