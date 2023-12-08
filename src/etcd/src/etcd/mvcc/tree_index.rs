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

use std::collections::{btree_map::Entry, BTreeMap, Bound};

use bytes::Bytes;

use crate::etcd::mvcc::{
    key_index::{IndexGet, KeyIndex, RevisionNotFound},
    revision::Revision,
};

#[derive(Debug, Default)]
pub struct TreeIndex {
    tree: BTreeMap<Bytes, KeyIndex>,
}

impl TreeIndex {
    pub fn put(&mut self, key: Bytes, rev: Revision) {
        match self.tree.entry(key.clone()) {
            Entry::Vacant(ent) => {
                ent.insert(KeyIndex::new(key, rev.main, rev.sub));
            }
            Entry::Occupied(ent) => {
                ent.into_mut().put(rev.main, rev.sub);
            }
        }
    }

    pub fn tombstone(&mut self, key: Bytes, rev: Revision) -> Result<(), RevisionNotFound> {
        let ki = self.tree.get_mut(&key).ok_or(RevisionNotFound)?;
        ki.tombstone(rev.main, rev.sub)
    }

    pub fn get(&self, key: Bytes, at_rev: u64) -> Result<IndexGet, RevisionNotFound> {
        self.unsafe_get(key, at_rev)
    }

    pub fn revisions(
        &self,
        key: Bytes,
        end: Option<Bytes>,
        at_rev: u64,
        limit: usize,
    ) -> (Vec<Revision>, u64) {
        let mut revs = vec![];
        let mut total = 0;
        match end {
            None => {
                if let Ok(IndexGet { modified: rev, .. }) = self.unsafe_get(key, at_rev) {
                    revs.push(rev);
                    total += 1;
                }
            }
            Some(end) => {
                self.unsafe_visit(key, end, |ki| {
                    if let Ok(IndexGet { modified: rev, .. }) = ki.get(at_rev) {
                        if limit == 0 || revs.len() < limit {
                            revs.push(rev);
                        }
                        total += 1;
                    }
                    true
                });
            }
        }
        (revs, total)
    }

    fn unsafe_get(&self, key: Bytes, at_rev: u64) -> Result<IndexGet, RevisionNotFound> {
        let ki = self.tree.get(&key).ok_or(RevisionNotFound)?;
        ki.get(at_rev)
    }

    fn unsafe_visit<F>(&self, key: Bytes, end: Bytes, mut f: F)
    where
        F: FnMut(&KeyIndex) -> bool,
    {
        let mut cursor = self.tree.lower_bound(Bound::Included(&key));
        while let Some((k, v)) = cursor.key_value() {
            if !end.is_empty() && k >= &end {
                break;
            }
            if !f(v) {
                break;
            }
            cursor.move_next();
        }
    }
}

#[cfg(test)]
mod tests {
    use derive_new::new;

    use super::*;

    #[test]
    fn test_tree_index_get() {
        let mut ti = TreeIndex::default();
        let key = Bytes::from_static("foo".as_ref());
        let created = Revision::main(2);
        let modified = Revision::main(4);
        let deleted = Revision::main(6);
        ti.put(key.clone(), created);
        ti.put(key.clone(), modified);
        ti.tombstone(key.clone(), deleted).unwrap();

        assert_eq!(Err(RevisionNotFound), ti.get(key.clone(), 0));
        assert_eq!(Err(RevisionNotFound), ti.get(key.clone(), 1));
        assert_eq!(
            Ok(IndexGet::new(created, created, 1)),
            ti.get(key.clone(), 2)
        );
        assert_eq!(
            Ok(IndexGet::new(created, created, 1)),
            ti.get(key.clone(), 3)
        );
        assert_eq!(
            Ok(IndexGet::new(modified, created, 2)),
            ti.get(key.clone(), 4)
        );
        assert_eq!(
            Ok(IndexGet::new(modified, created, 2)),
            ti.get(key.clone(), 5)
        );
        assert_eq!(Err(RevisionNotFound), ti.get(key.clone(), 6));
    }

    #[test]
    fn test_tree_index_tombstone() {
        let mut ti = TreeIndex::default();
        let key = Bytes::from_static("foo".as_ref());
        ti.put(key.clone(), Revision::main(1));
        ti.tombstone(key.clone(), Revision::main(2)).unwrap();
        assert_eq!(Err(RevisionNotFound), ti.get(key.clone(), 2));
        assert_eq!(
            Err(RevisionNotFound),
            ti.tombstone(key.clone(), Revision::main(3))
        );
    }

    #[test]
    fn test_tree_index_revisions() {
        let mut ti = TreeIndex::default();
        ti.put("foo".into(), Revision::main(1));
        ti.put("foo1".into(), Revision::main(2));
        ti.put("foo2".into(), Revision::main(3));
        ti.put("foo2".into(), Revision::main(4));
        ti.put("foo1".into(), Revision::main(5));
        ti.put("foo".into(), Revision::main(6));

        #[derive(new, Debug)]
        struct TestCase {
            key: &'static str,
            end: Option<&'static str>,
            rev: u64,
            limit: usize,
            total: u64,
            revs: Vec<u64>,
        }

        let cases = vec![
            // single key that not found
            TestCase::new("bar", None, 6, 0, 0, vec![]),
            // single key that found
            TestCase::new("foo", None, 6, 0, 1, vec![6]),
            // various range keys, fixed atRev, unlimited
            TestCase::new("foo", Some("foo1"), 6, 0, 1, vec![6]),
            TestCase::new("foo", Some("foo2"), 6, 0, 2, vec![6, 5]),
            TestCase::new("foo", Some("fop"), 6, 0, 3, vec![6, 5, 4]),
            TestCase::new("foo1", Some("fop"), 6, 0, 2, vec![5, 4]),
            TestCase::new("foo2", Some("fop"), 6, 0, 1, vec![4]),
            TestCase::new("foo3", Some("fop"), 6, 0, 0, vec![]),
            // fixed range keys, various atRev, unlimited
            TestCase::new("foo1", Some("fop"), 1, 0, 0, vec![]),
            TestCase::new("foo1", Some("fop"), 2, 1, 1, vec![2]),
            TestCase::new("foo1", Some("fop"), 3, 2, 2, vec![2, 3]),
            TestCase::new("foo1", Some("fop"), 4, 2, 2, vec![2, 4]),
            TestCase::new("foo1", Some("fop"), 5, 2, 2, vec![5, 4]),
            TestCase::new("foo1", Some("fop"), 6, 2, 2, vec![5, 4]),
            // fixed range keys, fixed atRev, various limit
            TestCase::new("foo", Some("fop"), 6, 1, 3, vec![6]),
            TestCase::new("foo", Some("fop"), 6, 2, 3, vec![6, 5]),
            TestCase::new("foo", Some("fop"), 6, 3, 3, vec![6, 5, 4]),
            TestCase::new("foo", Some("fop"), 3, 1, 3, vec![1]),
            TestCase::new("foo", Some("fop"), 3, 2, 3, vec![1, 2]),
            TestCase::new("foo", Some("fop"), 3, 3, 3, vec![1, 2, 3]),
        ];

        for case in cases {
            let dbg = format!("{case:?}");
            let TestCase {
                key,
                end,
                rev,
                limit,
                total,
                revs,
            } = case;
            let (actual_revs, actual_total) =
                ti.revisions(key.into(), end.map(Into::into), rev, limit);
            assert_eq!(total, actual_total, "{dbg}");
            assert_eq!(
                revs.into_iter().map(Revision::main).collect::<Vec<_>>(),
                actual_revs,
                "{dbg}"
            )
        }
    }
}
