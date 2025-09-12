use std::{cmp, collections::HashMap, io, ops::RangeInclusive};

use bytes::BytesMut;
use opentelemetry::propagation::{Extractor, Injector};
use rangemap::RangeInclusiveSet;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use tokio_util::codec::{Decoder, LengthDelimitedCodec};
use tracing::warn;

use crate::{
    actor::ActorId,
    agent::{Booked, Bookie},
    base::{CrsqlDbVersion, CrsqlDbVersionRange, CrsqlSeq, CrsqlSeqRange},
    broadcast::{ChangeV1, Timestamp},
};

#[derive(Debug, Clone, PartialEq, Readable, Writable)]
pub enum SyncMessage {
    V1(SyncMessageV1),
}

#[derive(Debug, Clone, PartialEq, Readable, Writable)]
pub enum SyncMessageV1 {
    State(SyncStateV1),
    Changeset(ChangeV1),
    Clock(Timestamp),
    Rejection(SyncRejectionV1),
    Request(SyncRequestV1),
}

#[derive(Debug, Default, Clone, PartialEq, Readable, Writable)]
pub struct SyncTraceContextV1 {
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

impl Injector for SyncTraceContextV1 {
    fn set(&mut self, key: &str, value: String) {
        match key {
            "traceparent" if !value.is_empty() => self.traceparent = Some(value),
            "tracestate" if !value.is_empty() => self.tracestate = Some(value),
            _ => {}
        }
    }
}

impl Extractor for SyncTraceContextV1 {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            "traceparent" => self.traceparent.as_deref(),
            "tracestate" => self.tracestate.as_deref(),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        let mut v = Vec::with_capacity(2);
        if self.traceparent.is_some() {
            v.push("traceparent");
        }
        if self.tracestate.is_some() {
            v.push("tracestate");
        }
        v
    }
}

pub type SyncRequestV1 = Vec<(ActorId, Vec<SyncNeedV1>)>;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Readable, Writable)]
pub enum SyncRejectionV1 {
    #[error("max concurrency reached")]
    MaxConcurrencyReached,
    #[error("different cluster")]
    DifferentCluster,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncStateV1 {
    pub actor_id: ActorId,
    pub heads: HashMap<ActorId, CrsqlDbVersion>,
    pub need: HashMap<ActorId, Vec<RangeInclusive<CrsqlDbVersion>>>,
    pub partial_need: HashMap<ActorId, HashMap<CrsqlDbVersion, Vec<RangeInclusive<CrsqlSeq>>>>,
    pub last_cleared_ts: Option<Timestamp>,
}

impl SyncStateV1 {
    pub fn need_len(&self) -> u64 {
        self.need
            .values()
            .flat_map(|v| v.iter().map(|range| (range.end().0 - range.start().0) + 1))
            .sum::<u64>()
            + (
                self.partial_need
                    .values()
                    .flat_map(|partials| {
                        partials.values().flat_map(|ranges| {
                            ranges
                                .iter()
                                .map(|range| (range.end().0 - range.start().0) + 1)
                        })
                    })
                    .sum::<u64>()
                    / 50
                // this is how many chunks we're looking at, kind of random...
            )
    }

    pub fn need_len_for_actor(&self, actor_id: &ActorId) -> u64 {
        self.need
            .get(actor_id)
            .map(|v| {
                v.iter()
                    .map(|range| (range.end().0 - range.start().0) + 1)
                    .sum()
            })
            .unwrap_or(0)
            + self
                .partial_need
                .get(actor_id)
                .map(|partials| partials.len() as u64)
                .unwrap_or(0)
    }

    pub fn compute_available_needs(
        &self,
        other: &SyncStateV1,
    ) -> HashMap<ActorId, Vec<SyncNeedV1>> {
        let mut needs: HashMap<ActorId, Vec<SyncNeedV1>> = HashMap::new();

        for (actor_id, head) in other.heads.iter() {
            if *actor_id == self.actor_id {
                continue;
            }
            if *head == CrsqlDbVersion(0) {
                warn!(actor_id = %other.actor_id, "sent a 0 head version for actor id {}", actor_id);
                continue;
            }
            let other_haves = {
                let mut haves =
                    RangeInclusiveSet::from_iter([(CrsqlDbVersion(1)..=*head)].into_iter());

                // remove needs
                if let Some(other_need) = other.need.get(actor_id) {
                    for need in other_need.iter() {
                        // create gaps
                        haves.remove(need.clone());
                    }
                }

                // remove partials
                if let Some(other_partials) = other.partial_need.get(actor_id) {
                    for (v, _) in other_partials.iter() {
                        haves.remove(*v..=*v);
                    }
                }

                // we are left with all the versions they fully have!
                haves
            };

            if let Some(our_need) = self.need.get(actor_id) {
                for range in our_need.iter() {
                    for overlap in other_haves.overlapping(range) {
                        let start = cmp::max(range.start(), overlap.start());
                        let end = cmp::min(range.end(), overlap.end());
                        needs.entry(*actor_id).or_default().push(SyncNeedV1::Full {
                            versions: CrsqlDbVersionRange::new(*start, *end),
                        })
                    }
                }
            }

            if let Some(our_partials) = self.partial_need.get(actor_id) {
                for (v, seqs) in our_partials.iter() {
                    if other_haves.contains(v) {
                        needs
                            .entry(*actor_id)
                            .or_default()
                            .push(SyncNeedV1::Partial {
                                version: *v,
                                seqs: seqs.iter().map(CrsqlSeqRange::from).collect(),
                            });
                    } else if let Some(other_seqs) = other
                        .partial_need
                        .get(actor_id)
                        .and_then(|versions| versions.get(v))
                    {
                        let max_other_seq = other_seqs.iter().map(|range| *range.end()).max();
                        let max_our_seq = seqs.iter().map(|range| *range.end()).max();

                        let end_seq = cmp::max(max_other_seq, max_our_seq);

                        if let Some(end) = end_seq {
                            let mut other_seqs_haves =
                                RangeInclusiveSet::from_iter([CrsqlSeq(0)..=end]);

                            for seqs in other_seqs.iter() {
                                other_seqs_haves.remove(seqs.clone());
                            }

                            let seqs = seqs
                                .iter()
                                .flat_map(|range| {
                                    other_seqs_haves.overlapping(range).map(|overlap| {
                                        let start = cmp::max(range.start(), overlap.start());
                                        let end = cmp::min(range.end(), overlap.end());
                                        CrsqlSeqRange::new(*start, *end)
                                    })
                                })
                                .collect::<Vec<CrsqlSeqRange>>();

                            if !seqs.is_empty() {
                                needs
                                    .entry(*actor_id)
                                    .or_default()
                                    .push(SyncNeedV1::Partial { version: *v, seqs });
                            }
                        }
                    }
                }
            }

            let missing = match self.heads.get(actor_id) {
                Some(our_head) => {
                    if head > our_head {
                        Some((*our_head + 1)..=*head)
                    } else {
                        None
                    }
                }
                None => Some(CrsqlDbVersion(1)..=*head),
            };

            if let Some(missing) = missing {
                needs.entry(*actor_id).or_default().push(SyncNeedV1::Full {
                    versions: missing.into(),
                });
            }
        }

        needs
    }
}

impl<'a, C> Readable<'a, C> for SyncStateV1
where
    C: speedy::Context,
{
    fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        let actor_id = ActorId::read_from(reader)?;
        let heads = HashMap::<ActorId, CrsqlDbVersion>::read_from(reader)?;

        // Read need: HashMap<ActorId, Vec<RangeInclusive<CrsqlDbVersion>>>
        let need_len = usize::read_from(reader)?;
        let mut need = HashMap::with_capacity(need_len);
        for _ in 0..need_len {
            let actor_id = ActorId::read_from(reader)?;
            let ranges_len = usize::read_from(reader)?;
            let mut ranges = Vec::with_capacity(ranges_len);
            for _ in 0..ranges_len {
                let start = CrsqlDbVersion::read_from(reader)?;
                let end = CrsqlDbVersion::read_from(reader)?;
                ranges.push(start..=end);
            }
            need.insert(actor_id, ranges);
        }

        // Read partial_need: HashMap<ActorId, HashMap<CrsqlDbVersion, Vec<RangeInclusive<CrsqlSeq>>>>
        let partial_need_len = usize::read_from(reader)?;
        let mut partial_need = HashMap::with_capacity(partial_need_len);
        for _ in 0..partial_need_len {
            let actor_id = ActorId::read_from(reader)?;
            let versions_len = usize::read_from(reader)?;
            let mut versions_map = HashMap::with_capacity(versions_len);
            for _ in 0..versions_len {
                let version = CrsqlDbVersion::read_from(reader)?;
                let seq_ranges_len = usize::read_from(reader)?;
                let mut seq_ranges = Vec::with_capacity(seq_ranges_len);
                for _ in 0..seq_ranges_len {
                    let start = CrsqlSeq::read_from(reader)?;
                    let end = CrsqlSeq::read_from(reader)?;
                    seq_ranges.push(start..=end);
                }
                versions_map.insert(version, seq_ranges);
            }
            partial_need.insert(actor_id, versions_map);
        }

        let last_cleared_ts = Option::<Timestamp>::read_from(reader)?;

        Ok(SyncStateV1 {
            actor_id,
            heads,
            need,
            partial_need,
            last_cleared_ts,
        })
    }
}

impl<C> Writable<C> for SyncStateV1
where
    C: speedy::Context,
{
    fn write_to<T: ?Sized + speedy::Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
        self.actor_id.write_to(writer)?;
        self.heads.write_to(writer)?;

        // Write need: HashMap<ActorId, Vec<RangeInclusive<CrsqlDbVersion>>>
        self.need.len().write_to(writer)?;
        for (actor_id, ranges) in &self.need {
            actor_id.write_to(writer)?;
            ranges.len().write_to(writer)?;
            for range in ranges {
                range.start().write_to(writer)?;
                range.end().write_to(writer)?;
            }
        }

        // Write partial_need: HashMap<ActorId, HashMap<CrsqlDbVersion, Vec<RangeInclusive<CrsqlSeq>>>>
        self.partial_need.len().write_to(writer)?;
        for (actor_id, versions_map) in &self.partial_need {
            actor_id.write_to(writer)?;
            versions_map.len().write_to(writer)?;
            for (version, seq_ranges) in versions_map {
                version.write_to(writer)?;
                seq_ranges.len().write_to(writer)?;
                for range in seq_ranges {
                    range.start().write_to(writer)?;
                    range.end().write_to(writer)?;
                }
            }
        }

        self.last_cleared_ts.write_to(writer)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SyncNeedV1 {
    Full {
        versions: CrsqlDbVersionRange,
    },
    Partial {
        version: CrsqlDbVersion,
        seqs: Vec<CrsqlSeqRange>,
    },
    Empty {
        ts: Option<Timestamp>,
    },
}

impl SyncNeedV1 {
    #[inline]
    pub fn count(&self) -> usize {
        match self {
            SyncNeedV1::Full { versions } => versions.len(),
            SyncNeedV1::Partial { .. } => 1,
            SyncNeedV1::Empty { .. } => 1,
        }
    }
}

impl<'a, C> Readable<'a, C> for SyncNeedV1
where
    C: speedy::Context,
{
    fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        let variant_tag = u8::read_from(reader)?;
        match variant_tag {
            0 => {
                let start = CrsqlDbVersion::read_from(reader)?;
                let end = CrsqlDbVersion::read_from(reader)?;
                let versions = (start..=end).into();
                Ok(SyncNeedV1::Full { versions })
            }
            1 => {
                let version = CrsqlDbVersion::read_from(reader)?;
                let seqs_len = usize::read_from(reader)?;
                let mut seqs = Vec::with_capacity(seqs_len);
                for _ in 0..seqs_len {
                    let start = CrsqlSeq::read_from(reader)?;
                    let end = CrsqlSeq::read_from(reader)?;
                    seqs.push((start..=end).into());
                }
                Ok(SyncNeedV1::Partial { version, seqs })
            }
            2 => {
                let ts = Option::<Timestamp>::read_from(reader)?;
                Ok(SyncNeedV1::Empty { ts })
            }
            _ => {
                // Read and discard the invalid tag to avoid issues, then create a proper error
                let _ = reader.read_u8()?;
                // This is a bit of a hack but should work for speedy contexts
                panic!("Invalid SyncNeedV1 variant tag: {}", variant_tag);
            }
        }
    }
}

impl<C> Writable<C> for SyncNeedV1
where
    C: speedy::Context,
{
    fn write_to<T: ?Sized + speedy::Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
        match self {
            SyncNeedV1::Full { versions } => {
                0u8.write_to(writer)?;
                versions.start().write_to(writer)?;
                versions.end().write_to(writer)?;
            }
            SyncNeedV1::Partial { version, seqs } => {
                1u8.write_to(writer)?;
                version.write_to(writer)?;
                seqs.len().write_to(writer)?;
                for range in seqs {
                    range.start().write_to(writer)?;
                    range.end().write_to(writer)?;
                }
            }
            SyncNeedV1::Empty { ts } => {
                2u8.write_to(writer)?;
                ts.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl From<SyncStateV1> for SyncMessage {
    fn from(value: SyncStateV1) -> Self {
        SyncMessage::V1(SyncMessageV1::State(value))
    }
}

// generates a `SyncMessage` to tell another node what versions we're missing
#[tracing::instrument(skip_all, level = "debug")]
pub async fn generate_sync(bookie: &Bookie, self_actor_id: ActorId) -> SyncStateV1 {
    let mut state = SyncStateV1 {
        actor_id: self_actor_id,
        ..Default::default()
    };

    let actors: Vec<(ActorId, Booked)> = {
        bookie
            .read::<&str, _>("generate_sync", None)
            .await
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    };

    for (actor_id, booked) in actors {
        let bookedr = booked.read("generate_sync", actor_id.as_simple()).await;

        let last_version = match bookedr.last() {
            None => continue,
            Some(v) => v,
        };

        let need: Vec<_> = bookedr.needed().iter().cloned().collect();

        if !need.is_empty() {
            state.need.insert(actor_id, need);
        }

        {
            for (v, partial) in bookedr
                .partials
                .iter()
                // don't set partial if it is effectively complete
                .filter(|(_, partial)| !partial.is_complete())
            {
                state.partial_need.entry(actor_id).or_default().insert(
                    *v,
                    partial
                        .seqs
                        .gaps(&(CrsqlSeq(0)..=partial.last_seq))
                        .collect(),
                );
            }
        }
        state.heads.insert(actor_id, last_version);
    }

    state
}

#[derive(Debug, thiserror::Error)]
pub enum SyncMessageEncodeError {
    #[error(transparent)]
    Encode(#[from] speedy::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum SyncMessageDecodeError {
    #[error(transparent)]
    Decode(#[from] speedy::Error),
    #[error("corrupted message, crc mismatch (got: {0}, expected {1})")]
    Corrupted(u32, u32),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl SyncMessage {
    pub fn state(&self) -> Option<&SyncStateV1> {
        match self {
            SyncMessage::V1(SyncMessageV1::State(state)) => Some(state),
            _ => None,
        }
    }

    pub fn from_slice<S: AsRef<[u8]>>(slice: S) -> Result<Self, speedy::Error> {
        Self::read_from_buffer(slice.as_ref())
    }

    pub fn from_buf(buf: &mut BytesMut) -> Result<Self, SyncMessageDecodeError> {
        Ok(Self::from_slice(buf)?)
    }

    pub fn decode(
        codec: &mut LengthDelimitedCodec,
        buf: &mut BytesMut,
    ) -> Result<Option<Self>, SyncMessageDecodeError> {
        Ok(match codec.decode(buf)? {
            Some(mut buf) => Some(Self::from_buf(&mut buf)?),
            None => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{dbsr, dbsri, dbvr, dbvri};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_compute_available_needs() {
        let actor1 = ActorId(Uuid::new_v4());

        let mut our_state = SyncStateV1::default();
        our_state.heads.insert(actor1, CrsqlDbVersion(10));

        let mut other_state = SyncStateV1::default();
        other_state.heads.insert(actor1, CrsqlDbVersion(13));

        assert_eq!(
            our_state.compute_available_needs(&other_state),
            [(
                actor1,
                vec![SyncNeedV1::Full {
                    versions: dbvr!(11, 13)
                }]
            )]
            .into()
        );

        our_state.need.entry(actor1).or_default().push(dbvri!(2, 5));
        our_state.need.entry(actor1).or_default().push(dbvri!(7, 7));

        assert_eq!(
            our_state.compute_available_needs(&other_state),
            [(
                actor1,
                vec![
                    SyncNeedV1::Full {
                        versions: dbvr!(2, 5)
                    },
                    SyncNeedV1::Full {
                        versions: dbvr!(7, 7)
                    },
                    SyncNeedV1::Full {
                        versions: dbvr!(11, 13)
                    }
                ]
            )]
            .into()
        );

        our_state.partial_need.insert(
            actor1,
            [(CrsqlDbVersion(9), vec![dbsri!(100, 120), dbsri!(130, 132)])].into(),
        );

        assert_eq!(
            our_state.compute_available_needs(&other_state),
            [(
                actor1,
                vec![
                    SyncNeedV1::Full {
                        versions: dbvr!(2, 5)
                    },
                    SyncNeedV1::Full {
                        versions: dbvr!(7, 7)
                    },
                    SyncNeedV1::Partial {
                        version: CrsqlDbVersion(9),
                        seqs: vec![dbsr!(100, 120), dbsr!(130, 132)]
                    },
                    SyncNeedV1::Full {
                        versions: dbvr!(11, 13)
                    }
                ]
            )]
            .into()
        );

        other_state.partial_need.insert(
            actor1,
            [(CrsqlDbVersion(9), vec![dbsri!(100, 110), dbsri!(130, 130)])].into(),
        );

        assert_eq!(
            our_state.compute_available_needs(&other_state),
            [(
                actor1,
                vec![
                    SyncNeedV1::Full {
                        versions: dbvr!(2, 5)
                    },
                    SyncNeedV1::Full {
                        versions: dbvr!(7, 7)
                    },
                    SyncNeedV1::Partial {
                        version: CrsqlDbVersion(9),
                        seqs: vec![dbsr!(111, 120), dbsr!(131, 132)]
                    },
                    SyncNeedV1::Full {
                        versions: dbvr!(11, 13)
                    }
                ]
            )]
            .into()
        );
    }

    #[test]
    fn test_sync_state_v1_serialization() {
        use speedy::{Readable, Writable};
        use std::collections::HashMap;

        let actor1 = ActorId(Uuid::new_v4());
        let actor2 = ActorId(Uuid::new_v4());

        let mut sync_state = SyncStateV1 {
            actor_id: actor1,
            heads: HashMap::new(),
            need: HashMap::new(),
            partial_need: HashMap::new(),
            last_cleared_ts: Some(Timestamp::from(12345u64)),
        };

        // Add some heads
        sync_state.heads.insert(actor1, CrsqlDbVersion(10));
        sync_state.heads.insert(actor2, CrsqlDbVersion(20));

        // Add some needs
        sync_state.need.insert(
            actor1,
            vec![
                CrsqlDbVersion(1)..=CrsqlDbVersion(5),
                CrsqlDbVersion(8)..=CrsqlDbVersion(9),
            ],
        );

        // Add some partial needs
        let mut partial_map = HashMap::new();
        partial_map.insert(
            CrsqlDbVersion(15),
            vec![CrsqlSeq(100)..=CrsqlSeq(110), CrsqlSeq(120)..=CrsqlSeq(130)],
        );
        sync_state.partial_need.insert(actor2, partial_map);

        // Serialize
        let bytes = sync_state.write_to_vec().unwrap();

        // Deserialize
        let deserialized = SyncStateV1::read_from_buffer(&bytes).unwrap();

        assert_eq!(sync_state, deserialized);
    }

    #[test]
    fn test_sync_state_v1_empty_serialization() {
        use speedy::{Readable, Writable};

        let sync_state = SyncStateV1 {
            actor_id: ActorId(Uuid::new_v4()),
            heads: HashMap::new(),
            need: HashMap::new(),
            partial_need: HashMap::new(),
            last_cleared_ts: None,
        };

        let bytes = sync_state.write_to_vec().unwrap();
        let deserialized = SyncStateV1::read_from_buffer(&bytes).unwrap();

        assert_eq!(sync_state, deserialized);
    }

    #[test]
    fn test_sync_need_v1_full_serialization() {
        use speedy::{Readable, Writable};

        let sync_need = SyncNeedV1::Full {
            versions: (CrsqlDbVersion(5)..=CrsqlDbVersion(10)).into(),
        };

        let bytes = sync_need.write_to_vec().unwrap();
        let deserialized = SyncNeedV1::read_from_buffer(&bytes).unwrap();

        assert_eq!(sync_need, deserialized);
    }

    #[test]
    fn test_sync_need_v1_partial_serialization() {
        use speedy::{Readable, Writable};

        let sync_need = SyncNeedV1::Partial {
            version: CrsqlDbVersion(42),
            seqs: vec![
                (CrsqlSeq(0)..=CrsqlSeq(10)).into(),
                (CrsqlSeq(20)..=CrsqlSeq(30)).into(),
                (CrsqlSeq(50)..=CrsqlSeq(50)).into(),
            ],
        };

        let bytes = sync_need.write_to_vec().unwrap();
        let deserialized = SyncNeedV1::read_from_buffer(&bytes).unwrap();

        assert_eq!(sync_need, deserialized);
    }

    #[test]
    fn test_sync_need_v1_empty_serialization() {
        use speedy::{Readable, Writable};

        let sync_need = SyncNeedV1::Empty {
            ts: Some(Timestamp::from(67890u64)),
        };

        let bytes = sync_need.write_to_vec().unwrap();
        let deserialized = SyncNeedV1::read_from_buffer(&bytes).unwrap();

        assert_eq!(sync_need, deserialized);
    }

    #[test]
    fn test_sync_need_v1_empty_with_none_ts() {
        use speedy::{Readable, Writable};

        let sync_need = SyncNeedV1::Empty { ts: None };

        let bytes = sync_need.write_to_vec().unwrap();
        let deserialized = SyncNeedV1::read_from_buffer(&bytes).unwrap();

        assert_eq!(sync_need, deserialized);
    }

    #[test]
    fn test_sync_need_v1_roundtrip_all_variants() {
        use speedy::{Readable, Writable};

        let test_cases = vec![
            SyncNeedV1::Full {
                versions: (CrsqlDbVersion(1)..=CrsqlDbVersion(1)).into(),
            },
            SyncNeedV1::Partial {
                version: CrsqlDbVersion(10),
                seqs: vec![],
            },
            SyncNeedV1::Partial {
                version: CrsqlDbVersion(20),
                seqs: vec![(CrsqlSeq(1)..=CrsqlSeq(5)).into()],
            },
            SyncNeedV1::Empty { ts: None },
            SyncNeedV1::Empty {
                ts: Some(Timestamp::zero()),
            },
        ];

        for (i, original) in test_cases.into_iter().enumerate() {
            let bytes = original.write_to_vec().unwrap();
            let deserialized = SyncNeedV1::read_from_buffer(&bytes)
                .unwrap_or_else(|e| panic!("Failed to deserialize test case {}: {}", i, e));

            assert_eq!(original, deserialized, "Test case {} failed", i);
        }
    }
}
