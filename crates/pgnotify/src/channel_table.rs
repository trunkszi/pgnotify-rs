//! Fixed-capacity, cache-aligned byte-trie channel registry.
use crate::{
    error::{PgNotifyError, Result},
    notify_ref::prefix_word,
    types::{ChannelId, FixedString},
};

use uarch::prelude::{cache_line, cold_sink, hot_inline, register_pass};

const NONE: u32 = u32::MAX;
#[cache_line]
struct Node {
    children: [u32; 16],
}
uarch::layout::assert_layout!(Node, size = 64, align = 64, fields = { children: 0 });
impl Node {
    fn new() -> Self {
        Self {
            children: [NONE; 16],
        }
    }
}

uarch::bitword! {
    struct HintMeta(u64) {
        [0..=15] ID: u16,
        [16..=23] LEN: u8,
        [24] OCCUPIED,
        [32..=63] COUNT: u32,
    }
}

#[register_pass]
#[derive(Clone, Copy, Default)]
struct LookupHint {
    prefix: u64,
    meta: HintMeta,
}
uarch::layout::assert_layout!(LookupHint, size = 16, align = 8, fields = { prefix: 0, meta: 8 });

/// A fixed fingerprint index accelerates hits and unambiguous misses.
/// Collisions fall back to at most 126 trie edges, independent of channel count.
/// Registration allocates only on the subscription control path. IDs are stable until removal.
pub struct ChannelTable {
    nodes: Vec<Node>,
    // Separate terminal IDs keep all sixteen full-width edges in one cache line.
    terminals: Vec<Option<ChannelId>>,
    channels: Vec<Option<FixedString<64>>>,
    free: Vec<ChannelId>,
    len: usize,
    vacant_nodes: Vec<usize>,
    hints: Vec<LookupHint>,
}
impl Default for ChannelTable {
    fn default() -> Self {
        Self::new()
    }
}
impl ChannelTable {
    pub const DEFAULT_CAPACITY: usize = 256;
    pub fn new() -> Self {
        Self::allocate(Self::DEFAULT_CAPACITY)
    }
    /// # Errors
    /// Rejects capacities outside 1..=65536.
    pub fn with_capacity(capacity: usize) -> Result<Self> {
        if !(1..=65536).contains(&capacity) {
            return Err(PgNotifyError::InvalidConfig {
                reason: "channel_capacity",
            });
        }
        Ok(Self::allocate(capacity))
    }
    #[cold_sink]
    fn allocate(capacity: usize) -> Self {
        Self {
            nodes: vec![Node::new()],
            terminals: vec![None],
            channels: vec![None; capacity],
            free: (0..capacity).rev().map(|id| ChannelId(id as u16)).collect(),
            len: 0,
            vacant_nodes: Vec::new(),
            hints: vec![LookupHint::default(); capacity.next_power_of_two() * 2],
        }
    }
    /// # Errors
    /// Rejects invalid names or a full registry.
    pub fn subscribe(&mut self, channel: &str) -> Result<ChannelId> {
        self.register(FixedString::try_from_str(channel)?)
    }
    /// # Errors
    /// Rejects invalid names or a full registry.
    pub fn register(&mut self, channel: FixedString<64>) -> Result<ChannelId> {
        crate::types::validate_channel_name(channel.as_str())?;
        if let Some(id) = self.lookup(channel.as_str()) {
            return Ok(id);
        }
        let id = self.free.pop().ok_or(PgNotifyError::ChannelTableFull {
            capacity: self.channels.len(),
        })?;
        self.insert_at(channel, id);
        Ok(id)
    }
    pub(crate) fn register_at(&mut self, channel: FixedString<64>, id: ChannelId) -> Result<()> {
        crate::types::validate_channel_name(channel.as_str())?;
        let registered = self.lookup(channel.as_str());
        if registered == Some(id) {
            return Ok(());
        }
        if registered.is_some() || self.channel(id).is_some() {
            return Err(PgNotifyError::InvalidConfig {
                reason: "channel id mismatch",
            });
        }
        let index = self
            .free
            .iter()
            .position(|candidate| *candidate == id)
            .ok_or(PgNotifyError::UnknownChannelId(id))?;
        self.free.swap_remove(index);
        self.insert_at(channel, id);
        Ok(())
    }
    fn insert_at(&mut self, channel: FixedString<64>, id: ChannelId) {
        let mut at = 0;
        for digit in digits(channel.as_bytes()) {
            let next = self.nodes[at].children[digit];
            at = if next == NONE {
                let index = if let Some(index) = self.vacant_nodes.pop() {
                    index
                } else {
                    let index = self.nodes.len();
                    self.nodes.push(Node::new());
                    self.terminals.push(None);
                    index
                };
                self.nodes[at].children[digit] = index as u32;
                index
            } else {
                next as usize
            };
        }
        self.terminals[at] = Some(id);
        let prefix = prefix_word(channel.as_bytes());
        let index = self.hint_index(channel.as_bytes(), prefix);
        let hint = &mut self.hints[index];
        hint.meta = hint.meta.wrapping_add_top_field(HintMeta::COUNT, 1);
        if !hint.meta.test(HintMeta::OCCUPIED) {
            hint.prefix = prefix;
            hint.meta = hint.meta.patch(
                uarch::bits::BitPatch::EMPTY
                    .with_field(HintMeta::ID, u64::from(id.0))
                    .with_field(HintMeta::LEN, (channel.len() as u64) & 0xff)
                    .with_flag(HintMeta::OCCUPIED, true),
            );
        }
        self.channels[usize::from(id.0)] = Some(channel);
        self.len += 1;
    }
    #[inline]
    pub fn lookup(&self, name: &str) -> Option<ChannelId> {
        let bytes = name.as_bytes();
        if bytes.is_empty() || bytes.len() > 63 {
            return None;
        }
        let prefix = prefix_word(bytes);
        let hint = self.hints[self.hint_index(bytes, prefix)];
        if hint.meta.get(HintMeta::COUNT) == 0 {
            return None;
        }
        if uarch::ilp_all!(
            hint.meta.test(HintMeta::OCCUPIED),
            usize::from(hint.meta.get(HintMeta::LEN)) == bytes.len(),
            hint.prefix == prefix
        ) {
            let id = ChannelId(hint.meta.get(HintMeta::ID));
            if bytes.len() <= 8 {
                return Some(id);
            }
            if bytes.len() > 16 {
                return self.lookup_cached(name, id, hint.meta.get(HintMeta::COUNT));
            }
            if self.channel(id).is_some_and(|channel| {
                channel.len() == bytes.len()
                    && channel.as_bytes().last_chunk::<8>() == bytes.last_chunk::<8>()
            }) {
                return Some(id);
            }
        }
        if hint
            .meta
            .matches(uarch::pattern!(HintMeta { OCCUPIED & COUNT == 1 }))
        {
            return None;
        }
        self.lookup_trie(name)
    }
    #[inline(never)]
    fn lookup_cached(&self, name: &str, id: ChannelId, count: u32) -> Option<ChannelId> {
        if self
            .channel(id)
            .is_some_and(|channel| names_equal(channel.as_bytes(), name.as_bytes()))
        {
            Some(id)
        } else if count == 1 {
            None
        } else {
            self.lookup_trie(name)
        }
    }
    #[inline(never)]
    fn lookup_trie(&self, name: &str) -> Option<ChannelId> {
        self.find_node(name).and_then(|node| self.terminals[node])
    }
    #[inline]
    fn hint_index(&self, bytes: &[u8], prefix: u64) -> usize {
        let suffix = if bytes.len() >= 8 {
            prefix_word(&bytes[bytes.len() - 8..])
        } else {
            prefix
        };
        // Diffuse every byte before masking; shared prefixes and decimal suffixes otherwise cluster.
        let mut mixed = prefix ^ suffix.rotate_left(17) ^ bytes.len() as u64;
        mixed ^= mixed >> 30;
        mixed = mixed.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed ^= mixed >> 27;
        mixed = mixed.wrapping_mul(0x94d0_49bb_1331_11eb);
        (mixed ^ (mixed >> 31)) as usize & (self.hints.len() - 1)
    }
    fn find_node(&self, name: &str) -> Option<usize> {
        let mut at = 0;
        for digit in digits(name.as_bytes()) {
            let next = self.nodes[at].children[digit];
            if next == NONE {
                return None;
            }
            uarch::mem::prefetch_read::<uarch::mem::L1, _>(
                self.nodes.as_ptr().wrapping_add(next as usize),
            );
            at = next as usize;
        }
        Some(at)
    }
    #[hot_inline]
    pub fn channel(&self, id: ChannelId) -> Option<&FixedString<64>> {
        self.channels
            .get(usize::from(id.0))
            .and_then(Option::as_ref)
    }
    /// # Errors
    /// Rejects IDs not currently registered.
    pub fn remove(&mut self, id: ChannelId) -> Result<()> {
        let channel = *self
            .channel(id)
            .ok_or(PgNotifyError::UnknownChannelId(id))?;
        let mut path = [0_usize; 127];
        let mut edges = [0_usize; 126];
        let mut depth = 0;
        for digit in digits(channel.as_bytes()) {
            edges[depth] = digit;
            path[depth + 1] = self.nodes[path[depth]].children[digit] as usize;
            depth += 1;
        }
        self.terminals[path[depth]] = None;
        while depth > 0 {
            let node = path[depth];
            if self.terminals[node].is_some()
                || self.nodes[node].children.iter().any(|child| *child != NONE)
            {
                break;
            }
            self.nodes[path[depth - 1]].children[edges[depth - 1]] = NONE;
            self.vacant_nodes.push(node);
            depth -= 1;
        }
        let index = self.hint_index(channel.as_bytes(), prefix_word(channel.as_bytes()));
        let hint = &mut self.hints[index];
        hint.meta = hint
            .meta
            .wrapping_add_top_field(HintMeta::COUNT, u64::from(u32::MAX));
        if hint.meta.get(HintMeta::LEN) != 0 && hint.meta.get(HintMeta::ID) == id.0 {
            hint.meta = hint.meta.patch(uarch::patch!(HintMeta {
                LEN: 0,
                OCCUPIED: false
            }));
        }
        self.channels[usize::from(id.0)] = None;
        self.free.push(id);
        self.len -= 1;
        Ok(())
    }
    pub const fn len(&self) -> usize {
        self.len
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = (ChannelId, &FixedString<64>)> {
        self.channels
            .iter()
            .enumerate()
            .filter_map(|(id, name)| name.as_ref().map(|name| (ChannelId(id as u16), name)))
    }
}

fn digits(bytes: &[u8]) -> impl Iterator<Item = usize> + '_ {
    bytes
        .iter()
        .flat_map(|byte| [usize::from(byte >> 4), usize::from(byte & 15)])
}

#[inline]
fn names_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() || left.len() > 63 {
        return false;
    }
    let (left_words, left_tail) = left.as_chunks::<8>();
    let (right_words, right_tail) = right.as_chunks::<8>();
    let indices = &[0_usize, 1, 2, 3, 4, 5, 6][..left_words.len()];
    let different = uarch::reduce_four_way!(
        indices,
        0_u64,
        0_u64,
        |acc, &index| acc
            | (u64::from_ne_bytes(left_words[index]) ^ u64::from_ne_bytes(right_words[index])),
        |a, b| a | b
    );
    let mut tail_difference = 0_u8;
    for (&left, &right) in left_tail.iter().zip(right_tail) {
        tail_difference |= left ^ right;
    }
    uarch::ilp_all!(different == 0, tail_difference == 0)
}

#[cfg(test)]
mod layout_tests {
    use super::*;

    #[test]
    fn word_comparison_checks_every_byte_and_tail() {
        for len in 0..=63 {
            let left: Vec<u8> = (0..len).map(|index| index as u8).collect();
            assert!(names_equal(&left, &left));
            for changed in 0..len {
                let mut right = left.clone();
                right[changed] ^= 0x80;
                assert!(!names_equal(&left, &right), "length {len}, byte {changed}");
            }
            let mut extended = left.clone();
            extended.push(0);
            assert!(!names_equal(&left, &extended));
        }
    }

    #[test]
    fn hint_counter_wrap_preserves_maximum_identity_and_length() {
        let mut meta = HintMeta::new(0).patch(
            uarch::bits::BitPatch::EMPTY
                .with_field(HintMeta::ID, u64::from(u16::MAX))
                .with_field(HintMeta::LEN, 63)
                .with_field(HintMeta::COUNT, u64::from(u32::MAX))
                .with_flag(HintMeta::OCCUPIED, true),
        );
        assert_eq!(meta.raw(), 0xffff_ffff_013f_ffff);
        meta = meta.wrapping_add_top_field(HintMeta::COUNT, 1);
        assert_eq!(meta.raw(), 0x013f_ffff);
        meta = meta.wrapping_add_top_field(HintMeta::COUNT, 65536);
        assert_eq!(meta.get(HintMeta::COUNT), 65536);
        assert_eq!(meta.get(HintMeta::ID), u16::MAX);
        assert_eq!(meta.get(HintMeta::LEN), 63);
        assert!(meta.test(HintMeta::OCCUPIED));
    }

    #[test]
    fn nodes_fit_one_cache_line_and_hints_two_registers() {
        assert_eq!(size_of::<Node>(), 64);
        assert_eq!(align_of::<Node>(), 64);
        assert_eq!(size_of::<LookupHint>(), 16);
        assert_eq!(align_of::<LookupHint>(), 8);
    }
}
