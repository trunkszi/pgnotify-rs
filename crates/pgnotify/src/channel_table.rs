//! Fixed-capacity, cache-aligned byte-trie channel registry.
use crate::{
    error::{PgNotifyError, Result},
    types::{ChannelId, FixedString},
};

const NONE: u32 = u32::MAX;
#[repr(align(64))]
struct Node {
    children: [u32; 16],
    id: Option<ChannelId>,
}
impl Node {
    fn new() -> Self {
        Self {
            children: [NONE; 16],
            id: None,
        }
    }
}

/// Name lookup takes at most 126 radix-16 trie edges, independent of channel count.
/// Registration allocates only on the subscription control path. IDs are stable until removal.
#[repr(align(64))]
pub struct ChannelTable {
    nodes: Vec<Node>,
    channels: Vec<Option<FixedString<64>>>,
    free: Vec<ChannelId>,
    len: usize,
    vacant_nodes: Vec<usize>,
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
    fn allocate(capacity: usize) -> Self {
        Self {
            nodes: vec![Node::new()],
            channels: vec![None; capacity],
            free: (0..capacity).rev().map(|id| ChannelId(id as u16)).collect(),
            len: 0,
            vacant_nodes: Vec::new(),
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
        if self.lookup(channel.as_str()) == Some(id) {
            return Ok(());
        }
        if self.lookup(channel.as_str()).is_some() || self.channel(id).is_some() {
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
                    index
                };
                self.nodes[at].children[digit] = index as u32;
                index
            } else {
                next as usize
            };
        }
        self.nodes[at].id = Some(id);
        self.channels[usize::from(id.0)] = Some(channel);
        self.len += 1;
    }
    pub fn lookup(&self, name: &str) -> Option<ChannelId> {
        if !crate::types::validate_channel_name_bytes(name.as_bytes()) {
            return None;
        }
        self.find_node(name).and_then(|node| self.nodes[node].id)
    }
    fn find_node(&self, name: &str) -> Option<usize> {
        let mut at = 0;
        for digit in digits(name.as_bytes()) {
            let next = self.nodes[at].children[digit];
            if next == NONE {
                return None;
            }
            at = next as usize;
        }
        Some(at)
    }
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
        self.nodes[path[depth]].id = None;
        while depth > 0 {
            let node = path[depth];
            if self.nodes[node].id.is_some()
                || self.nodes[node].children.iter().any(|child| *child != NONE)
            {
                break;
            }
            self.nodes[path[depth - 1]].children[edges[depth - 1]] = NONE;
            self.vacant_nodes.push(node);
            depth -= 1;
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
