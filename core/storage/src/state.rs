// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

/// A block defines a list of transactions that it sees and the sequence of
/// the transactions (ledger). At the view of a block, after all
/// transactions being executed, the data associated with all addresses is
/// a State after the epoch defined by the block.
///
/// A writable state is copy-on-write reference to the base state in the
/// state manager. State is supposed to be owned by single user.

use std::fs::File;
use std::io::{Write, BufWriter};
use std::sync::Mutex;

use serde::{Serialize};

use cfx_types::{Address, H256, U256};

pub use super::impls::state::State;

lazy_static! {
    pub static ref STATE_KV_TRACE_WRITER: Mutex<BufWriter<File>> =
        Mutex::new(BufWriter::new(File::create("state_kv_trace_tmp").unwrap()));
}

#[derive(Debug, Serialize)]
struct Wrapper {
    data: Box<[u8]>,
}

#[derive(Serialize)]
pub enum StateKVTraceItem<'a, 'b> {
    Epoch(H256),
    Transaction(H256),
    Reward(Address, U256),
    Get(StorageKey<'a>),
    Set(StorageKey<'a>, &'b Box<[u8]>),
    Delete(StorageKey<'a>),
    DeletaAll(StorageKey<'a>, bool), // bool: is_read_only
}

pub fn serialize_into_file_wrapped<W, T: ?Sized>(
    writer: W, value: &T
) -> std::result::Result<(), Box<bincode::ErrorKind>>
where
    W: Write,
    T: Serialize,
{
    // println!("{}", serde_json::to_string(value).unwrap());
    let data = Box::from(bincode::serialize(value)?);
    bincode::serialize_into(writer, &Wrapper{data})
}

pub type WithProof = primitives::static_bool::Yes;
pub type NoProof = primitives::static_bool::No;

// The trait is created to separate the implementation to another file, and the
// concrete struct is put into inner mod, because the implementation is
// anticipated to be too complex to present in the same file of the API.
pub trait StateTrait {
    // Actions.
    fn get(&self, access_key: StorageKey) -> Result<Option<Box<[u8]>>>;
    fn set(&mut self, access_key: StorageKey, value: Box<[u8]>) -> Result<()>;
    fn delete(&mut self, access_key: StorageKey) -> Result<()>;
    fn delete_test_only(
        &mut self, access_key: StorageKey,
    ) -> Result<Option<Box<[u8]>>>;
    // Delete everything prefixed by access_key and return deleted key value
    // pairs.
    fn delete_all<AM: access_mode::AccessMode>(
        &mut self, access_key_prefix: StorageKey,
    ) -> Result<Option<Vec<MptKeyValue>>>;

    // Finalize
    /// It's costly to compute state root however it's only necessary to compute
    /// state root once before committing.
    fn compute_state_root(&mut self) -> Result<StateRootWithAuxInfo>;
    fn get_state_root(&self) -> Result<StateRootWithAuxInfo>;
    fn commit(&mut self, epoch: EpochId) -> Result<StateRootWithAuxInfo>;
}

pub trait StateTraitExt {
    fn get_with_proof(
        &self, access_key: StorageKey,
    ) -> Result<(Option<Box<[u8]>>, StateProof)>;

    /// Compute the merkle of the node under `access_key` in all tries.
    /// Node merkle is computed on the value and children hashes, ignoring the
    /// compressed path.
    fn get_node_merkle_all_versions<WithProof: StaticBool>(
        &self, access_key: StorageKey,
    ) -> Result<(NodeMerkleTriplet, NodeMerkleProof)>;
}

use super::{
    impls::{
        errors::*, node_merkle_proof::NodeMerkleProof, state_proof::StateProof,
    },
    utils::access_mode,
    MptKeyValue, StateRootWithAuxInfo,
};
use primitives::{EpochId, NodeMerkleTriplet, StaticBool, StorageKey};
