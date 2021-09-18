// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

pub use self::{
    account_entry::{OverlayAccount, COMMISSION_PRIVILEGE_SPECIAL_KEY},
    substate::{cleanup_mode, CallStackInfo, Substate},
};

use self::account_entry::{AccountEntry, AccountState};
use crate::{hash::KECCAK_EMPTY, transaction_pool::SharedTransactionPool};
use cfx_bytes::Bytes;
use cfx_internal_common::{
    debug::ComputeEpochDebugRecord, StateRootWithAuxInfo,
};
use cfx_parameters::{
    internal_contract_addresses::SPONSOR_WHITELIST_CONTROL_CONTRACT_ADDRESS,
    staking::*,
};
use cfx_state::{
    maybe_address,
    state_trait::{
        CheckpointTrait, CheckpointTxDeltaTrait, CheckpointTxTrait,
        StateOpsTrait, StateOpsTxTrait, StateTrait, StateTxDeltaTrait,
        StateTxTrait,
    },
    CleanupMode, CollateralCheckResult, SubstateTrait,
};
use cfx_statedb::{
    ErrorKind as DbErrorKind, Result as DbResult, StateDbExt,
    StateDbGeneric as StateDb,
};
use cfx_storage::{utils::access_mode, StorageState, StorageStateTrait};
use cfx_types::{address_util::AddressUtil, Address, H256, U256};
use parking_lot::{
    MappedRwLockWriteGuard, RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard,
};
#[cfg(test)]
use primitives::storage::STORAGE_LAYOUT_REGULAR_V0;
use primitives::{
    Account, DepositList, EpochId, SkipInputCheck, SponsorInfo, StorageKey,
    StorageLayout, StorageValue, VoteStakeList,
};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};

mod account_entry;
#[cfg(test)]
mod account_entry_tests;
pub mod prefetcher;
#[cfg(test)]
mod state_tests;
mod substate;

#[derive(Copy, Clone)]
pub enum RequireCache {
    None,
    Code,
    DepositList,
    VoteStakeList,
}

#[derive(Copy, Clone, Debug)]
struct StakingState {
    // This is the total number of CFX issued.
    total_issued_tokens: U256,
    // This is the total number of CFX used as staking.
    total_staking_tokens: U256,
    // This is the total number of CFX used as collateral.
    // This field should never be read during tx execution. (Can be updated)
    total_storage_tokens: U256,
    // This is the interest rate per block.
    interest_rate_per_block: U256,
    // This is the accumulated interest rate.
    accumulate_interest_rate: U256,
}

pub type State = StateGeneric<StorageState>;

pub struct StateGenericIO<StateDbStorage: StorageStateTrait> {
    pub db: StateDb<StateDbStorage>,

    // Contains the changes to the states and some unchanged state entries.
    cache: RwLock<HashMap<Address, AccountEntry>>,
}

pub struct StateGenericInfo {
    // Only created once for txpool notification.
    // Each element is an Ok(Account) for updated account, or Err(Address)
    // for deleted account.
    accounts_to_notify: Vec<Result<Account, Address>>,

    // TODO: try not to make it special?
    staking_state: StakingState,

    // Checkpoint to the changes.
    staking_state_checkpoints: RwLock<Vec<StakingState>>,
    checkpoints: RwLock<Vec<HashMap<Address, Option<AccountEntry>>>>,
}

// Take apart StateGeneric to realize partial borrow
pub struct StateGeneric<StateDbStorage: StorageStateTrait> {
    pub io: StateGenericIO<StateDbStorage>,
    pub info: StateGenericInfo,
}

impl<StateDbStorage: StorageStateTrait> StateGenericIO<StateDbStorage> {
    /// Collects the cache (`ownership_change` in `OverlayAccount`) of storage
    /// change and write to substate.
    /// It is idempotent. But its execution is costly.
    fn collect_ownership_changed(
        &self, info: &mut StateGenericInfo, substate: &mut Substate,
    ) -> DbResult<()> {
        if let Some(checkpoint) = info.checkpoints.get_mut().last() {
            for address in checkpoint.keys() {
                if let Some(ref mut maybe_acc) =
                    self.cache.write().get_mut(address).filter(|x| x.is_dirty())
                {
                    if let Some(ref mut acc) = maybe_acc.account.as_mut() {
                        acc.commit_ownership_change(&self.db, substate)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Charge and refund all the storage collaterals.
    /// The suicided addresses are skimmed because their collateral have been
    /// checked out. This function should only be called in post-processing
    /// of a transaction.
    fn settle_collateral_for_all(
        &self, info: &mut StateGenericInfo, substate: &Substate,
        account_start_nonce: U256,
    ) -> DbResult<CollateralCheckResult>
    {
        for address in substate.keys_for_collateral_changed().iter() {
            match self.settle_collateral_for_address(
                info,
                address,
                substate,
                account_start_nonce,
            )? {
                CollateralCheckResult::Valid => {}
                res => return Ok(res),
            }
        }
        Ok(CollateralCheckResult::Valid)
    }

    // TODO: This function can only be called after VM execution. There are some
    // test cases breaks this assumption, which will be fixed in a separated PR.
    fn collect_and_settle_collateral(
        &self, info: &mut StateGenericInfo, original_sender: &Address,
        storage_limit: &U256, substate: &mut Substate,
        account_start_nonce: U256,
    ) -> DbResult<CollateralCheckResult>
    {
        self.collect_ownership_changed(info, substate)?;
        let res = match self.settle_collateral_for_all(
            info,
            substate,
            account_start_nonce,
        )? {
            CollateralCheckResult::Valid => {
                self.check_storage_limit(original_sender, storage_limit)?
            }
            res => res,
        };
        Ok(res)
    }

    fn record_storage_and_whitelist_entries_release(
        &self, info: &mut StateGenericInfo, address: &Address,
        substate: &mut Substate,
    ) -> DbResult<()>
    {
        self.remove_whitelists_for_contract::<access_mode::Write>(
            info, address,
        )?;

        // Process collateral for removed storage.
        // TODO: try to do it in a better way, e.g. first log the deletion
        //  somewhere then apply the collateral change.
        {
            let mut sponsor_whitelist_control_address = self.require_exists(
                info,
                &SPONSOR_WHITELIST_CONTROL_CONTRACT_ADDRESS,
                /* require_code = */ false,
            )?;
            sponsor_whitelist_control_address
                .commit_ownership_change(&self.db, substate)?;
        }

        let account_cache_read_guard = self.cache.read();
        let maybe_account = account_cache_read_guard
            .get(address)
            .and_then(|acc| acc.account.as_ref());

        let storage_key_value = self
            .db
            .iterate_all(StorageKey::new_storage_root_key(address), None)?;
        for (key, value) in &storage_key_value {
            if let StorageKey::StorageKey { storage_key, .. } =
                StorageKey::from_key_bytes::<SkipInputCheck>(&key[..])
            {
                // Check if the key has been touched. We use the local
                // information to find out if collateral refund is necessary
                // for touched keys.
                if maybe_account.map_or(true, |acc| {
                    acc.storage_value_write_cache().get(storage_key).is_none()
                }) {
                    let storage_value =
                        rlp::decode::<StorageValue>(value.as_ref())?;
                    let storage_owner =
                        storage_value.owner.as_ref().unwrap_or(address);
                    substate.record_storage_release(
                        storage_owner,
                        COLLATERAL_UNITS_PER_STORAGE_KEY,
                    );
                }
            }
        }

        if let Some(acc) = maybe_account {
            // The current value isn't important because it will be deleted.
            for (key, _value) in acc.storage_value_write_cache() {
                if let Some(storage_owner) =
                    acc.original_ownership_at(&self.db, key)?
                {
                    substate.record_storage_release(
                        &storage_owner,
                        COLLATERAL_UNITS_PER_STORAGE_KEY,
                    );
                }
            }
        }
        Ok(())
    }

    // It's guaranteed that the second call of this method is a no-op.
    fn compute_state_root(
        &mut self, info: &mut StateGenericInfo,
        mut debug_record: Option<&mut ComputeEpochDebugRecord>,
    ) -> DbResult<StateRootWithAuxInfo>
    {
        debug!("state.compute_state_root");

        assert!(info.checkpoints.get_mut().is_empty());
        assert!(info.staking_state_checkpoints.get_mut().is_empty());

        let mut sorted_dirty_accounts =
            self.cache.get_mut().drain().collect::<Vec<_>>();
        sorted_dirty_accounts.sort_by(|a, b| a.0.cmp(&b.0));

        let mut killed_addresses = Vec::new();
        for (address, entry) in sorted_dirty_accounts.iter_mut() {
            entry.state = AccountState::Committed;
            match &mut entry.account {
                None => {
                    killed_addresses.push(*address);
                    info.accounts_to_notify.push(Err(*address));
                }
                Some(account) => {
                    account.commit(
                        self,
                        address,
                        debug_record.as_deref_mut(),
                    )?;
                    info.accounts_to_notify.push(Ok(account.as_account()?));
                }
            }
        }
        self.recycle_storage(killed_addresses, debug_record.as_deref_mut())?;
        self.commit_staking_state(info, debug_record.as_deref_mut())?;
        self.db.compute_state_root(debug_record)
    }

    fn commit(
        &mut self, info: &mut StateGenericInfo, epoch_id: EpochId,
        mut debug_record: Option<&mut ComputeEpochDebugRecord>,
    ) -> DbResult<StateRootWithAuxInfo>
    {
        debug!("Commit epoch[{}]", epoch_id);
        self.compute_state_root(info, debug_record.as_deref_mut())?;
        Ok(self.db.commit(epoch_id, debug_record)?)
    }
}

impl StateGenericInfo {
    /// Calculate the secondary reward for the next block number.
    fn bump_block_number_accumulate_interest(&mut self) -> U256 {
        assert!(self.staking_state_checkpoints.get_mut().is_empty());
        self.staking_state.accumulate_interest_rate =
            self.staking_state.accumulate_interest_rate
                * (*INTEREST_RATE_PER_BLOCK_SCALE
                    + self.staking_state.interest_rate_per_block)
                / *INTEREST_RATE_PER_BLOCK_SCALE;
        let secondary_reward = self.staking_state.total_storage_tokens
            * self.staking_state.interest_rate_per_block
            / *INTEREST_RATE_PER_BLOCK_SCALE;
        // TODO: the interest from tokens other than storage and staking should
        // send to public fund.
        secondary_reward
    }

    /// Maintain `total_issued_tokens`.
    fn add_total_issued(&mut self, v: U256) {
        assert!(self.staking_state_checkpoints.get_mut().is_empty());
        self.staking_state.total_issued_tokens += v;
    }

    /// Maintain `total_issued_tokens`. This is only used in the extremely
    /// unlikely case that there are a lot of partial invalid blocks.
    fn subtract_total_issued(&mut self, v: U256) {
        assert!(self.staking_state_checkpoints.get_mut().is_empty());
        self.staking_state.total_issued_tokens -= v;
    }
}

impl<StateDbStorage: StorageStateTrait> StateGenericIO<StateDbStorage> {
    fn new_contract_with_admin(
        &self, info: &mut StateGenericInfo, contract: &Address,
        admin: &Address, balance: U256, nonce: U256,
        storage_layout: Option<StorageLayout>,
    ) -> DbResult<()>
    {
        Self::update_cache(
            &mut *self.cache.write(),
            info.checkpoints.get_mut(),
            contract,
            AccountEntry::new_dirty(Some(
                OverlayAccount::new_contract_with_admin(
                    contract,
                    balance,
                    nonce,
                    admin,
                    storage_layout,
                ),
            )),
        );
        Ok(())
    }

    fn balance(&self, address: &Address) -> DbResult<U256> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(U256::zero(), |account| *account.balance())
        })
    }

    fn is_contract_with_code(&self, address: &Address) -> DbResult<bool> {
        if !address.is_contract_address() {
            return Ok(false);
        }
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(false, |acc| acc.code_hash() != KECCAK_EMPTY)
        })
    }

    fn sponsor_for_gas(&self, address: &Address) -> DbResult<Option<Address>> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(None, |acc| {
                maybe_address(&acc.sponsor_info().sponsor_for_gas)
            })
        })
    }

    fn sponsor_for_collateral(
        &self, address: &Address,
    ) -> DbResult<Option<Address>> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(None, |acc| {
                maybe_address(&acc.sponsor_info().sponsor_for_collateral)
            })
        })
    }

    fn set_sponsor_for_gas(
        &self, info: &mut StateGenericInfo, address: &Address,
        sponsor: &Address, sponsor_balance: &U256, upper_bound: &U256,
    ) -> DbResult<()>
    {
        if *sponsor != self.sponsor_for_gas(address)?.unwrap_or_default()
            || *sponsor_balance != self.sponsor_balance_for_gas(address)?
        {
            self.require_exists(info, address, false).map(|mut x| {
                x.set_sponsor_for_gas(sponsor, sponsor_balance, upper_bound)
            })
        } else {
            Ok(())
        }
    }

    fn set_sponsor_for_collateral(
        &self, info: &mut StateGenericInfo, address: &Address,
        sponsor: &Address, sponsor_balance: &U256,
    ) -> DbResult<()>
    {
        if *sponsor != self.sponsor_for_collateral(address)?.unwrap_or_default()
            || *sponsor_balance
                != self.sponsor_balance_for_collateral(address)?
        {
            self.require_exists(info, address, false).map(|mut x| {
                x.set_sponsor_for_collateral(sponsor, sponsor_balance)
            })
        } else {
            Ok(())
        }
    }

    fn sponsor_info(&self, address: &Address) -> DbResult<Option<SponsorInfo>> {
        self.ensure_account_loaded(address, RequireCache::None, |maybe_acc| {
            maybe_acc.map(|acc| acc.sponsor_info().clone())
        })
    }

    fn sponsor_gas_bound(&self, address: &Address) -> DbResult<U256> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(U256::zero(), |acc| acc.sponsor_info().sponsor_gas_bound)
        })
    }

    fn sponsor_balance_for_gas(&self, address: &Address) -> DbResult<U256> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(U256::zero(), |acc| {
                acc.sponsor_info().sponsor_balance_for_gas
            })
        })
    }

    fn sponsor_balance_for_collateral(
        &self, address: &Address,
    ) -> DbResult<U256> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(U256::zero(), |acc| {
                acc.sponsor_info().sponsor_balance_for_collateral
            })
        })
    }

    fn set_admin(
        &self, info: &mut StateGenericInfo, contract_address: &Address,
        admin: &Address,
    ) -> DbResult<()>
    {
        self.require_exists(info, &contract_address, false)?
            .set_admin(admin);
        Ok(())
    }

    fn sub_sponsor_balance_for_gas(
        &self, info: &mut StateGenericInfo, address: &Address, by: &U256,
    ) -> DbResult<()> {
        if !by.is_zero() {
            self.require_exists(info, address, false)?
                .sub_sponsor_balance_for_gas(by);
        }
        Ok(())
    }

    fn add_sponsor_balance_for_gas(
        &self, info: &mut StateGenericInfo, address: &Address, by: &U256,
    ) -> DbResult<()> {
        if !by.is_zero() {
            self.require_exists(info, address, false)?
                .add_sponsor_balance_for_gas(by);
        }
        Ok(())
    }

    fn sub_sponsor_balance_for_collateral(
        &self, info: &mut StateGenericInfo, address: &Address, by: &U256,
    ) -> DbResult<()> {
        if !by.is_zero() {
            self.require_exists(info, address, false)?
                .sub_sponsor_balance_for_collateral(by);
        }
        Ok(())
    }

    fn add_sponsor_balance_for_collateral(
        &self, info: &mut StateGenericInfo, address: &Address, by: &U256,
    ) -> DbResult<()> {
        if !by.is_zero() {
            self.require_exists(info, address, false)?
                .add_sponsor_balance_for_collateral(by);
        }
        Ok(())
    }

    fn check_commission_privilege(
        &self, contract_address: &Address, user: &Address,
    ) -> DbResult<bool> {
        match self.ensure_account_loaded(
            &SPONSOR_WHITELIST_CONTROL_CONTRACT_ADDRESS,
            RequireCache::None,
            |acc| {
                acc.map_or(Ok(false), |acc| {
                    acc.check_commission_privilege(
                        &self.db,
                        contract_address,
                        user,
                    )
                })
            },
        ) {
            Ok(Ok(bool)) => Ok(bool),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(e),
        }
    }

    fn add_commission_privilege(
        &self, info: &mut StateGenericInfo, contract_address: Address,
        contract_owner: Address, user: Address,
    ) -> DbResult<()>
    {
        info!("add_commission_privilege contract_address: {:?}, contract_owner: {:?}, user: {:?}", contract_address, contract_owner, user);

        let mut account = self.require_exists(
            info,
            &SPONSOR_WHITELIST_CONTROL_CONTRACT_ADDRESS,
            false,
        )?;
        Ok(account.add_commission_privilege(
            contract_address,
            contract_owner,
            user,
        ))
    }

    fn remove_commission_privilege(
        &self, info: &mut StateGenericInfo, contract_address: Address,
        contract_owner: Address, user: Address,
    ) -> DbResult<()>
    {
        let mut account = self.require_exists(
            info,
            &SPONSOR_WHITELIST_CONTROL_CONTRACT_ADDRESS,
            false,
        )?;
        Ok(account.remove_commission_privilege(
            contract_address,
            contract_owner,
            user,
        ))
    }

    // TODO: maybe return error for reserved address? Not sure where is the best
    //  place to do the check.
    fn nonce(&self, address: &Address) -> DbResult<U256> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(U256::zero(), |account| *account.nonce())
        })
    }

    fn init_code(
        &self, info: &mut StateGenericInfo, address: &Address, code: Bytes,
        owner: Address,
    ) -> DbResult<()>
    {
        self.require_exists(info, address, false)?
            .init_code(code, owner);
        Ok(())
    }

    fn code_hash(&self, address: &Address) -> DbResult<Option<H256>> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.and_then(|acc| Some(acc.code_hash()))
        })
    }

    fn code_size(&self, address: &Address) -> DbResult<Option<usize>> {
        self.ensure_account_loaded(address, RequireCache::Code, |acc| {
            acc.and_then(|acc| acc.code_size())
        })
    }

    fn code_owner(&self, address: &Address) -> DbResult<Option<Address>> {
        self.ensure_account_loaded(address, RequireCache::Code, |acc| {
            acc.as_ref().map_or(None, |acc| acc.code_owner())
        })
    }

    fn code(&self, address: &Address) -> DbResult<Option<Arc<Vec<u8>>>> {
        self.ensure_account_loaded(address, RequireCache::Code, |acc| {
            acc.as_ref().map_or(None, |acc| acc.code())
        })
    }

    fn staking_balance(&self, address: &Address) -> DbResult<U256> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(U256::zero(), |account| *account.staking_balance())
        })
    }

    fn collateral_for_storage(&self, address: &Address) -> DbResult<U256> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(U256::zero(), |account| {
                *account.collateral_for_storage()
            })
        })
    }

    fn admin(&self, address: &Address) -> DbResult<Address> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(Address::zero(), |acc| *acc.admin())
        })
    }

    fn withdrawable_staking_balance(
        &self, address: &Address, current_block_number: u64,
    ) -> DbResult<U256> {
        self.ensure_account_loaded(
            address,
            RequireCache::VoteStakeList,
            |acc| {
                acc.map_or(U256::zero(), |acc| {
                    acc.withdrawable_staking_balance(current_block_number)
                })
            },
        )
    }

    fn locked_staking_balance_at_block_number(
        &self, address: &Address, block_number: u64,
    ) -> DbResult<U256> {
        self.ensure_account_loaded(
            address,
            RequireCache::VoteStakeList,
            |acc| {
                acc.map_or(U256::zero(), |acc| {
                    acc.staking_balance()
                        - acc.withdrawable_staking_balance(block_number)
                })
            },
        )
    }

    fn deposit_list_length(&self, address: &Address) -> DbResult<usize> {
        self.ensure_account_loaded(address, RequireCache::DepositList, |acc| {
            acc.map_or(0, |acc| acc.deposit_list().map_or(0, |l| l.len()))
        })
    }

    fn vote_stake_list_length(&self, address: &Address) -> DbResult<usize> {
        self.ensure_account_loaded(
            address,
            RequireCache::VoteStakeList,
            |acc| {
                acc.map_or(0, |acc| {
                    acc.vote_stake_list().map_or(0, |l| l.len())
                })
            },
        )
    }

    fn clean_account(
        &self, info: &mut StateGenericInfo, address: &Address,
    ) -> DbResult<()> {
        *&mut *self.require_or_new_basic_account(
            info,
            address,
            &U256::zero(),
        )? = OverlayAccount::from_loaded(address, Default::default());
        Ok(())
    }

    // TODO: This implementation will fail
    // tests::load_chain_tests::test_load_chain. We need to figure out why.
    //
    // fn clean_account(&mut self, address: &Address) -> DbResult<()> {
    //     Self::update_cache(
    //         self.cache.get_mut(),
    //         self.checkpoints.get_mut(),
    //         address,
    //         AccountEntry::new_dirty(None),
    //     );
    //     Ok(())
    // }

    fn inc_nonce(
        &self, info: &mut StateGenericInfo, address: &Address,
        account_start_nonce: &U256,
    ) -> DbResult<()>
    {
        self.require_or_new_basic_account(info, address, account_start_nonce)
            .map(|mut x| x.inc_nonce())
    }

    fn set_nonce(
        &self, info: &mut StateGenericInfo, address: &Address, nonce: &U256,
    ) -> DbResult<()> {
        self.require_or_new_basic_account(info, address, nonce)
            .map(|mut x| x.set_nonce(&nonce))
    }

    fn sub_balance(
        &self, info: &mut StateGenericInfo, address: &Address, by: &U256,
        cleanup_mode: &mut CleanupMode,
    ) -> DbResult<()>
    {
        if !by.is_zero() {
            self.require_exists(info, address, false)?.sub_balance(by);
        }

        if let CleanupMode::TrackTouched(ref mut set) = *cleanup_mode {
            if self.exists(address)? {
                set.insert(*address);
            }
        }
        Ok(())
    }

    fn add_balance(
        &self, info: &mut StateGenericInfo, address: &Address, by: &U256,
        cleanup_mode: CleanupMode, account_start_nonce: U256,
    ) -> DbResult<()>
    {
        let exists = self.exists(address)?;
        if !address.is_valid_address() {
            // Sending to invalid addresses are not allowed. Note that this
            // check is required because at serialization we assume
            // only valid addresses.
            //
            // There are checks to forbid it at transact level.
            //
            // The logic here is intended for incorrect miner coin-base. In this
            // case, the mining reward get lost.
            debug!(
                "add_balance: address does not already exist and is not a valid address. {:?}",
                address
            );
            return Ok(());
        }
        if !by.is_zero()
            || (cleanup_mode == CleanupMode::ForceCreate && !exists)
        {
            self.require_or_new_basic_account(
                info,
                address,
                &account_start_nonce,
            )?
            .add_balance(by);
        }

        if let CleanupMode::TrackTouched(set) = cleanup_mode {
            if exists {
                set.insert(*address);
            }
        }
        Ok(())
    }

    fn transfer_balance(
        &self, info: &mut StateGenericInfo, from: &Address, to: &Address,
        by: &U256, mut cleanup_mode: CleanupMode, account_start_nonce: U256,
    ) -> DbResult<()>
    {
        self.sub_balance(info, from, by, &mut cleanup_mode)?;
        self.add_balance(info, to, by, cleanup_mode, account_start_nonce)?;
        Ok(())
    }

    fn deposit(
        &self, info: &mut StateGenericInfo, address: &Address, amount: &U256,
        current_block_number: u64,
    ) -> DbResult<()>
    {
        if !amount.is_zero() {
            {
                let mut account = self.require_exists(info, address, false)?;
                account.cache_staking_info(
                    true,  /* cache_deposit_list */
                    false, /* cache_vote_list */
                    &self.db,
                )?;
                account.deposit(
                    *amount,
                    info.staking_state.accumulate_interest_rate,
                    current_block_number,
                );
            }
            info.staking_state.total_staking_tokens += *amount;
        }
        Ok(())
    }

    fn withdraw(
        &self, info: &mut StateGenericInfo, address: &Address, amount: &U256,
    ) -> DbResult<U256> {
        if !amount.is_zero() {
            let interest;
            {
                let mut account = self.require_exists(info, address, false)?;
                account.cache_staking_info(
                    true,  /* cache_deposit_list */
                    false, /* cache_vote_list */
                    &self.db,
                )?;
                interest = account.withdraw(
                    *amount,
                    info.staking_state.accumulate_interest_rate,
                );
            }
            // the interest will be put in balance.
            info.staking_state.total_issued_tokens += interest;
            info.staking_state.total_staking_tokens -= *amount;
            Ok(interest)
        } else {
            Ok(U256::zero())
        }
    }

    fn vote_lock(
        &self, info: &mut StateGenericInfo, address: &Address, amount: &U256,
        unlock_block_number: u64,
    ) -> DbResult<()>
    {
        if !amount.is_zero() {
            let mut account = self.require_exists(info, address, false)?;
            account.cache_staking_info(
                false, /* cache_deposit_list */
                true,  /* cache_vote_list */
                &self.db,
            )?;
            account.vote_lock(*amount, unlock_block_number);
        }
        Ok(())
    }

    fn remove_expired_vote_stake_info(
        &self, info: &mut StateGenericInfo, address: &Address,
        current_block_number: u64,
    ) -> DbResult<()>
    {
        let mut account = self.require_exists(info, address, false)?;
        account.cache_staking_info(
            false, /* cache_deposit_list */
            true,  /* cache_vote_list */
            &self.db,
        )?;
        account.remove_expired_vote_stake_info(current_block_number);
        Ok(())
    }
}

impl StateGenericInfo {
    fn total_issued_tokens(&self) -> U256 {
        self.staking_state.total_issued_tokens
    }

    fn total_staking_tokens(&self) -> U256 {
        self.staking_state.total_staking_tokens
    }

    fn total_storage_tokens(&self) -> U256 {
        self.staking_state.total_storage_tokens
    }
}

impl<StateDbStorage: StorageStateTrait> StateGenericIO<StateDbStorage> {
    fn remove_contract(
        &self, info: &mut StateGenericInfo, address: &Address,
    ) -> DbResult<()> {
        let removed_whitelist = self
            .remove_whitelists_for_contract::<access_mode::Write>(
                info, address,
            )?;
        if !removed_whitelist.is_empty() {
            error!(
                "removed_whitelist here should be empty unless in unit tests."
            );
        }
        Self::update_cache(
            &mut self.cache.write(),
            info.checkpoints.get_mut(),
            address,
            AccountEntry::new_dirty(None),
        );

        Ok(())
    }

    fn exists(&self, address: &Address) -> DbResult<bool> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.is_some()
        })
    }

    fn exists_and_not_null(&self, address: &Address) -> DbResult<bool> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(false, |acc| !acc.is_null())
        })
    }

    fn storage_at(&self, address: &Address, key: &[u8]) -> DbResult<U256> {
        self.ensure_account_loaded(address, RequireCache::None, |acc| {
            acc.map_or(Ok(U256::zero()), |account| {
                account.storage_at(&self.db, key)
            })
        })?
    }

    fn set_storage(
        &self, info: &mut StateGenericInfo, address: &Address, key: Vec<u8>,
        value: U256, owner: Address,
    ) -> DbResult<()>
    {
        if self.storage_at(address, &key)? != value {
            self.require_exists(info, address, false)?
                .set_storage(key, value, owner)
        }
        Ok(())
    }
}

impl StateGenericInfo {
    /// Create a recoverable checkpoint of this state. Return the checkpoint
    /// index. The checkpoint records any old value which is alive at the
    /// creation time of the checkpoint and updated after that and before
    /// the creation of the next checkpoint.
    fn checkpoint(&mut self) -> usize {
        self.staking_state_checkpoints
            .get_mut()
            .push(self.staking_state.clone());
        let checkpoints = self.checkpoints.get_mut();
        let index = checkpoints.len();
        checkpoints.push(HashMap::new());
        index
    }

    /// Merge last checkpoint with previous.
    /// Caller should make sure the function
    /// `collect_ownership_changed()` was called before calling
    /// this function.
    fn discard_checkpoint(&mut self) {
        // merge with previous checkpoint
        let last = self.checkpoints.get_mut().pop();
        if let Some(mut checkpoint) = last {
            self.staking_state_checkpoints.get_mut().pop();
            if let Some(ref mut prev) = self.checkpoints.get_mut().last_mut() {
                if prev.is_empty() {
                    **prev = checkpoint;
                } else {
                    for (k, v) in checkpoint.drain() {
                        prev.entry(k).or_insert(v);
                    }
                }
            }
        }
    }
}

impl<StateDbStorage: StorageStateTrait> StateGenericIO<StateDbStorage> {
    /// Revert to the last checkpoint and discard it.
    fn revert_to_checkpoint(&self, info: &mut StateGenericInfo) {
        if let Some(mut checkpoint) = info.checkpoints.get_mut().pop() {
            info.staking_state = info
                .staking_state_checkpoints
                .get_mut()
                .pop()
                .expect("staking_state_checkpoint should exist");
            for (k, v) in checkpoint.drain() {
                match v {
                    Some(v) => match self.cache.write().entry(k) {
                        Entry::Occupied(mut e) => {
                            e.get_mut().overwrite_with(v);
                        }
                        Entry::Vacant(e) => {
                            e.insert(v);
                        }
                    },
                    None => {
                        if let Entry::Occupied(e) = self.cache.write().entry(k)
                        {
                            if e.get().is_dirty() {
                                e.remove();
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<StateDbStorage: StorageStateTrait> StateGeneric<StateDbStorage> {
    pub fn new(db: StateDb<StateDbStorage>) -> DbResult<Self> {
        let annual_interest_rate = db.get_annual_interest_rate()?;
        let accumulate_interest_rate = db.get_accumulate_interest_rate()?;
        let total_issued_tokens = db.get_total_issued_tokens()?;
        let total_staking_tokens = db.get_total_staking_tokens()?;
        let total_storage_tokens = db.get_total_storage_tokens()?;

        let staking_state = if db.is_initialized()? {
            StakingState {
                total_issued_tokens,
                total_staking_tokens,
                total_storage_tokens,
                interest_rate_per_block: annual_interest_rate
                    / U256::from(BLOCKS_PER_YEAR),
                accumulate_interest_rate,
            }
        } else {
            // If db is not initialized, all the loaded value should be zero.
            assert!(
                annual_interest_rate.is_zero(),
                "annual_interest_rate is non-zero when db is un-init"
            );
            assert!(
                accumulate_interest_rate.is_zero(),
                "accumulate_interest_rate is non-zero when db is un-init"
            );
            assert!(
                total_issued_tokens.is_zero(),
                "total_issued_tokens is non-zero when db is un-init"
            );
            assert!(
                total_staking_tokens.is_zero(),
                "total_staking_tokens is non-zero when db is un-init"
            );
            assert!(
                total_storage_tokens.is_zero(),
                "total_storage_tokens is non-zero when db is un-init"
            );

            StakingState {
                total_issued_tokens: U256::default(),
                total_staking_tokens: U256::default(),
                total_storage_tokens: U256::default(),
                interest_rate_per_block: *INITIAL_INTEREST_RATE_PER_BLOCK,
                accumulate_interest_rate: *ACCUMULATED_INTEREST_RATE_SCALE,
            }
        };

        Ok(StateGeneric {
            io: StateGenericIO {
                db,
                cache: Default::default(),
            },
            info: StateGenericInfo {
                staking_state_checkpoints: Default::default(),
                checkpoints: Default::default(),
                staking_state,
                accounts_to_notify: Default::default(),
            },
        })
    }
}

impl<StateDbStorage: StorageStateTrait> StateGenericIO<StateDbStorage> {
    /// Charges or refund storage collateral and update `total_storage_tokens`.
    fn settle_collateral_for_address(
        &self, info: &mut StateGenericInfo, addr: &Address,
        substate: &dyn SubstateTrait, account_start_nonce: U256,
    ) -> DbResult<CollateralCheckResult>
    {
        let (inc_collaterals, sub_collaterals) =
            substate.get_collateral_change(addr);
        let (inc, sub) = (
            *DRIPS_PER_STORAGE_COLLATERAL_UNIT * inc_collaterals,
            *DRIPS_PER_STORAGE_COLLATERAL_UNIT * sub_collaterals,
        );

        if !sub.is_zero() {
            self.sub_collateral_for_storage(
                info,
                addr,
                &sub,
                account_start_nonce,
            )?;
        }
        if !inc.is_zero() {
            let balance = if addr.is_contract_address() {
                self.sponsor_balance_for_collateral(addr)?
            } else {
                self.balance(addr)?
            };
            // sponsor_balance is not enough to cover storage incremental.
            if inc > balance {
                return Ok(CollateralCheckResult::NotEnoughBalance {
                    required: inc,
                    got: balance,
                });
            }
            self.add_collateral_for_storage(info, addr, &inc)?;
        }
        Ok(CollateralCheckResult::Valid)
    }

    fn check_storage_limit(
        &self, original_sender: &Address, storage_limit: &U256,
    ) -> DbResult<CollateralCheckResult> {
        let collateral_for_storage =
            self.collateral_for_storage(original_sender)?;
        if collateral_for_storage > *storage_limit {
            Ok(CollateralCheckResult::ExceedStorageLimit {
                limit: *storage_limit,
                required: collateral_for_storage,
            })
        } else {
            Ok(CollateralCheckResult::Valid)
        }
    }

    #[cfg(test)]
    pub fn new_contract(
        &mut self, info: &mut StateGenericInfo, contract: &Address,
        balance: U256, nonce: U256,
    ) -> DbResult<()>
    {
        Self::update_cache(
            self.cache.get_mut(),
            info.checkpoints.get_mut(),
            contract,
            AccountEntry::new_dirty(Some(OverlayAccount::new_contract(
                contract,
                balance,
                nonce,
                Some(STORAGE_LAYOUT_REGULAR_V0),
            ))),
        );
        Ok(())
    }

    /// Caller should make sure that staking_balance for this account is
    /// sufficient enough.
    fn add_collateral_for_storage(
        &self, info: &mut StateGenericInfo, address: &Address, by: &U256,
    ) -> DbResult<()> {
        if !by.is_zero() {
            self.require_exists(info, address, false)?
                .add_collateral_for_storage(by);
            info.staking_state.total_storage_tokens += *by;
        }
        Ok(())
    }

    fn sub_collateral_for_storage(
        &self, info: &mut StateGenericInfo, address: &Address, by: &U256,
        account_start_nonce: U256,
    ) -> DbResult<()>
    {
        let collateral = self.collateral_for_storage(address)?;
        let refundable = if by > &collateral { &collateral } else { by };
        let burnt = *by - *refundable;
        if !refundable.is_zero() {
            self.require_or_new_basic_account(
                info,
                address,
                &account_start_nonce,
            )?
            .sub_collateral_for_storage(refundable);
        }
        info.staking_state.total_storage_tokens -= *by;
        info.staking_state.total_issued_tokens -= burnt;

        Ok(())
    }

    #[allow(dead_code)]
    pub fn touch(
        &mut self, info: &mut StateGenericInfo, address: &Address,
    ) -> DbResult<()> {
        drop(self.require_exists(info, address, false)?);
        Ok(())
    }

    fn needs_update(require: RequireCache, account: &OverlayAccount) -> bool {
        trace!("update_account_cache account={:?}", account);
        match require {
            RequireCache::None => false,
            RequireCache::Code => !account.is_code_loaded(),
            RequireCache::DepositList => account.deposit_list().is_none(),
            RequireCache::VoteStakeList => account.vote_stake_list().is_none(),
        }
    }

    /// Load required account data from the databases. Returns whether the
    /// cache succeeds.
    fn update_account_cache(
        require: RequireCache, account: &mut OverlayAccount,
        db: &StateDb<StateDbStorage>,
    ) -> DbResult<bool>
    {
        match require {
            RequireCache::None => Ok(true),
            RequireCache::Code => account.cache_code(db),
            RequireCache::DepositList => account.cache_staking_info(
                true,  /* cache_deposit_list */
                false, /* cache_vote_list */
                db,
            ),
            RequireCache::VoteStakeList => account.cache_staking_info(
                false, /* cache_deposit_list */
                true,  /* cache_vote_list */
                db,
            ),
        }
    }

    fn commit_staking_state(
        &mut self, info: &mut StateGenericInfo,
        mut debug_record: Option<&mut ComputeEpochDebugRecord>,
    ) -> DbResult<()>
    {
        self.db.set_annual_interest_rate(
            &(info.staking_state.interest_rate_per_block
                * U256::from(BLOCKS_PER_YEAR)),
            debug_record.as_deref_mut(),
        )?;
        self.db.set_accumulate_interest_rate(
            &info.staking_state.accumulate_interest_rate,
            debug_record.as_deref_mut(),
        )?;
        self.db.set_total_issued_tokens(
            &info.staking_state.total_issued_tokens,
            debug_record.as_deref_mut(),
        )?;
        self.db.set_total_staking_tokens(
            &info.staking_state.total_staking_tokens,
            debug_record.as_deref_mut(),
        )?;
        self.db.set_total_storage_tokens(
            &info.staking_state.total_storage_tokens,
            debug_record,
        )?;
        Ok(())
    }

    /// Assume that only contract with zero `collateral_for_storage` will be
    /// killed.
    pub fn recycle_storage(
        &mut self, killed_addresses: Vec<Address>,
        mut debug_record: Option<&mut ComputeEpochDebugRecord>,
    ) -> DbResult<()>
    {
        // TODO: Think about kill_dust and collateral refund.
        for address in &killed_addresses {
            self.db.delete_all(
                StorageKey::new_storage_root_key(address),
                debug_record.as_deref_mut(),
            )?;
            self.db.delete_all(
                StorageKey::new_code_root_key(address),
                debug_record.as_deref_mut(),
            )?;
            self.db.delete(
                StorageKey::new_account_key(address),
                debug_record.as_deref_mut(),
            )?;
            self.db.delete(
                StorageKey::new_deposit_list_key(address),
                debug_record.as_deref_mut(),
            )?;
            self.db.delete(
                StorageKey::new_vote_list_key(address),
                debug_record.as_deref_mut(),
            )?;
        }
        Ok(())
    }

    // FIXME: this should be part of the statetrait however transaction pool
    // creates circular dep.  if it proves impossible to break the loop we
    // use associated types for the tx pool.
    pub fn commit_and_notify(
        &mut self, info: &mut StateGenericInfo, epoch_id: EpochId,
        txpool: &SharedTransactionPool,
        debug_record: Option<&mut ComputeEpochDebugRecord>,
    ) -> DbResult<StateRootWithAuxInfo>
    {
        let result = self.commit(info, epoch_id, debug_record)?;

        debug!("Notify epoch[{}]", epoch_id);

        let mut accounts_for_txpool = vec![];
        for updated_or_deleted in &info.accounts_to_notify {
            // if the account is updated.
            if let Ok(account) = updated_or_deleted {
                accounts_for_txpool.push(account.clone());
            }
        }
        {
            // TODO: use channel to deliver the message.
            let txpool_clone = txpool.clone();
            std::thread::Builder::new()
                .name("txpool_update_state".into())
                .spawn(move || {
                    txpool_clone.notify_modified_accounts(accounts_for_txpool);
                })
                .expect("can not notify tx pool to start state");
        }

        Ok(result)
    }

    fn remove_whitelists_for_contract<AM: access_mode::AccessMode>(
        &self, info: &mut StateGenericInfo, address: &Address,
    ) -> DbResult<HashMap<Vec<u8>, Address>> {
        let mut storage_owner_map = HashMap::new();
        let key_values = self.db.delete_all(
            StorageKey::new_storage_key(
                &SPONSOR_WHITELIST_CONTROL_CONTRACT_ADDRESS,
                address.as_ref(),
            ),
            /* debug_record = */ None,
        )?;
        let mut sponsor_whitelist_control_address = self.require_exists(
            info,
            &SPONSOR_WHITELIST_CONTROL_CONTRACT_ADDRESS,
            /* require_code = */ false,
        )?;
        for (key, value) in &key_values {
            if let StorageKey::StorageKey { storage_key, .. } =
                StorageKey::from_key_bytes::<SkipInputCheck>(&key[..])
            {
                let storage_value =
                    rlp::decode::<StorageValue>(value.as_ref())?;
                let storage_owner = storage_value.owner.unwrap_or_else(|| {
                    SPONSOR_WHITELIST_CONTROL_CONTRACT_ADDRESS.clone()
                });
                storage_owner_map.insert(storage_key.to_vec(), storage_owner);
            }
        }

        // Then scan storage changes in cache.
        for (key, _value) in
            sponsor_whitelist_control_address.storage_value_write_cache()
        {
            if key.starts_with(address.as_ref()) {
                if let Some(storage_owner) =
                    sponsor_whitelist_control_address
                        .original_ownership_at(&self.db, key)?
                {
                    storage_owner_map.insert(key.clone(), storage_owner);
                } else {
                    // The corresponding entry has been reset during transaction
                    // execution, so we do not need to handle it now.
                    storage_owner_map.remove(key);
                }
            }
        }
        if !AM::is_read_only() {
            // Note removal of all keys in storage_value_read_cache and
            // storage_value_write_cache.
            for (key, _storage_owner) in &storage_owner_map {
                debug!("delete sponsor key {:?}", key);
                sponsor_whitelist_control_address.set_storage(
                    key.clone(),
                    U256::zero(),
                    /* owner doesn't matter for 0 value */
                    Address::zero(),
                );
            }
        }

        Ok(storage_owner_map)
    }

    /// Return whether or not the address exists.
    pub fn try_load(&self, address: &Address) -> DbResult<bool> {
        match self.ensure_account_loaded(address, RequireCache::None, |maybe| {
            maybe.is_some()
        }) {
            Err(e) => Err(e),
            Ok(false) => Ok(false),
            Ok(true) => {
                // Try to load the code.
                match self.ensure_account_loaded(
                    address,
                    RequireCache::Code,
                    |_| (),
                ) {
                    Ok(()) => Ok(true),
                    Err(e) => Err(e),
                }
            }
        }
    }

    // FIXME: rewrite this method before enable it for the first time, because
    //  there have been changes to kill_account and collateral processing.
    #[allow(unused)]
    pub fn kill_garbage(
        &mut self, touched: &HashSet<Address>, remove_empty_touched: bool,
        min_balance: &Option<U256>, kill_contracts: bool,
    ) -> DbResult<()>
    {
        // TODO: consider both balance and staking_balance
        let to_kill: HashSet<_> = {
            self.cache
                .get_mut()
                .iter()
                .filter_map(|(address, ref entry)| {
                    if touched.contains(address)
                        && ((remove_empty_touched
                            && entry.exists_and_is_null())
                            || (min_balance.map_or(false, |ref balance| {
                                entry.account.as_ref().map_or(false, |acc| {
                                    (acc.is_basic() || kill_contracts)
                                        && acc.balance() < balance
                                        && entry
                                            .old_balance
                                            .as_ref()
                                            .map_or(false, |b| {
                                                acc.balance() < b
                                            })
                                })
                            })))
                    {
                        Some(address.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };
        for address in to_kill {
            // TODO: The kill_garbage relies on the info in contract kill
            // process. So it is processed later than contract kill. But we do
            // not want to kill some contract again here. We must discuss it
            // before enable kill_garbage.
            unimplemented!()
        }

        Ok(())
    }

    /// Get the value of storage at a specific checkpoint.
    #[cfg(test)]
    pub fn checkpoint_storage_at(
        &self, info: &StateGenericInfo, start_checkpoint_index: usize,
        address: &Address, key: &Vec<u8>,
    ) -> DbResult<Option<U256>>
    {
        #[derive(Debug)]
        enum ReturnKind {
            OriginalAt,
            SameAsNext,
        }

        let kind = {
            let checkpoints = info.checkpoints.read();

            if start_checkpoint_index >= checkpoints.len() {
                return Ok(None);
            }

            let mut kind = None;

            for checkpoint in checkpoints.iter().skip(start_checkpoint_index) {
                match checkpoint.get(address) {
                    Some(Some(AccountEntry {
                        account: Some(ref account),
                        ..
                    })) => {
                        if let Some(value) = account.cached_storage_at(key) {
                            return Ok(Some(value));
                        } else if account.is_newly_created_contract() {
                            return Ok(Some(U256::zero()));
                        } else {
                            kind = Some(ReturnKind::OriginalAt);
                            break;
                        }
                    }
                    Some(Some(AccountEntry { account: None, .. })) => {
                        return Ok(Some(U256::zero()));
                    }
                    Some(None) => {
                        kind = Some(ReturnKind::OriginalAt);
                        break;
                    }
                    // This key does not have a checkpoint entry.
                    None => {
                        kind = Some(ReturnKind::SameAsNext);
                    }
                }
            }

            kind.expect("start_checkpoint_index is checked to be below checkpoints_len; for loop above must have been executed at least once; it will either early return, or set the kind value to Some; qed")
        };

        match kind {
            ReturnKind::SameAsNext => Ok(Some(self.storage_at(address, key)?)),
            ReturnKind::OriginalAt => {
                match self.db.get::<StorageValue>(
                    StorageKey::new_storage_key(address, key.as_ref()),
                )? {
                    Some(storage_value) => Ok(Some(storage_value.value)),
                    None => Ok(Some(U256::zero())),
                }
            }
        }
    }

    pub fn set_storage_layout(
        &mut self, info: &mut StateGenericInfo, address: &Address,
        layout: StorageLayout,
    ) -> DbResult<()>
    {
        self.require_exists(info, address, false)?
            .set_storage_layout(layout);
        Ok(())
    }

    pub fn update_cache(
        cache: &mut HashMap<Address, AccountEntry>,
        checkpoints: &mut Vec<HashMap<Address, Option<AccountEntry>>>,
        address: &Address, account: AccountEntry,
    )
    {
        let is_dirty = account.is_dirty();
        let old_value = cache.insert(*address, account);
        if is_dirty {
            if let Some(ref mut checkpoint) = checkpoints.last_mut() {
                checkpoint.entry(*address).or_insert(old_value);
            }
        }
    }

    fn insert_cache_if_fresh_account(
        cache: &mut HashMap<Address, AccountEntry>, address: &Address,
        maybe_account: Option<OverlayAccount>,
    ) -> bool
    {
        if !cache.contains_key(address) {
            cache.insert(*address, AccountEntry::new_clean(maybe_account));
            true
        } else {
            false
        }
    }

    pub fn ensure_account_loaded<F, U>(
        &self, address: &Address, require: RequireCache, f: F,
    ) -> DbResult<U>
    where F: Fn(Option<&OverlayAccount>) -> U {
        // Return immediately when there is no need to have db operation.
        if let Some(maybe_acc) = self.cache.read().get(address) {
            if let Some(account) = &maybe_acc.account {
                let needs_update = Self::needs_update(require, account);
                if !needs_update {
                    return Ok(f(Some(account)));
                }
            } else {
                return Ok(f(None));
            }
        }

        let mut cache_write_lock = {
            let upgradable_lock = self.cache.upgradable_read();
            if upgradable_lock.contains_key(address) {
                // TODO: the account can be updated here if the relevant methods
                //  to update account can run with &OverlayAccount.
                RwLockUpgradableReadGuard::upgrade(upgradable_lock)
            } else {
                // Load the account from db.
                let mut maybe_loaded_acc = self
                    .db
                    .get_account(address)?
                    .map(|acc| OverlayAccount::from_loaded(address, acc));
                if let Some(account) = &mut maybe_loaded_acc {
                    Self::update_account_cache(require, account, &self.db)?;
                }
                let mut cache_write_lock =
                    RwLockUpgradableReadGuard::upgrade(upgradable_lock);
                Self::insert_cache_if_fresh_account(
                    &mut *cache_write_lock,
                    address,
                    maybe_loaded_acc,
                );

                cache_write_lock
            }
        };

        let cache = &mut *cache_write_lock;
        let account = cache.get_mut(address).unwrap();
        if let Some(maybe_acc) = &mut account.account {
            if !Self::update_account_cache(require, maybe_acc, &self.db)? {
                return Err(DbErrorKind::IncompleteDatabase(
                    maybe_acc.address().clone(),
                )
                .into());
            }
        }

        Ok(f(cache
            .get(address)
            .and_then(|entry| entry.account.as_ref())))
    }

    fn require_exists(
        &self, info: &mut StateGenericInfo, address: &Address,
        require_code: bool,
    ) -> DbResult<MappedRwLockWriteGuard<OverlayAccount>>
    {
        fn no_account_is_an_error(
            address: &Address,
        ) -> DbResult<OverlayAccount> {
            bail!(DbErrorKind::IncompleteDatabase(*address));
        }
        self.require_or_set(info, address, require_code, no_account_is_an_error)
    }

    fn require_or_new_basic_account(
        &self, info: &mut StateGenericInfo, address: &Address,
        account_start_nonce: &U256,
    ) -> DbResult<MappedRwLockWriteGuard<OverlayAccount>>
    {
        self.require_or_set(info, address, false, |address| {
            if address.is_valid_address() {
                // Note that it is possible to first send money to a pre-calculated contract
                // address and then deploy contracts. So we are going to *allow* sending to a contract
                // address and use new_basic() to create a *stub* there. Because the contract serialization
                // is a super-set of the normal address serialization, this should just work.
                Ok(OverlayAccount::new_basic(
                    address,
                    U256::zero(),
                    account_start_nonce.into(),
                    None,
                ))
            } else {
                unreachable!(
                    "address does not already exist and is not an user account. {:?}",
                    address
                )
            }
        })
    }

    fn require_or_set<F>(
        &self, info: &mut StateGenericInfo, address: &Address,
        require_code: bool, default: F,
    ) -> DbResult<MappedRwLockWriteGuard<OverlayAccount>>
    where
        F: FnOnce(&Address) -> DbResult<OverlayAccount>,
    {
        let mut cache;
        if !self.cache.read().contains_key(address) {
            let account = self
                .db
                .get_account(address)?
                .map(|acc| OverlayAccount::from_loaded(address, acc));
            cache = self.cache.write();
            Self::insert_cache_if_fresh_account(&mut *cache, address, account);
        } else {
            cache = self.cache.write();
        };

        // Save the value before modification into the checkpoint.
        if let Some(ref mut checkpoint) = info.checkpoints.write().last_mut() {
            checkpoint.entry(*address).or_insert_with(|| {
                cache.get(address).map(AccountEntry::clone_dirty)
            });
        }

        let entry = (*cache)
            .get_mut(address)
            .expect("entry known to exist in the cache");

        // Set the dirty flag.
        entry.state = AccountState::Dirty;

        if entry.account.is_none() {
            entry.account = Some(default(address)?);
        }

        if require_code {
            if !Self::update_account_cache(
                RequireCache::Code,
                entry
                    .account
                    .as_mut()
                    .expect("Required account must exist."),
                &self.db,
            )? {
                bail!(DbErrorKind::IncompleteDatabase(*address));
            }
        }

        Ok(RwLockWriteGuard::map(cache, |c| {
            c.get_mut(address)
                .expect("Entry known to exist in the cache.")
                .account
                .as_mut()
                .expect("Required account must exist.")
        }))
    }

    #[cfg(any(test, feature = "testonly_code"))]
    pub fn clear(&mut self, info: &mut StateGenericInfo) {
        assert!(info.checkpoints.get_mut().is_empty());
        assert!(info.staking_state_checkpoints.get_mut().is_empty());
        self.cache.get_mut().clear();
        info.staking_state.interest_rate_per_block =
            self.db.get_annual_interest_rate().expect("no db error")
                / U256::from(BLOCKS_PER_YEAR);
        info.staking_state.accumulate_interest_rate =
            self.db.get_accumulate_interest_rate().expect("no db error");
        info.staking_state.total_issued_tokens =
            self.db.get_total_issued_tokens().expect("no db error");
        info.staking_state.total_staking_tokens =
            self.db.get_total_staking_tokens().expect("no db error");
        info.staking_state.total_storage_tokens =
            self.db.get_total_storage_tokens().expect("no db error");
    }
}

impl<StateDbStorage: StorageStateTrait> StateGeneric<StateDbStorage> {
    #[cfg(test)]
    pub fn new_contract(
        &mut self, contract: &Address, balance: U256, nonce: U256,
    ) -> DbResult<()> {
        self.io
            .new_contract(&mut self.info, contract, balance, nonce)
    }

    #[cfg(test)]
    pub fn add_collateral_for_storage(
        &mut self, address: &Address, by: &U256,
    ) -> DbResult<()> {
        self.io
            .add_collateral_for_storage(&mut self.info, address, by)
    }

    #[cfg(test)]
    pub fn sub_collateral_for_storage(
        &mut self, address: &Address, by: &U256, account_start_nonce: U256,
    ) -> DbResult<()> {
        self.io.sub_collateral_for_storage(
            &mut self.info,
            address,
            by,
            account_start_nonce,
        )
    }

    #[allow(dead_code)]
    pub fn touch(
        &mut self, info: &mut StateGenericInfo, address: &Address,
    ) -> DbResult<()> {
        self.io.touch(info, address)
    }

    /// Assume that only contract with zero `collateral_for_storage` will be
    /// killed.
    pub fn recycle_storage(
        &mut self, killed_addresses: Vec<Address>,
        debug_record: Option<&mut ComputeEpochDebugRecord>,
    ) -> DbResult<()>
    {
        self.io.recycle_storage(killed_addresses, debug_record)
    }

    // FIXME: this should be part of the statetrait however transaction pool
    // creates circular dep.  if it proves impossible to break the loop we
    // use associated types for the tx pool.
    pub fn commit_and_notify(
        &mut self, epoch_id: EpochId, txpool: &SharedTransactionPool,
        debug_record: Option<&mut ComputeEpochDebugRecord>,
    ) -> DbResult<StateRootWithAuxInfo>
    {
        self.io.commit_and_notify(
            &mut self.info,
            epoch_id,
            txpool,
            debug_record,
        )
    }

    /// Return whether or not the address exists.
    pub fn try_load(&self, address: &Address) -> DbResult<bool> {
        self.io.try_load(address)
    }

    // FIXME: rewrite this method before enable it for the first time, because
    //  there have been changes to kill_account and collateral processing.
    #[allow(unused)]
    pub fn kill_garbage(
        &mut self, touched: &HashSet<Address>, remove_empty_touched: bool,
        min_balance: &Option<U256>, kill_contracts: bool,
    ) -> DbResult<()>
    {
        self.io.kill_garbage(
            touched,
            remove_empty_touched,
            min_balance,
            kill_contracts,
        )
    }

    /// Get the value of storage at a specific checkpoint.
    #[cfg(test)]
    pub fn checkpoint_storage_at(
        &self, start_checkpoint_index: usize, address: &Address, key: &Vec<u8>,
    ) -> DbResult<Option<U256>> {
        self.io.checkpoint_storage_at(
            &self.info,
            start_checkpoint_index,
            address,
            key,
        )
    }

    pub fn set_storage_layout(
        &mut self, address: &Address, layout: StorageLayout,
    ) -> DbResult<()> {
        self.io.set_storage_layout(&mut self.info, address, layout)
    }

    pub fn ensure_account_loaded<F, U>(
        &self, address: &Address, require: RequireCache, f: F,
    ) -> DbResult<U>
    where F: Fn(Option<&OverlayAccount>) -> U {
        self.io.ensure_account_loaded(address, require, f)
    }

    #[cfg(any(test, feature = "testonly_code"))]
    pub fn clear(&mut self) { self.io.clear(&mut self.info) }
}

impl<StateDbStorage: StorageStateTrait> StateOpsTxTrait
    for StateGeneric<StateDbStorage>
{
    fn bump_block_number_accumulate_interest(&mut self) -> U256 {
        self.info.bump_block_number_accumulate_interest()
    }

    fn add_total_issued(&mut self, v: U256) {
        self.info.add_total_issued(v)
    }

    fn subtract_total_issued(&mut self, v: U256) {
        self.info.subtract_total_issued(v)
    }

    fn new_contract_with_admin(
        &mut self, contract: &Address, admin: &Address, balance: U256,
        nonce: U256, storage_layout: Option<StorageLayout>,
    ) -> DbResult<()>
    {
        self.io.new_contract_with_admin(
            &mut self.info,
            contract,
            admin,
            balance,
            nonce,
            storage_layout,
        )
    }

    fn balance(&self, address: &Address) -> DbResult<U256> {
        self.io.balance(address)
    }

    fn is_contract_with_code(&self, address: &Address) -> DbResult<bool> {
        self.io.is_contract_with_code(address)
    }

    fn sponsor_for_gas(&self, address: &Address) -> DbResult<Option<Address>> {
        self.io.sponsor_for_gas(address)
    }

    fn sponsor_for_collateral(
        &self, address: &Address,
    ) -> DbResult<Option<Address>> {
        self.io.sponsor_for_collateral(address)
    }

    fn set_sponsor_for_gas(
        &mut self, address: &Address, sponsor: &Address,
        sponsor_balance: &U256, upper_bound: &U256,
    ) -> DbResult<()>
    {
        self.io.set_sponsor_for_gas(
            &mut self.info,
            address,
            sponsor,
            sponsor_balance,
            upper_bound,
        )
    }

    fn set_sponsor_for_collateral(
        &mut self, address: &Address, sponsor: &Address, sponsor_balance: &U256,
    ) -> DbResult<()> {
        self.io.set_sponsor_for_collateral(
            &mut self.info,
            address,
            sponsor,
            sponsor_balance,
        )
    }

    fn sponsor_gas_bound(&self, address: &Address) -> DbResult<U256> {
        self.io.sponsor_gas_bound(address)
    }

    fn sponsor_balance_for_gas(&self, address: &Address) -> DbResult<U256> {
        self.io.sponsor_balance_for_gas(address)
    }

    fn sponsor_balance_for_collateral(
        &self, address: &Address,
    ) -> DbResult<U256> {
        self.io.sponsor_balance_for_collateral(address)
    }

    fn set_admin(
        &mut self, contract_address: &Address, admin: &Address,
    ) -> DbResult<()> {
        self.io.set_admin(&mut self.info, contract_address, admin)
    }

    fn sub_sponsor_balance_for_gas(
        &mut self, address: &Address, by: &U256,
    ) -> DbResult<()> {
        self.io
            .sub_sponsor_balance_for_gas(&mut self.info, address, by)
    }

    fn add_sponsor_balance_for_gas(
        &mut self, address: &Address, by: &U256,
    ) -> DbResult<()> {
        self.io
            .add_sponsor_balance_for_gas(&mut self.info, address, by)
    }

    fn sub_sponsor_balance_for_collateral(
        &mut self, address: &Address, by: &U256,
    ) -> DbResult<()> {
        self.io
            .sub_sponsor_balance_for_collateral(&mut self.info, address, by)
    }

    fn add_sponsor_balance_for_collateral(
        &mut self, address: &Address, by: &U256,
    ) -> DbResult<()> {
        self.io
            .add_sponsor_balance_for_collateral(&mut self.info, address, by)
    }

    fn check_commission_privilege(
        &self, contract_address: &Address, user: &Address,
    ) -> DbResult<bool> {
        self.io.check_commission_privilege(contract_address, user)
    }

    fn add_commission_privilege(
        &mut self, contract_address: Address, contract_owner: Address,
        user: Address,
    ) -> DbResult<()>
    {
        self.io.add_commission_privilege(
            &mut self.info,
            contract_address,
            contract_owner,
            user,
        )
    }

    fn remove_commission_privilege(
        &mut self, contract_address: Address, contract_owner: Address,
        user: Address,
    ) -> DbResult<()>
    {
        self.io.remove_commission_privilege(
            &mut self.info,
            contract_address,
            contract_owner,
            user,
        )
    }

    fn nonce(&self, address: &Address) -> DbResult<U256> {
        self.io.nonce(address)
    }

    fn init_code(
        &mut self, address: &Address, code: Vec<u8>, owner: Address,
    ) -> DbResult<()> {
        self.io.init_code(&mut self.info, address, code, owner)
    }

    fn code_hash(&self, address: &Address) -> DbResult<Option<H256>> {
        self.io.code_hash(address)
    }

    fn code_size(&self, address: &Address) -> DbResult<Option<usize>> {
        self.io.code_size(address)
    }

    fn code_owner(&self, address: &Address) -> DbResult<Option<Address>> {
        self.io.code_owner(address)
    }

    fn code(&self, address: &Address) -> DbResult<Option<Arc<Vec<u8>>>> {
        self.io.code(address)
    }

    fn staking_balance(&self, address: &Address) -> DbResult<U256> {
        self.io.staking_balance(address)
    }

    fn collateral_for_storage(&self, address: &Address) -> DbResult<U256> {
        self.io.collateral_for_storage(address)
    }

    fn admin(&self, address: &Address) -> DbResult<Address> {
        self.io.admin(address)
    }

    fn withdrawable_staking_balance(
        &self, address: &Address, current_block_number: u64,
    ) -> DbResult<U256> {
        self.io
            .withdrawable_staking_balance(address, current_block_number)
    }

    fn locked_staking_balance_at_block_number(
        &self, address: &Address, block_number: u64,
    ) -> DbResult<U256> {
        self.io
            .locked_staking_balance_at_block_number(address, block_number)
    }

    fn deposit_list_length(&self, address: &Address) -> DbResult<usize> {
        self.io.deposit_list_length(address)
    }

    fn vote_stake_list_length(&self, address: &Address) -> DbResult<usize> {
        self.io.vote_stake_list_length(address)
    }

    fn inc_nonce(
        &mut self, address: &Address, account_start_nonce: &U256,
    ) -> DbResult<()> {
        self.io
            .inc_nonce(&mut self.info, address, account_start_nonce)
    }

    fn set_nonce(&mut self, address: &Address, nonce: &U256) -> DbResult<()> {
        self.io.set_nonce(&mut self.info, address, nonce)
    }

    fn sub_balance(
        &mut self, address: &Address, by: &U256, cleanup_mode: &mut CleanupMode,
    ) -> DbResult<()> {
        self.io
            .sub_balance(&mut self.info, address, by, cleanup_mode)
    }

    fn add_balance(
        &mut self, address: &Address, by: &U256, cleanup_mode: CleanupMode,
        account_start_nonce: U256,
    ) -> DbResult<()>
    {
        self.io.add_balance(
            &mut self.info,
            address,
            by,
            cleanup_mode,
            account_start_nonce,
        )
    }

    fn transfer_balance(
        &mut self, from: &Address, to: &Address, by: &U256,
        cleanup_mode: CleanupMode, account_start_nonce: U256,
    ) -> DbResult<()>
    {
        self.io.transfer_balance(
            &mut self.info,
            from,
            to,
            by,
            cleanup_mode,
            account_start_nonce,
        )
    }

    fn deposit(
        &mut self, address: &Address, amount: &U256, current_block_number: u64,
    ) -> DbResult<()> {
        self.io
            .deposit(&mut self.info, address, amount, current_block_number)
    }

    fn withdraw(&mut self, address: &Address, amount: &U256) -> DbResult<U256> {
        self.io.withdraw(&mut self.info, address, amount)
    }

    fn vote_lock(
        &mut self, address: &Address, amount: &U256, unlock_block_number: u64,
    ) -> DbResult<()> {
        self.io
            .vote_lock(&mut self.info, address, amount, unlock_block_number)
    }

    fn remove_expired_vote_stake_info(
        &mut self, address: &Address, current_block_number: u64,
    ) -> DbResult<()> {
        self.io.remove_expired_vote_stake_info(
            &mut self.info,
            address,
            current_block_number,
        )
    }

    fn remove_contract(&mut self, address: &Address) -> DbResult<()> {
        self.io.remove_contract(&mut self.info, address)
    }

    fn exists(&self, address: &Address) -> DbResult<bool> {
        self.io.exists(address)
    }

    fn exists_and_not_null(&self, address: &Address) -> DbResult<bool> {
        self.io.exists_and_not_null(address)
    }

    fn storage_at(&self, address: &Address, key: &[u8]) -> DbResult<U256> {
        self.io.storage_at(address, key)
    }

    fn set_storage(
        &mut self, address: &Address, key: Vec<u8>, value: U256, owner: Address,
    ) -> DbResult<()> {
        self.io
            .set_storage(&mut self.info, address, key, value, owner)
    }
}

impl<StateDbStorage: StorageStateTrait> CheckpointTxDeltaTrait
    for StateGeneric<StateDbStorage>
{
    fn checkpoint(&mut self) -> usize { self.info.checkpoint() }

    fn discard_checkpoint(&mut self) { self.info.discard_checkpoint() }

    fn revert_to_checkpoint(&mut self) {
        self.io.revert_to_checkpoint(&mut self.info)
    }
}

impl<StateDbStorage: StorageStateTrait> CheckpointTxTrait
    for StateGeneric<StateDbStorage>
{
}

impl<StateDbStorage: StorageStateTrait> StateTxDeltaTrait
    for StateGeneric<StateDbStorage>
{
    type Substate = Substate;

    fn collect_ownership_changed(
        &mut self, substate: &mut Self::Substate,
    ) -> DbResult<()> {
        self.io.collect_ownership_changed(&mut self.info, substate)
    }

    fn settle_collateral_for_all(
        &mut self, substate: &Self::Substate, account_start_nonce: U256,
    ) -> DbResult<CollateralCheckResult> {
        self.io.settle_collateral_for_all(
            &mut self.info,
            substate,
            account_start_nonce,
        )
    }

    fn collect_and_settle_collateral(
        &mut self, original_sender: &Address, storage_limit: &U256,
        substate: &mut Substate, account_start_nonce: U256,
    ) -> DbResult<CollateralCheckResult>
    {
        self.io.collect_and_settle_collateral(
            &mut self.info,
            original_sender,
            storage_limit,
            substate,
            account_start_nonce,
        )
    }

    fn record_storage_and_whitelist_entries_release(
        &mut self, address: &Address, substate: &mut Self::Substate,
    ) -> DbResult<()> {
        self.io.record_storage_and_whitelist_entries_release(
            &mut self.info,
            address,
            substate,
        )
    }
}

impl<StateDbStorage: StorageStateTrait> StateTxTrait
    for StateGeneric<StateDbStorage>
{
}

impl<StateDbStorage: StorageStateTrait> StateOpsTrait
    for StateGeneric<StateDbStorage>
{
    fn sponsor_info(&self, address: &Address) -> DbResult<Option<SponsorInfo>> {
        self.io.sponsor_info(address)
    }

    fn clean_account(&mut self, address: &Address) -> DbResult<()> {
        self.io.clean_account(&mut self.info, address)
    }

    fn total_issued_tokens(&self) -> U256 { self.info.total_issued_tokens() }

    fn total_staking_tokens(&self) -> U256 { self.info.total_staking_tokens() }

    fn total_storage_tokens(&self) -> U256 { self.info.total_storage_tokens() }
}

impl<StateDbStorage: StorageStateTrait> CheckpointTrait
    for StateGeneric<StateDbStorage>
{
}

impl<StateDbStorage: StorageStateTrait> StateTrait
    for StateGeneric<StateDbStorage>
{
    fn compute_state_root(
        &mut self, debug_record: Option<&mut ComputeEpochDebugRecord>,
    ) -> DbResult<StateRootWithAuxInfo> {
        self.io.compute_state_root(&mut self.info, debug_record)
    }

    fn commit(
        &mut self, epoch_id: EpochId,
        debug_record: Option<&mut ComputeEpochDebugRecord>,
    ) -> DbResult<StateRootWithAuxInfo>
    {
        self.io.commit(&mut self.info, epoch_id, debug_record)
    }
}

/// Methods that are intentionally kept private because the fields may not have
/// been loaded from db.
trait AccountEntryProtectedMethods {
    fn deposit_list(&self) -> Option<&DepositList>;
    fn vote_stake_list(&self) -> Option<&VoteStakeList>;
    fn code_size(&self) -> Option<usize>;
    fn code(&self) -> Option<Arc<Bytes>>;
    fn code_owner(&self) -> Option<Address>;
}
