use std::sync::Arc;
use std::collections::{HashSet, VecDeque};
use std::fs::File;
use std::io::{Write, BufWriter};

use clap::{crate_version, load_yaml, App};
use primitives::receipt::TRANSACTION_OUTCOME_EXCEPTION_WITHOUT_NONCE_BUMPING;
use serde::Serialize;

use primitives::{Action, SignedTransaction};
use cfxcore::{
    block_data_manager::DbType,
    vm_factory::VmFactory,
    sync::utils::initialize_data_manager,
    pow::PowComputer,
    vm::{Env},
};
use cfx_types::{H256, U256, Address};
use client::configuration::Configuration;

extern crate serde_json;

#[derive(Serialize)]
struct BlockTrace {
    transactions: Vec<Arc<SignedTransaction>>,
    env: Env,
}
#[derive(Serialize)]
struct EpochTrace {
    epoch_hash: H256,
    block_traces: Vec<BlockTrace>,
    rewards: Vec<(Address, U256)>,
    new_mint: U256,
    burnt_fee: U256,
}

fn main() {
    let yaml = load_yaml!("../../src/cli.yaml");
    let matches = App::from_yaml(yaml).version(crate_version!()).get_matches();
    let conf = Configuration::parse(&matches).unwrap();

    let trace_file = File::create("trace.txt").unwrap();
    let address_file = File::create("address.txt").unwrap();
    let info_file = File::create("info.txt").unwrap();
    let mut trace_writer = BufWriter::new(&trace_file);
    let mut address_writer = BufWriter::new(&address_file);
    let mut info_writer = BufWriter::new(&info_file);
    let mut addresses = HashSet::new();
    let mut rewards_queue = VecDeque::new();

    let mut cnts = vec![0; 3];

    let vm = VmFactory::new(1024 * 32);
    let pow = Arc::new(PowComputer::new(true));
    let (data_man, _genesis_block) =
        initialize_data_manager("./blockchain_data/blockchain_db",
            DbType::Rocksdb, pow, vm);
    let mut epoch = data_man.earliest_epoch_with_block_body();
    if epoch == 0 {
        epoch = 1;
    }
    let epoch_begin = epoch;
    'epoch_loop: while let Some(epoch_later_hashes) = data_man
        .executed_epoch_set_hashes_from_db(epoch + 12)
    {
        let epoch_later = epoch_later_hashes.last().unwrap();
        let hashes = data_man.executed_epoch_set_hashes_from_db(epoch).unwrap();
        let pivot_hash = hashes.last().unwrap().clone();
        let pivot_block = data_man.block_by_hash(&pivot_hash, false).unwrap();
        let ctx = data_man.get_epoch_execution_context(&pivot_hash).unwrap();
        let mut last_block_hash = pivot_block.block_header.parent_hash().clone();
        let mut block_trace = Vec::new();
        let mut rewards = Vec::new();
        let mut block_number = ctx.start_block_number;
        for hash in hashes {
            let block = data_man.block_by_hash(&hash, false).unwrap();
            let res = data_man
                .block_execution_result_by_hash_from_db(&hash)
                .unwrap();
            let receipts = &res
                .1
                .block_receipts
                .as_ref()
                .receipts;
            assert!(block.transactions.len() == receipts.len());
            let mut transactions = Vec::new();
            for (transaction, receipt) in block.transactions.iter().zip(receipts.iter()) {
                match transaction.as_ref().transaction.transaction.unsigned.action {
                    Action::Create => {
                    }
                    Action::Call(address) => {
                        addresses.insert(address);
                    }
                }
                cnts[receipt.outcome_status as usize] += 1;
                if receipt.outcome_status != TRANSACTION_OUTCOME_EXCEPTION_WITHOUT_NONCE_BUMPING {
                    transactions.push(transaction.clone());
                }
            }
            block_trace.push(BlockTrace {
                transactions,
                env: Env {
                    number: block_number,
                    author: block.block_header.author().clone(),
                    timestamp: pivot_block.block_header.timestamp(),
                    difficulty: block.block_header.difficulty().clone(),
                    gas_limit: block.block_header.gas_limit().clone(),
                    last_hash: last_block_hash,
                    accumulated_gas_used: U256::zero(), // No use here
                    epoch_height: pivot_block.block_header.height(),
                    transaction_epoch_bound: conf.raw_conf.transaction_epoch_bound,
                },
            });
            let reward = match data_man
                .block_reward_result_by_hash_with_epoch(
                    &hash, &epoch_later, false, false)
            {
                None => { break 'epoch_loop }
                Some(reward) => { reward }
            };
            let author = block.block_header.author();
            rewards.push((author.clone(), reward));
            addresses.insert(author.clone());
            last_block_hash = hash;
            block_number += 1;
        }
        let epoch_rewards = if rewards_queue.len() == 12 {
                rewards_queue.pop_front().unwrap()
            } else {
                Vec::new()
            };
        rewards_queue.push_back(rewards);
        let mut rewards = Vec::new();
        let mut new_mint = U256::from(0);
        for (author, reward) in epoch_rewards {
            rewards.push((author, reward.total_reward));
            new_mint += reward.total_reward - reward.tx_fee;
        }
        let burnt_fee = data_man.db_manager.epoch_reward_result_from_db(
            &epoch_later,
        ).unwrap().burnt_fee;
        let epoch_trace = EpochTrace {
            epoch_hash: pivot_hash,
            block_traces: block_trace,
            rewards,
            new_mint,
            burnt_fee,
        };
        writeln!(trace_writer, "{}",
            serde_json::to_string(&epoch_trace)
                .expect("Can not stringify the transaction!"))
        .unwrap();
        epoch += 1;
    }
    for address in addresses.iter() {
        writeln!(address_writer, "{:x}", address).unwrap();
    }
    eprintln!("Epoch {} to {} exported.", epoch_begin, epoch - 1);
    writeln!(info_writer, "{}", epoch - 1).unwrap();
    println!("{} {} {}", cnts[0], cnts[1], cnts[2]);
    writeln!(info_writer, "{} {} {}", cnts[0], cnts[1], cnts[2]).unwrap();
}
