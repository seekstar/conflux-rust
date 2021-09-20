use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    process::{self, Command},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use clap::{App, Arg};
use ctrlc::CtrlC;
use parking_lot::{Mutex, MutexGuard};
use rustc_hex::FromHex;
use serde::Deserialize;
use threadpool::ThreadPool;

use cfx_parameters::{
    consensus::{GENESIS_GAS_LIMIT, ONE_CFX_IN_DRIP},
    consensus_internal::{
        GENESIS_TOKEN_COUNT_IN_CFX, TWO_YEAR_UNLOCK_TOKEN_COUNT_IN_CFX,
    },
};
use cfx_state::{
    state_trait::{StateOpsTrait, StateTrait},
    CleanupMode,
};
use cfx_statedb::StateDb;
use cfx_storage::{StateIndex, StorageManager, StorageManagerTrait};
use cfx_types::{address_util::AddressUtil, Address, H256, U256};
use cfxcore::{
    executive::{
        contract_address, ExecutionOutcome, Executive, TransactOptions,
    },
    machine::{new_machine_with_builtin, Machine},
    pow::PowComputer,
    spec::genesis::{
        execute_genesis_transaction, initialize_internal_contract_accounts,
        GENESIS_ACCOUNT_ADDRESS_STR, GENESIS_TRANSACTION_CREATE_CREATE2FACTORY,
        GENESIS_TRANSACTION_CREATE_FUND_POOL,
        GENESIS_TRANSACTION_CREATE_GENESIS_TOKEN_MANAGER_FOUR_YEAR_UNLOCK,
        GENESIS_TRANSACTION_CREATE_GENESIS_TOKEN_MANAGER_TWO_YEAR_UNLOCK,
        GENESIS_TRANSACTION_DATA_STR,
    },
    state::State,
    verification::{compute_receipts_root, compute_transaction_root},
    vm::{CreateContractAddress, Env},
    vm_factory::VmFactory,
    BlockDataManager, WORKER_COMPUTATION_PARALLELISM,
};
use primitives::{
    Action, Block, BlockHeaderBuilder, BlockReceipts, SignedTransaction,
    Transaction,
};

use client::configuration::Configuration;

const GENESIS_VERSION: &str = "1949000000000000000000000000000000001001";

extern crate serde_json;

#[derive(Deserialize)]
struct BlockTrace {
    transactions: Vec<SignedTransaction>,
    env: Env,
}
#[allow(dead_code)]
#[derive(Deserialize)]
struct EpochTrace {
    epoch_hash: H256,
    block_traces: Vec<BlockTrace>,
    rewards: Vec<(Address, U256)>,
    new_mint: U256,
    burnt_fee: U256,
}

struct PerfExecInfo {
    transaction_executed_total: usize,
    old_height: u64,
    new_height: u64,
}

fn main() {
    let arg_matches = App::new("Conflux transaction replayer")
        .arg(
            Arg::with_name("data-dir")
                .short("d")
                .long("--data-dir")
                .value_name("DIR")
                .help("The directory which stores trace.txt and mined.txt")
                .takes_value(true)
                .default_value("."),
        )
        .arg(
            Arg::with_name("commit-interval")
                .short("i")
                .long("commit-interval")
                .value_name("commit-interval")
                .help("Commit every <commit-interval> transactions")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("perf-log")
                .long("perf-log")
                .value_name("perf-log")
                .help("Path to save perf log")
                .takes_value(true)
                .default_value("perf-log.txt"),
        )
        .arg(
            Arg::with_name("start-epoch")
                .long("start-epoch")
                .value_name("start-epoch")
                .help(
                    "The epoch number before the first epoch number to replay",
                )
                .takes_value(true),
        )
        .arg(
            Arg::with_name("epoch-to-execute")
                .long("epoch-to-execute")
                .value_name("epoch-to-execute")
                .help("The number of epochs to execute")
                .takes_value(true),
        )
        .get_matches();
    let mut conf = Configuration::parse(&arg_matches).unwrap();
    conf.raw_conf.chain_id = Some(1029);
    let dir = arg_matches.value_of("data-dir").unwrap();
    let commit_interval = arg_matches
        .value_of("commit-interval")
        .map(|x| x.parse::<usize>().unwrap());
    let perf_log_path = arg_matches.value_of("perf-log").unwrap();
    let trace_path = format!("{}/trace.txt", dir);
    let address_path = format!("{}/address.txt", dir);
    let trace_file = File::open(trace_path).unwrap();
    let trace_reader = BufReader::new(trace_file);
    let perf_log_file = File::create(perf_log_path).unwrap();
    let perf_log_writer = Arc::new(Mutex::new(BufWriter::new(perf_log_file)));

    let addresses = read_addresses(&address_path);

    let mut transaction_executed: usize = 0;
    let mut transact_time = Duration::from_secs(0);
    let mut commit_time = Duration::from_secs(0);

    let mut height;
    let mut epoch_hash;
    let (data_man_ori, _, _, _) = open_db(&conf);
    conf.raw_conf.conflux_data_dir = "./replay_data".into();
    let (data_man_replay, mut state, machine) = if let Some(start_epoch) =
        arg_matches.value_of("start-epoch")
    {
        let (data_man_replay, _, _, machine) = open_db(&conf);
        height = start_epoch.parse().unwrap();
        epoch_hash = data_man_replay
            .executed_epoch_set_hashes_from_db(height)
            .unwrap()
            .last()
            .unwrap()
            .clone();
        let state = next_state_from_db(&data_man_replay, &epoch_hash, height);
        (data_man_replay, state, machine)
    } else {
        remove_dir_content(&conf.raw_conf.conflux_data_dir);
        let (data_man_replay, _, genesis_block, machine) = open_db(&conf);
        height = 0;
        epoch_hash = genesis_block.hash();
        let state = next_state(&data_man_replay, &epoch_hash, height);
        (data_man_replay, state, machine)
    };
    let mut last_committed_height = height;
    height += 1;
    let mut new_height = height;

    let epoch_to_execute = Arc::new(AtomicU64::new(
        match arg_matches.value_of("epoch-to-execute") {
            Some(s) => s.parse().unwrap(),
            None => u64::MAX,
        },
    ));

    let mut not_executed_drop_cnt = 0;
    let mut not_executed_to_reconsider_packing_cnt = 0;
    let mut execution_error_bump_nonce_cnt = 0;
    let mut finished_cnt = 0;

    const DELETE_COMMITMENT_DELAY: usize = 100000;
    let mut prev_epoches = VecDeque::with_capacity(DELETE_COMMITMENT_DELAY);

    let mut trace_lines = trace_reader.lines().peekable();
    if let Some(Ok(line)) = trace_lines.peek() {
        let epoch_trace = serde_json::from_str::<EpochTrace>(&line).unwrap();
        let mut h = epoch_trace.block_traces.last().unwrap().env.epoch_height;
        if h > height {
            eprintln!("Incomplete trace: Trace of epoch {} not found", height);
            return;
        }
        if h < height {
            println!("Skipping {} epoches in trace", height - h);
            while h < height {
                trace_lines.next();
                h += 1;
            }
        }
    }

    let perf_exec_info = Arc::new(Mutex::new(PerfExecInfo {
        transaction_executed_total: 0,
        old_height: height,
        new_height,
    }));
    let perf_info_printer_canceller = Arc::new(AtomicBool::new(false));
    let perf_info_printer_canceller_cloned =
        perf_info_printer_canceller.clone();
    let perf_log_writer_cloned = perf_log_writer.clone();
    let perf_exec_info_cloned = perf_exec_info.clone();
    let perf_info_printer = std::thread::spawn(move || {
        print_perf_info(
            perf_info_printer_canceller_cloned,
            perf_log_writer_cloned,
            perf_exec_info_cloned,
        )
    });

    fn handle_epoch(
        state: &mut State, data_man_replay: &BlockDataManager,
        epoch_hash: &mut H256, commit_time: &mut Duration, height: u64,
        new_height: &mut u64, last_committed_height: &mut u64,
        prev_epoches: &mut VecDeque<H256>,
        perf_exec_info: &Arc<Mutex<PerfExecInfo>>,
    )
    {
        *epoch_hash = H256::random();
        commit_state(state, data_man_replay, epoch_hash, commit_time);
        *state = next_state(data_man_replay, epoch_hash, *new_height);
        if prev_epoches.len() == DELETE_COMMITMENT_DELAY {
            data_man_replay.remove_epoch_execution_commitment(
                &prev_epoches.pop_front().unwrap(),
            );
        }
        prev_epoches.push_back(*epoch_hash);
        data_man_replay.insert_executed_epoch_set_hashes_to_db(
            *new_height,
            &vec![*epoch_hash],
        );
        *last_committed_height = height;
        // if check_state(&data_man_replay, &epoch_hash, &data_man_ori, height,
        // &addresses) {     return;
        // }
        if *new_height % 2000 == 0 {
            let keep = 2000 * 50;
            if *new_height > keep {
                cleanup_snapshots(data_man_replay, *new_height - keep);
            }
        }
        *new_height += 1;
        perf_exec_info.lock().new_height += 1;
    }

    let epoch_to_execute_cloned = epoch_to_execute.clone();
    CtrlC::set_handler(move || {
        let v = epoch_to_execute_cloned.as_ref();
        v.store(0, Ordering::Relaxed);
    });
    for line in trace_lines {
        let line = line.unwrap();
        if line.len() == 0 {
            break;
        }
        if let Err(0) = epoch_to_execute.as_ref().fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |x| {
                if x == 0 || x == u64::MAX {
                    None
                } else {
                    Some(x - 1)
                }
            },
        ) {
            break;
        }
        let epoch_trace = serde_json::from_str::<EpochTrace>(&line).unwrap();
        for block_trace in epoch_trace.block_traces {
            let spec = machine.spec(block_trace.env.number);
            state.bump_block_number_accumulate_interest();
            initialize_internal_contract_accounts(
                &mut state,
                machine
                    .internal_contracts()
                    .initialized_at(block_trace.env.number),
                spec.contract_start_nonce,
            );
            for transaction in block_trace.transactions {
                let now = Instant::now();
                let exe_res = Executive::new(
                    &mut state,
                    &block_trace.env,
                    machine.as_ref(),
                    &spec,
                )
                .transact(&transaction, TransactOptions::with_no_tracing());
                transact_time += now.elapsed();
                let exe_res = exe_res.unwrap();
                match exe_res {
                    ExecutionOutcome::NotExecutedDrop(_) => {
                        not_executed_drop_cnt += 1;
                    }
                    ExecutionOutcome::NotExecutedToReconsiderPacking(_) => {
                        not_executed_to_reconsider_packing_cnt += 1;
                    }
                    ExecutionOutcome::ExecutionErrorBumpNonce(_, _) => {
                        execution_error_bump_nonce_cnt += 1;
                    }
                    ExecutionOutcome::Finished(_) => {
                        finished_cnt += 1;
                    }
                }
                perf_exec_info.lock().transaction_executed_total += 1;
                if let Some(commit_interval) = commit_interval {
                    transaction_executed += 1;
                    if transaction_executed == commit_interval {
                        transaction_executed = 0;
                        handle_epoch(
                            &mut state,
                            &data_man_replay,
                            &mut epoch_hash,
                            &mut commit_time,
                            height,
                            &mut new_height,
                            &mut last_committed_height,
                            &mut prev_epoches,
                            &perf_exec_info,
                        );
                    }
                }
            }
        }
        let mut merged_rewards = HashMap::new();
        for reward in epoch_trace.rewards {
            *merged_rewards.entry(reward.0).or_insert(U256::from(0)) +=
                reward.1;
        }
        for (address, reward) in merged_rewards {
            state
                .add_balance(
                    &address,
                    &reward,
                    CleanupMode::ForceCreate,
                    U256::zero(),
                )
                .unwrap();
        }
        if epoch_trace.new_mint > epoch_trace.burnt_fee {
            state
                .add_total_issued(epoch_trace.new_mint - epoch_trace.burnt_fee);
        } else {
            state.subtract_total_issued(
                epoch_trace.burnt_fee - epoch_trace.new_mint,
            );
        }
        if commit_interval == None {
            handle_epoch(
                &mut state,
                &data_man_replay,
                &mut epoch_hash,
                &mut commit_time,
                height,
                &mut new_height,
                &mut last_committed_height,
                &mut prev_epoches,
                &perf_exec_info,
            );
        }
        height += 1;
        perf_exec_info.lock().old_height += 1;
    }
    height -= 1;
    perf_exec_info.lock().old_height -= 1;

    perf_info_printer_canceller.store(true, Ordering::Relaxed);
    perf_info_printer.join().unwrap();

    println!(
        "old_height: {}\n\
         new_height: {}\n\
         NotExecutedDrop: {}\n\
         NotExecutedToReconsiderPacking: {}\n\
         ExecutionErrorBumpNonce: {}\n\
         Finished: {}",
        height,
        new_height,
        not_executed_drop_cnt,
        not_executed_to_reconsider_packing_cnt,
        execution_error_bump_nonce_cnt,
        finished_cnt
    );
    println!(
        "transact_time: {} ms\n\
         commit_time: {} ms",
        transact_time.as_millis(),
        commit_time.as_millis()
    );

    if last_committed_height < height || transaction_executed > 0 {
        epoch_hash = H256::random();
        commit_state(
            &mut state,
            &data_man_replay,
            &epoch_hash,
            &mut commit_time,
        );
    }
    check_state(
        &data_man_replay,
        &epoch_hash,
        &data_man_ori,
        height,
        &addresses,
    );
}

fn print_perf_item(writer: &mut MutexGuard<BufWriter<File>>, item: &str) {
    write!(writer, "{}", item).unwrap();
    print!("{}", item);
}
fn divide_with_one_space(s: &str) -> String {
    let mut s = s.trim().split_whitespace();
    let mut ret = String::new();
    while let Some(v) = s.next() {
        ret += v;
        ret += " ";
    }
    ret.pop(); // Returns None if empty
    ret
}
fn iostat_to_print() -> std::io::Result<String> {
    let cmd = "df replay_data | tail -n1 | awk '{print $1}' | sed 's,/dev/,,g' | sed 's,[1-9]*$,,g'";
    let output = Command::new("bash").arg("-c").arg(cmd).output()?;
    let dev_name = String::from_utf8(output.stdout).unwrap();
    let mut cmd = "iostat 1 2 | grep ".to_string();
    cmd += dev_name.trim();
    cmd += " | tail -n 1";
    let output = Command::new("bash").arg("-c").arg(cmd).output()?;
    Ok(divide_with_one_space(
        String::from_utf8(output.stdout).unwrap().trim(),
    ))
}
fn time_to_print() -> std::io::Result<String> {
    let mut cmd = "cat /proc/".to_string();
    cmd += &process::id().to_string();
    cmd += &"/stat | awk '{print $14,$15,$16,$17}'".to_string();
    let output = Command::new("bash").arg("-c").arg(&cmd).output()?;
    Ok(String::from_utf8(output.stdout).unwrap().trim().to_string())
}
fn cpu_mem() -> std::io::Result<String> {
    let output = Command::new("ps")
        .arg("-q")
        .arg(process::id().to_string())
        .arg("-o")
        .arg("%cpu,%mem")
        .output()?;
    let out = String::from_utf8(output.stdout).unwrap();
    let mut lines = out.lines();
    lines.next().unwrap(); // Ignore
    Ok(lines.next().unwrap().to_string())
}
fn print_perf_info_once(
    writer: &mut MutexGuard<BufWriter<File>>,
    perf_exec_info: &Arc<Mutex<PerfExecInfo>>, start_time: &Instant,
    iostat_out: String,
) -> std::io::Result<()>
{
    let perf_exec_info = perf_exec_info.lock();
    print_perf_item(writer, &format!("{} ", start_time.elapsed().as_millis()));
    print_perf_item(writer, &format!("{} ", perf_exec_info.old_height));
    print_perf_item(writer, &format!("{} ", perf_exec_info.new_height));
    print_perf_item(
        writer,
        &format!("{} ", perf_exec_info.transaction_executed_total),
    );
    print_perf_item(writer, &(iostat_out + " "));
    print_perf_item(writer, &(time_to_print()? + " "));
    print_perf_item(writer, &cpu_mem()?);
    print_perf_item(writer, "\n");
    Ok(())
}
fn print_perf_info(
    cancelled: Arc<AtomicBool>, perf_log_writer: Arc<Mutex<BufWriter<File>>>,
    perf_exec_info: Arc<Mutex<PerfExecInfo>>,
)
{
    let start_time = Instant::now();
    print_perf_item(
        &mut perf_log_writer.lock(),
        "Time(ms) old_height new_height tx-exe \
         device iops kB_read/s kB_wrtn/s kB_read kB_wrtn \
         utime stime cutime cstime \
         %cpu %mem\n",
    );
    loop {
        if cancelled.load(Ordering::Relaxed) {
            break;
        }
        let iostat_out = iostat_to_print();
        let mut writer = perf_log_writer.lock();
        match iostat_out {
            Ok(iostat_out) => {
                if let Err(err) = print_perf_info_once(
                    &mut writer,
                    &perf_exec_info,
                    &start_time,
                    iostat_out,
                ) {
                    print_perf_item(&mut writer, &err.to_string());
                }
            }
            Err(err) => {
                print_perf_item(&mut writer, &err.to_string());
            }
        }
    }
}

fn open_db(
    conf: &Configuration,
) -> (BlockDataManager, State, Block, Arc<Machine>) {
    let storage_manager = Arc::new(
        StorageManager::new(conf.storage_config())
            .expect("Failed to initialize storage."),
    );
    let vm = VmFactory::new(1024 * 32);
    let machine = Arc::new(new_machine_with_builtin(conf.common_params(), vm));
    let (genesis_block, state) = genesis_state(
        &storage_manager,
        Address::from_str(GENESIS_VERSION).unwrap(),
        U256::zero(),
        machine.clone(),
        conf.raw_conf.execute_genesis, /* need_to_execute */
        conf.raw_conf.chain_id,
    );

    let worker_thread_pool = Arc::new(Mutex::new(ThreadPool::with_name(
        "Tx Recover".into(),
        WORKER_COMPUTATION_PARALLELISM,
    )));
    let (db_path, db_config) = conf.db_config();
    let ledger_db = db::open_database(db_path.to_str().unwrap(), &db_config)
        .map_err(|e| format!("Failed to open database {:?}", e))
        .unwrap();
    let pow_config = conf.pow_config();
    let pow = Arc::new(PowComputer::new(pow_config.use_octopus()));

    let data_man = BlockDataManager::new(
        conf.cache_config(),
        Arc::new(genesis_block.clone()),
        ledger_db.clone(),
        storage_manager,
        worker_thread_pool,
        conf.data_mananger_config(),
        pow.clone(),
    );
    (data_man, state, genesis_block, machine)
}

fn get_state_no_commit_by_epoch_hash(
    data_man: &BlockDataManager, epoch_hash: &H256,
) -> State {
    State::new(StateDb::new(
        data_man
            .storage_manager
            .get_state_no_commit(
                data_man.get_state_readonly_index(epoch_hash).unwrap(),
                false,
            )
            .unwrap()
            .unwrap(),
    ))
    .unwrap()
}

fn check_state(
    data_man_replay: &BlockDataManager, epoch_hash_replay: &H256,
    data_man_ori: &BlockDataManager, epoch_height: u64,
    addresses: &Vec<Address>,
) -> bool
{
    let state_replay =
        get_state_no_commit_by_epoch_hash(data_man_replay, epoch_hash_replay);
    let epoch_hashes = match data_man_ori
        .executed_epoch_set_hashes_from_db(epoch_height)
    {
        Some(x) => x,
        None => {
            eprintln!("Fail to read epoch {} from data_man_ori", epoch_height);
            return true;
        }
    };
    let epoch_id = epoch_hashes.last().unwrap();
    let state_ori = get_state_no_commit_by_epoch_hash(data_man_ori, &epoch_id);

    let mut wrong = false;
    for address in addresses {
        let ori_balance = state_ori.balance(address).unwrap();
        let cur_balance = state_replay.balance(address).unwrap();
        if ori_balance != cur_balance {
            println!("Error: epoch_height {}, address {:x}: ori_balance = {}, cur_balance = {}",
                epoch_height, address, ori_balance, cur_balance);
            wrong = true;
        } else {
            // println!("{} good", address);
        }
    }
    return wrong;
}

fn commit_state(
    state: &mut State, data_man: &BlockDataManager, epoch_hash: &H256,
    commit_time: &mut Duration,
)
{
    let now = Instant::now();
    let state_root = state.commit(epoch_hash.clone(), None).unwrap();
    data_man.insert_epoch_execution_commitment(
        epoch_hash.clone(),
        state_root,
        H256::zero(),
        H256::zero(),
    );
    *commit_time += now.elapsed();
}

fn next_state_from_db(
    data_man: &BlockDataManager, epoch_hash: &H256, height: u64,
) -> State {
    State::new(StateDb::new(
        data_man
            .storage_manager
            .get_state_for_next_epoch(StateIndex::new_for_next_epoch(
                epoch_hash,
                &data_man
                    .load_epoch_execution_commitment_from_db(epoch_hash)
                    .unwrap()
                    .state_root_with_aux_info,
                height,
                data_man.get_snapshot_epoch_count(),
            ))
            .unwrap()
            .unwrap(),
    ))
    .unwrap()
}

fn next_state(
    data_man: &BlockDataManager, epoch_hash: &H256, height: u64,
) -> State {
    State::new(StateDb::new(
        data_man
            .storage_manager
            .get_state_for_next_epoch(StateIndex::new_for_next_epoch(
                epoch_hash,
                &data_man
                    .get_epoch_execution_commitment(epoch_hash)
                    .unwrap()
                    .state_root_with_aux_info,
                height,
                data_man.get_snapshot_epoch_count(),
            ))
            .unwrap()
            .unwrap(),
    ))
    .unwrap()
}

fn cleanup_snapshots(data_man: &BlockDataManager, height_keep: u64) {
    let storage = data_man.storage_manager.get_storage_manager();
    let mut old_pivot_snapshot_infos_to_remove = Vec::new();
    let mut old_pivot_snapshots_to_remove = Vec::new();
    {
        let current_snapshots = storage.current_snapshots.read();
        for snapshot_info in current_snapshots.iter() {
            let snapshot_epoch_id = snapshot_info.get_snapshot_epoch_id();
            if snapshot_info.height < height_keep {
                old_pivot_snapshot_infos_to_remove
                    .push(snapshot_epoch_id.clone());
                old_pivot_snapshots_to_remove.push(snapshot_epoch_id.clone());
            }
        }
    }
    storage
        .remove_snapshots(
            &old_pivot_snapshots_to_remove,
            &Vec::new(),
            &old_pivot_snapshot_infos_to_remove.drain(..).collect(),
        )
        .unwrap();
}

fn genesis_state(
    storage_manager: &Arc<StorageManager>, test_net_version: Address,
    initial_difficulty: U256, machine: Arc<Machine>, need_to_execute: bool,
    genesis_chain_id: Option<u32>,
) -> (Block, State)
{
    let mut state =
        State::new(StateDb::new(storage_manager.get_state_for_genesis_write()))
            .expect("Failed to initialize state");

    let mut genesis_block_author = test_net_version;
    genesis_block_author.set_user_account_type_bits();

    initialize_internal_contract_accounts(
        &mut state,
        machine.internal_contracts().initialized_at_genesis(),
        machine.spec(0).contract_start_nonce,
    );

    let genesis_account_address =
        GENESIS_ACCOUNT_ADDRESS_STR.parse::<Address>().unwrap();

    let genesis_token_count =
        U256::from(GENESIS_TOKEN_COUNT_IN_CFX) * U256::from(ONE_CFX_IN_DRIP);
    state.add_total_issued(genesis_token_count);
    let two_year_unlock_token_count =
        U256::from(TWO_YEAR_UNLOCK_TOKEN_COUNT_IN_CFX)
            * U256::from(ONE_CFX_IN_DRIP);
    let four_year_unlock_token_count =
        genesis_token_count - two_year_unlock_token_count;

    let genesis_account_init_balance =
        U256::from(ONE_CFX_IN_DRIP) * 100 + genesis_token_count;
    state
        .add_balance(
            &genesis_account_address,
            &genesis_account_init_balance,
            CleanupMode::NoEmpty,
            /* account_start_nonce = */ U256::zero(),
        )
        .unwrap();

    let genesis_chain_id = genesis_chain_id.unwrap_or(0);
    let mut genesis_transaction = Transaction::default();
    genesis_transaction.data = GENESIS_TRANSACTION_DATA_STR.as_bytes().into();
    genesis_transaction.action = Action::Call(Default::default());
    genesis_transaction.chain_id = genesis_chain_id;

    let mut create_create2factory_transaction = Transaction::default();
    create_create2factory_transaction.nonce = 0.into();
    create_create2factory_transaction.data =
        GENESIS_TRANSACTION_CREATE_CREATE2FACTORY
            .from_hex()
            .unwrap();
    create_create2factory_transaction.action = Action::Create;
    create_create2factory_transaction.chain_id = genesis_chain_id;
    create_create2factory_transaction.gas = 300000.into();
    create_create2factory_transaction.gas_price = 1.into();
    create_create2factory_transaction.storage_limit = 512;

    let mut create_genesis_token_manager_two_year_unlock_transaction =
        Transaction::default();
    create_genesis_token_manager_two_year_unlock_transaction.nonce = 1.into();
    create_genesis_token_manager_two_year_unlock_transaction.data =
        GENESIS_TRANSACTION_CREATE_GENESIS_TOKEN_MANAGER_TWO_YEAR_UNLOCK
            .from_hex()
            .unwrap();
    create_genesis_token_manager_two_year_unlock_transaction.value =
        two_year_unlock_token_count;
    create_genesis_token_manager_two_year_unlock_transaction.action =
        Action::Create;
    create_genesis_token_manager_two_year_unlock_transaction.chain_id =
        genesis_chain_id;
    create_genesis_token_manager_two_year_unlock_transaction.gas =
        2800000.into();
    create_genesis_token_manager_two_year_unlock_transaction.gas_price =
        1.into();
    create_genesis_token_manager_two_year_unlock_transaction.storage_limit =
        16000;

    let mut create_genesis_token_manager_four_year_unlock_transaction =
        Transaction::default();
    create_genesis_token_manager_four_year_unlock_transaction.nonce = 2.into();
    create_genesis_token_manager_four_year_unlock_transaction.data =
        GENESIS_TRANSACTION_CREATE_GENESIS_TOKEN_MANAGER_FOUR_YEAR_UNLOCK
            .from_hex()
            .unwrap();
    create_genesis_token_manager_four_year_unlock_transaction.value =
        four_year_unlock_token_count;
    create_genesis_token_manager_four_year_unlock_transaction.action =
        Action::Create;
    create_genesis_token_manager_four_year_unlock_transaction.chain_id =
        genesis_chain_id;
    create_genesis_token_manager_four_year_unlock_transaction.gas =
        5000000.into();
    create_genesis_token_manager_four_year_unlock_transaction.gas_price =
        1.into();
    create_genesis_token_manager_four_year_unlock_transaction.storage_limit =
        32000;

    let mut create_genesis_investor_fund_transaction = Transaction::default();
    create_genesis_investor_fund_transaction.nonce = 3.into();
    create_genesis_investor_fund_transaction.data =
        GENESIS_TRANSACTION_CREATE_FUND_POOL.from_hex().unwrap();
    create_genesis_investor_fund_transaction.action = Action::Create;
    create_genesis_investor_fund_transaction.chain_id = genesis_chain_id;
    create_genesis_investor_fund_transaction.gas = 400000.into();
    create_genesis_investor_fund_transaction.gas_price = 1.into();
    create_genesis_investor_fund_transaction.storage_limit = 1000;

    let mut create_genesis_team_fund_transaction = Transaction::default();
    create_genesis_team_fund_transaction.nonce = 4.into();
    create_genesis_team_fund_transaction.data =
        GENESIS_TRANSACTION_CREATE_FUND_POOL.from_hex().unwrap();
    create_genesis_team_fund_transaction.action = Action::Create;
    create_genesis_team_fund_transaction.chain_id = genesis_chain_id;
    create_genesis_team_fund_transaction.gas = 400000.into();
    create_genesis_team_fund_transaction.gas_price = 1.into();
    create_genesis_team_fund_transaction.storage_limit = 1000;

    let mut create_genesis_eco_fund_transaction = Transaction::default();
    create_genesis_eco_fund_transaction.nonce = 5.into();
    create_genesis_eco_fund_transaction.data =
        GENESIS_TRANSACTION_CREATE_FUND_POOL.from_hex().unwrap();
    create_genesis_eco_fund_transaction.action = Action::Create;
    create_genesis_eco_fund_transaction.chain_id = genesis_chain_id;
    create_genesis_eco_fund_transaction.gas = 400000.into();
    create_genesis_eco_fund_transaction.gas_price = 1.into();
    create_genesis_eco_fund_transaction.storage_limit = 1000;

    let mut create_genesis_community_fund_transaction = Transaction::default();
    create_genesis_community_fund_transaction.nonce = 6.into();
    create_genesis_community_fund_transaction.data =
        GENESIS_TRANSACTION_CREATE_FUND_POOL.from_hex().unwrap();
    create_genesis_community_fund_transaction.action = Action::Create;
    create_genesis_community_fund_transaction.chain_id = genesis_chain_id;
    create_genesis_community_fund_transaction.gas = 400000.into();
    create_genesis_community_fund_transaction.gas_price = 1.into();
    create_genesis_community_fund_transaction.storage_limit = 1000;

    let genesis_transactions = vec![
        Arc::new(genesis_transaction.fake_sign(Default::default())),
        Arc::new(
            create_create2factory_transaction
                .fake_sign(genesis_account_address),
        ),
        Arc::new(
            create_genesis_token_manager_two_year_unlock_transaction
                .fake_sign(genesis_account_address),
        ),
        Arc::new(
            create_genesis_token_manager_four_year_unlock_transaction
                .fake_sign(genesis_account_address),
        ),
        Arc::new(
            create_genesis_investor_fund_transaction
                .fake_sign(genesis_account_address),
        ),
        Arc::new(
            create_genesis_team_fund_transaction
                .fake_sign(genesis_account_address),
        ),
        Arc::new(
            create_genesis_eco_fund_transaction
                .fake_sign(genesis_account_address),
        ),
        Arc::new(
            create_genesis_community_fund_transaction
                .fake_sign(genesis_account_address),
        ),
    ];

    if need_to_execute {
        const CREATE2FACTORY_TX_INDEX: usize = 1;
        /*
        const TWO_YEAR_UNLOCK_TX_INDEX: usize = 2;
        const FOUR_YEAR_UNLOCK_TX_INDEX: usize = 3;
        const INVESTOR_FUND_TX_INDEX: usize = 4;
        const TEAM_FUND_TX_INDEX: usize = 5;
        const ECO_FUND_TX_INDEX: usize = 6;
        const COMMUNITY_FUND_TX_INDEX: usize = 7;
        */
        let contract_name_list = vec![
            "CREATE2FACTORY",
            "TWO_YEAR_UNLOCK",
            "FOUR_YEAR_UNLOCK",
            "INVESTOR_FUND",
            "TEAM_FUND",
            "ECO_FUND",
            "COMMUNITY_FUND",
        ];

        for i in CREATE2FACTORY_TX_INDEX..=contract_name_list.len() {
            execute_genesis_transaction(
                genesis_transactions[i].as_ref(),
                &mut state,
                machine.clone(),
            );

            let (contract_address, _) = contract_address(
                CreateContractAddress::FromSenderNonceAndCodeHash,
                0.into(),
                &genesis_account_address,
                &(i - 1).into(),
                &genesis_transactions[i].as_ref().data,
            );

            state
                .set_admin(&contract_address, &Address::zero())
                .expect("");
            // info!(
            //     "Genesis {:?} addresses: {:?}",
            //     contract_name_list[i - 1],
            //     contract_address
            // );
        }
    }

    state
        .clean_account(&genesis_account_address)
        .expect("Clean account failed");

    let state_root = state.compute_state_root(None).unwrap();
    let receipt_root = compute_receipts_root(&vec![Arc::new(BlockReceipts {
        receipts: vec![],
        block_number: 0,
        secondary_reward: U256::zero(),
        tx_execution_error_messages: vec![],
    })]);

    let mut genesis = Block::new(
        BlockHeaderBuilder::new()
            .with_deferred_state_root(state_root.aux_info.state_root_hash)
            .with_deferred_receipts_root(receipt_root)
            .with_gas_limit(GENESIS_GAS_LIMIT.into())
            .with_author(genesis_block_author)
            .with_difficulty(initial_difficulty)
            .with_transactions_root(compute_transaction_root(
                &genesis_transactions,
            ))
            .build(),
        genesis_transactions,
    );
    genesis.block_header.compute_hash();
    // debug!(
    //     "Initialize genesis_block={:?} hash={:?}",
    //     genesis,
    //     genesis.hash()
    // );

    state.commit(genesis.block_header.hash(), None).unwrap();

    genesis.block_header.pow_hash = Some(Default::default());
    // debug!(
    //     "genesis debug_record {}",
    //     serde_json::to_string(&debug_record).unwrap()
    // );
    println!("Hash of genesis: {:x}", genesis.hash());
    (genesis, state)
}

fn read_addresses(address_path: &str) -> Vec<Address> {
    let address_file = File::open(address_path).unwrap();
    let mut address_reader = BufReader::new(address_file);
    let mut ret = Vec::new();
    let mut line = String::new();
    while let Ok(_) = address_reader.read_line(&mut line) {
        if line.len() == 0 {
            break;
        }
        let address: Address = line.parse().unwrap();
        line.clear();
        ret.push(address);
    }
    ret
}

fn remove_dir_content(path: &str) {
    if let Ok(rdir) = std::fs::read_dir(path) {
        for entry in rdir {
            if let Ok(entry) = entry {
                if let Ok(ftype) = entry.file_type() {
                    if ftype.is_dir() {
                        std::fs::remove_dir_all(entry.path()).unwrap();
                    } else {
                        std::fs::remove_file(entry.path()).unwrap();
                    }
                }
            }
        }
    }
}
