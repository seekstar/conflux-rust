use std::{
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
    sync::Arc,
};

use clap::{App, Arg};
use parking_lot::Mutex;
use rustc_hex::FromHex;
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
use cfx_statedb::{StateDb, StateDbGetOriginalMethods};
use cfx_storage::{StorageManager, StorageManagerTrait};
use cfx_types::{address_util::AddressUtil, Address, H256, U256};
use cfxcore::{
    executive::contract_address,
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
    vm::CreateContractAddress,
    vm_factory::VmFactory,
    BlockDataManager, WORKER_COMPUTATION_PARALLELISM,
};
use primitives::{
    Action, Block, BlockHeaderBuilder, BlockReceipts, Transaction,
};

use client::configuration::Configuration;

const GENESIS_VERSION: &str = "1949000000000000000000000000000000001001";

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
            Arg::with_name("epoch-number")
                .short("n")
                .long("epoch-number")
                .value_name("epoch-number")
                .takes_value(true),
        )
        .get_matches();
    let mut conf = Configuration::parse(&arg_matches).unwrap();
    conf.raw_conf.chain_id = Some(1029);
    let dir = arg_matches.value_of("data-dir").unwrap();
    let height = arg_matches
        .value_of("epoch-number")
        .unwrap()
        .parse()
        .unwrap();
    let address_path = format!("{}/address.txt", dir);
    let addresses = read_addresses(&address_path);
    let (data_man_ori, _, _, _) = open_db(&conf);
    conf.raw_conf.conflux_data_dir = "./replay_data".into();
    let (data_man_replay, _, _, _) = open_db(&conf);
    check_state(&data_man_replay, &data_man_ori, height, &addresses);
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
    data_man_replay: &BlockDataManager, data_man_ori: &BlockDataManager,
    epoch_height: u64, addresses: &Vec<Address>,
) -> bool
{
    let epoch_hashes_ori = data_man_ori
        .executed_epoch_set_hashes_from_db(epoch_height)
        .unwrap();
    let epoch_id_ori = epoch_hashes_ori.last().unwrap();
    let state_ori =
        get_state_no_commit_by_epoch_hash(data_man_ori, &epoch_id_ori);

    let epoch_hashes_replay = data_man_replay
        .executed_epoch_set_hashes_from_db(epoch_height)
        .unwrap();
    let epoch_id_replay = epoch_hashes_replay.last().unwrap();
    let state_replay =
        get_state_no_commit_by_epoch_hash(data_man_replay, epoch_id_replay);

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

        // state must have been committed
        let ori_storage_root =
            state_ori.db.get_original_storage_root(address).unwrap();
        let cur_storage_root =
            state_replay.db.get_original_storage_root(address).unwrap();
        if ori_storage_root != cur_storage_root {
            println!("Error: epoch_height {}, address {:x}: ori_storage_root = {:?}, cur_storage_root = {:?}",
                epoch_height, address, ori_storage_root, cur_storage_root);
            wrong = true;
        } else {
            // println!("storage_root of address {:x} is good", address);
        }
    }
    return wrong;
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
