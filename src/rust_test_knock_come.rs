//! ### The "knock-come" deadlock free pattern
//! ```text
//! Øyvind Teig, Trondheim, Norway
//! This was "my" first Rust code. Thanks to pair programming with Google AI and Claude!
//! ```
//!
//! ### Resources
//! * **Blog note:** [The knock-come deadlock free pattern](https://www.teigfam.net/oyvind/home/technology/009-the-knock-come-deadlock-free-pattern/)
//! * **GitHub:** [rust_test_knock_come](<https://github.com/Aclassifier/rust_test_knock_come>)
//!
//! ### Version history
//!
//! ```text
//! 18Jul2026 v0.918  Google AI did not succeed with the Pages concept. Maybe I should just read https://docs.github.com/en/pages
//! 18Jul2026 v0.918  Testing out "cargo doc --open" and /.github/workflows/deploy.yml created , so that I GitHub will show the doc file
//!                   for those who don't have Rust installed (I hope).
//! 17Jul2026 v0.918  Removing the non pin! version of _local timer in task_slave, removing unnecessary complexity in v0.917
//! 17Jul2026 v0.917  New version numbering. Have made several branches to find the version that deadlocked - it was v0.030
//!                   Stored under /src_frozen_versions/v0.917_rust_test_knock_come.rs since this seems to be a max complexity
//! 16Jul2026 0.0.917 Trying to get the 0.0.911 version with the deadlock back with MasterForceSendSlaveSelect. Not tested, just this commit.
//!                   MS_USE_CONSTANT_TIMEOUT is new. get_random_duration -> get_next_timeout.
//!                   MasterForceSendSlaveSelectDeadlocks -> MasterForceSendSlaveSelect
//! 14Jul2026 0.0.916 USE_NESTED_SELECT -> CURRENT_SEMANTICS. But does MasterForceSendSlaveSelect really deadlock?
//! 13Jul2026 0.0.915 Also printing out cnts_per.ms_spontaneous_data_err_cnt on the master side (for USE_NESTED_SELECT 0)
//!                   CURRENT_SEND_MODE was wrong! But mixing USE_NESTED_SELECT 0 or 1 on master is or seem to have been ok
//!                   print_and_clear_slave_cnts caller was 20 all over!
//!                   Full control of coloumn printing, see COL1_WIDTH etc.
//! 13Jul2026 0.0.914 USE_NESTED_SELECT 1, see _log.txt
//! 13Jul2026 0.0.913 local_timer now is a "reptimer" using last .deadline rather than Instant::now() which for every timeout included
//!                   the processing time to get there. See _log.txt, which seems to get averages close to the theoretical sum
//!                   [0..99] ms = (99*100) / 2 = 4950 and average divide by 100 = 49.5s
//! 12Jul2026 0.0.912 local_timer was updated on each round. It should only be updated when the timer has trigged. It should also go from 0 to
//!                   RANDOM_VAL_MAX_MS 99. Changed in master and slave. USE_NESTED_SELECT 0 runs (not tested 1 yet)
//! 12Jul2026 0.0.911 So much change with logging! USE_NESTED_SELECT 0 runs (not tested 1 yet). DT in _log.txt double of what it should be.
//!                   rustfmt.toml new
//! 12Jul2026 0.0.910 Layout. after_knock_come_data_send new name
//! 09Jul2026 0.0.910 debug printing now done on individual print functions with individual strucs for slave and master. Not tested, no logs
//! 09Jul2026 0.0.909 Copy added to Message, now #[derive(Clone, Copy, Debug, PartialEq)] (for speed)
//! 09Jul2026 0.0.908 Layout
//! 09Jul2026 0.0.908 Now only two tasks, with internals controleld by USE_NESTED_SELECT 0 or 1. Come in slave now a function.
//!                   Proper ///-headers added. Not tested, no logs!
//! 08Jul2026 0.0.907 The two master tasks now is only one, where send come is controlled by USE_NESTED_SELECT. In work, no logs
//! 08Jul2026 0.0.906 Now statistics and print-criteria er "wild" withe respect to the two. Next version will rectify this
//!                   USE_NESTED_SELECT 0 has always worked (but compare logs with USE_NESTED_SELECT 1 in log(4) in _log.txt)
//! 08Jul2026 0.0.905 USE_NESTED_SELECT 1 seems to work (see log(3) in _log.txt)
//! 05Jul2026 0.0.904 "/// comments used above tasks
//! 05Jul2026 0.0.904 Names of chans in main
//! 05Jul2026 0.0.903 Lots of new names! Approaching USE_NESTED_SELECT 1 usage
//! 05Jul2026 0.0.902 "Format on save" in VS Code set. Some new comments
//! 05Jul2026 0.0.902 Main file main.rs -> rust_test_knock_come with main function inside (see Cargo.toml)
//! 05Jul2026 0.0.901 Added USE_NESTED_SELECT, but 0 or 1 equal for this version
//! 04Jul2026 0.0.900 Same version but file knock_come_redraw.rs added as a copy-from file
//! 22Jun2026 0.0.900 Some left curly brackets moved to start of line to use VS Code folding
//! 21Jun2026 0.0.900 Testing clickable URLs (4) as starting with // https:..
//!                   Solution: GitHub allows clickable urls only in README.md, not in code,
//!                   but they are clickable in VS Code
//! 21Jun2026 0.0.900 print_welcome like in XC. Using chrono. Plus some comments on the
//!                   "catch" part of try_send in task_b_master
//! 21Jun2026 0.0.320 ms_spontaneous_data_err_cnt is new. Typically between 1 and 18 (obs random timeouts)
//! 20Jun2026 0.0.312 Name of channels changed, and some variables
//! 19Jun2026 0.0.310 Delta time printed out for print of CountersOnly
//! 19Jun2026 0.0.300 Statistics of fairness printed out with a correct print_and_clear_debug_cnts
//!                   ComeData removed because it was simply wrong, since Come always has no data
//! 18Jun2026 0.0.210 New heading above (2)
//! 18Jun2026 0.0.210 println_iff is new, to control printing
//! 18Jun2026 0.0.200 Add strict data sequence verification via asserts and post-send increments
//!                   message type more generic so that they don't have the same names as task variables
//! 17Jun2026 0.0.101 More comments
//! 17Jun2026 0.0.100 More verification etc. Runs
//! 16Jun2026 0.0.050 Final functional version using Tokio biased select to match XC hardware priority
//! 16Jun2026 0.0.040 Integrated idiomatic Rust enums with data payload and state variables
//! 16Jun2026 0.0.030 Knock channel converted to a pure signal channel using unit type ()
//! 17Jul2026 v0.030  0.0.030 stored in /src_frozen_versions/v0.030_main.rs since this deadlocked
//! 16Jun2026 0.0.020 Runs with knock-come, but data are not as wanted
//! 15Jun2026 0.0.010 First version, runs but no knock-come
//! ```

use rand::Rng;
use std::time::Duration;

// =============================================================================================
const VERSION: &str = "0.918";
// =============================================================================================

// =============================================================================================
// GLOBALS
// =============================================================================================

#[allow(dead_code)]
#[derive(Copy, Clone, PartialEq, Debug)]
enum TaskSemantics {
    MasterTrySendSlaveSelect,
    MasterSendSlaveNestedSelect,
    MasterForceSendSlaveSelect,
} // enum

const CURRENT_SEMANTICS: TaskSemantics = TaskSemantics::MasterSendSlaveNestedSelect;

#[rustfmt::skip]
mod config {
    pub const RANDOM_VAL_MIN_MS:       u64  =     0;
    pub const CONST_VAL_MS:            u64  =    50; // Provided MS_USE_CONSTANT_TIMEOUT true
    pub const RANDOM_VAL_MAX_MS:       u64  =    99;
    pub const MAX_SUM_CNT:             u64  =  1000;
    pub const MS_USE_CONSTANT_TIMEOUT: bool = false; // MS_: Master and Slave
                                                     // Using CONST_VAL_MS instead of random [RANDOM_VAL_MIN_MS..RANDOM_VAL_MAX_MS]
} // mod
use config::*;

// Coloumn layout common for print_and_clear_master_cnts and print_and_clear_slave_cnts
//  COL1        COL2      COL3      COL4       COL5         COL6          COL7
//   -1-        -4--      -4--      -4--       -12--------- -10-------    -5---    (USE_NESTED_SELECT)
// S @21  KNOCK 1000 COME 1000 SENT 1000 SDATA 901+0=901               DT 48.63s   (0)
// M @1   KNOCK 1000 COME 1000 DATA 1000 SDATA 901/e29      MASTER-INC DT 48.63s   (0)
// M @2   KNOCK  997 COME  997 DATA  997 SDATA 1000/e0      MASTER-DEC DT 49.73s   (1)
// S @21  KNOCK 1000 COME 1000 SENT 1000 SDATA 989+16=1005             DT 49.93s   (1)
const COL1_WIDTH: usize = 3;
const COL2_WIDTH: usize = 4;
const COL3_WIDTH: usize = 4;
const COL4_WIDTH: usize = 4;
const COL5_WIDTH: usize = 12;
const COL6_WIDTH: usize = 10;
const COL7_WIDTH: usize = 5;

macro_rules! code_block { ($($tokens:tt)*) => { $($tokens)* }; } // Avoids #[rustfmt::skip], no explicit export from block needed

// Between task_master and task_slave, channels set up in main
#[derive(Clone, Copy, Debug, PartialEq)]
enum Message {
    // fields are simply named 'val' since the variant tells us the context
    SpontaneousData { val: ExchangedDataT },
    Come, // No data
    SlaveData { val: ExchangedDataT },
} // enum

// =============================================================================================
// CONTROL LOGGING
// =============================================================================================
//
#[allow(dead_code)] // Tells Rust it is okay that some variants (like None) are not in active use right now
#[derive(Copy, Clone, PartialEq)]
enum LogLevel {
    None,
    CountersOnly,
    All,
} // enum

// Set this to choose what you want to see
const CURRENT_LOG_LEVEL: LogLevel = LogLevel::CountersOnly; // None, CountersOnly or All

// Central logging function that filters everything
fn println_iff(level: LogLevel, args: std::fmt::Arguments) {
    if CURRENT_LOG_LEVEL == LogLevel::All && (level == LogLevel::All || level == LogLevel::CountersOnly) {
        println!("{}", args);
    } else if CURRENT_LOG_LEVEL == LogLevel::CountersOnly && level == LogLevel::CountersOnly {
        println!("{}", args);
    }
} // fn println_iff

// =============================================================================================
// CODE PROPER
// =============================================================================================

#[derive(PartialEq)]
enum MasterComeSendT {
    TrySend,
    SendAsynchAwait,
} // enum
#[derive(PartialEq)]
enum SlaveReceiveT {
    OneSelect,
    SelectPlusNestedSelect,
} // enum

type ExchangedDataT = u32;
const DATA_FIRST_AND_INC: ExchangedDataT = 1;

// #[derive(Default)] automatically creates an init function under the hood that sets all u32 fields to 0
use std::time::Instant; // Put this with the other imports at the top of src/main.rs

// =============================================================================================
// LOGGING
// Legend
//     ms_ from Master to Slave
//     sm_ from Slave to Master
// =============================================================================================

struct MasterCntsAndTimerPeriodic {
    sm_knock_cnt: u64,
    ms_come_cnt: u64,
    sm_data_cnt: u32,                 // finally: from Slave to Master
    ms_spontaneous_data_cnt: u32,     // causing ms_spontaneous_data_cnt_1 or ms_spontaneous_data_cnt_2
    ms_spontaneous_data_err_cnt: u32, // Only if USE_NESTED_SELECT==0
    master_fatter_cnt: u32,
    master_same_cnt: u32,
    master_thinner_cnt: u32,
    print_time_prev: Instant,
} // struct

impl Default for MasterCntsAndTimerPeriodic {
    fn default() -> Self {
        Self {
            sm_knock_cnt: 0,
            ms_come_cnt: 0,
            sm_data_cnt: 0,
            ms_spontaneous_data_cnt: 0,
            ms_spontaneous_data_err_cnt: 0,
            master_fatter_cnt: 0,
            master_same_cnt: 0,
            master_thinner_cnt: 0,
            print_time_prev: Instant::now(),
        }
    }
} // Default

#[derive(Debug, Clone, Copy)]
struct SlaveCnts {
    sm_knock_cnt: u64,
    ms_come_cnt: u64,
    sm_data_cnt: u64,
    ms_spontaneous_data_cnt_1: u64,
    ms_spontaneous_data_cnt_2: u64,
    print_time_prev: Instant,
} // struct

impl Default for SlaveCnts {
    fn default() -> Self {
        Self {
            sm_knock_cnt: 0,
            ms_come_cnt: 0,
            sm_data_cnt: 0,
            ms_spontaneous_data_cnt_1: 0,
            ms_spontaneous_data_cnt_2: 0,
            print_time_prev: Instant::now(),
        }
    }
} // impl Default for

fn print_welcome() {
    // Fetches the current local time from your iMac during startup
    let local_time = chrono::Local::now();

    // Formats the date to exactly match the XC style (like "21Jun2026")
    let compile_date = local_time.format("%d%b%Y").to_string();
    let compile_time = local_time.format("%H:%M").to_string();

    println!(
        "\nRust KNOCK-COME v{} Mode: {:?} on {} {}\n\
        Timeout {} ms, cnt events at {} (Teig)",
        VERSION,
        CURRENT_SEMANTICS,
        compile_date,
        compile_time,
        if MS_USE_CONSTANT_TIMEOUT {
            format!("{}", CONST_VAL_MS)
        } else {
            format!("{}..{}", RANDOM_VAL_MIN_MS, RANDOM_VAL_MAX_MS)
        },
        MAX_SUM_CNT
    );
} // fn print_welcome

fn print_and_clear_master_cnts(caller: u64, cnts_per: &mut MasterCntsAndTimerPeriodic) {
    // Calculate delta seconds since the last printout
    let now = Instant::now();
    let delta_secs = now.duration_since(cnts_per.print_time_prev).as_secs_f32();

    let current_filling = if cnts_per.sm_data_cnt > cnts_per.ms_spontaneous_data_cnt {
        "MASTER-INC"
    } else if cnts_per.sm_data_cnt < cnts_per.ms_spontaneous_data_cnt {
        "MASTER-DEC"
    } else {
        "MASTER-EQL"
    };

    #[rustfmt::skip] // Want block layout, not compact
    let spontaneous_list = format!(
        "{}/e{}", 
        cnts_per.ms_spontaneous_data_cnt,
        cnts_per.ms_spontaneous_data_err_cnt);

    // Align columns with print_and_clear_slave_cnts
    #[rustfmt::skip] // Want block layout, not compact
    println_iff(
        LogLevel::CountersOnly,
        format_args!(
            "M @{:<COL1_WIDTH$} KNOCK {:>COL2_WIDTH$} COME {:>COL3_WIDTH$} DATA {:>COL4_WIDTH$} SDATA {:<COL5_WIDTH$} {:>COL6_WIDTH$} DT {:>COL7_WIDTH$.2}s",
            caller,
            cnts_per.sm_knock_cnt,
            cnts_per.ms_come_cnt,
            cnts_per.sm_data_cnt,
            spontaneous_list,
            current_filling,
            delta_secs,
        ),
    );

    *cnts_per = MasterCntsAndTimerPeriodic::default();
    cnts_per.print_time_prev = now;
} // fn print_and_clear_master_cnts

fn print_and_clear_slave_cnts(caller: u64, cnts_per: &mut SlaveCnts) {
    let now = Instant::now();
    let delta_secs = now.duration_since(cnts_per.print_time_prev).as_secs_f32();

    #[rustfmt::skip] // Want block layout, not compact
    let spontaneous_list = format!(
        "{}+{}={}",
        cnts_per.ms_spontaneous_data_cnt_1,
        cnts_per.ms_spontaneous_data_cnt_2,
        cnts_per.ms_spontaneous_data_cnt_1 + cnts_per.ms_spontaneous_data_cnt_2,
    );

    // Align columns with print_and_clear_master_cnts
    #[rustfmt::skip] // Want block layout, not compact
    println_iff(
        LogLevel::CountersOnly,
        format_args!(
            "S @{:<COL1_WIDTH$} KNOCK {:>COL2_WIDTH$} COME {:>COL3_WIDTH$} SENT {:>COL4_WIDTH$} SDATA {:<COL5_WIDTH$} {:>COL6_WIDTH$} DT {:>COL7_WIDTH$.2}s",
            caller, 
            cnts_per.sm_knock_cnt, 
            cnts_per.ms_come_cnt, 
            cnts_per.sm_data_cnt, 
            spontaneous_list, 
            "",
            delta_secs,
        ),
    );

    // Reset slave-counters
    *cnts_per = SlaveCnts::default();
    cnts_per.print_time_prev = now;
} // fn print_and_clear_slave_cnts

fn update_master_view_fairness_cnts(cnts_per: &mut MasterCntsAndTimerPeriodic) {
    if cnts_per.sm_data_cnt > cnts_per.ms_spontaneous_data_cnt {
        cnts_per.master_fatter_cnt += 1;
    } else if cnts_per.sm_data_cnt < cnts_per.ms_spontaneous_data_cnt {
        cnts_per.master_thinner_cnt += 1;
    } else {
        cnts_per.master_same_cnt += 1;
    }
} // fn update_master_view_fairness_cnts

// =============================================================================================
// STATE TRANSITION HANDLING
// =============================================================================================

#[derive(Copy, Clone, Debug, PartialEq)]
enum KnockComeState {
    SlaveSentDataNowReady, // Also init -> SlaveSentKnock
    SlaveSentKnock,        //           -> SlaveGotCome or SlaveGotSpontaneousData (value not needed)
    SlaveGotCome,          //           -> SlaveSentDataNowReady (atomic)
    MasterGotDataNowReady, // Also init -> MasterGotKnock
    MasterGotKnock,        //           -> MasterSentCome
    MasterSentCome,        //           -> MasterGotDataNowReady (atomic)
}

/// slave_set_knock_come_state transitions the slave's state from the current value to a new value.
///
/// This helper encapsulates the state machine logic for the slave task, tracking
/// and returning the updated lifecycle phase of the handshake protocol.
///
/// # Arguments
///
/// * `present_state` - The current active state of the slave event loop.
/// * `new_state` - The target state to transition into.
///
/// # Returns
///
/// Returns the updated `KnockComeState` that should be assigned to the slave's local state variable.
///
fn slave_set_knock_come_state(present_state: KnockComeState, new_state: KnockComeState) -> KnockComeState {
    // Rust uses 'cfg(debug_assertions)' to automatically enable/disable debug code.
    // This code only runs when compiling in debug mode (like #if DEBUG_KNOCKCOME == 1)
    if cfg!(debug_assertions) {
        match new_state {
            KnockComeState::SlaveSentKnock => {
                assert_eq!(present_state, KnockComeState::SlaveSentDataNowReady, "Invalid slave transition to SlaveSentKnock!");
            }
            KnockComeState::SlaveGotCome => {
                assert_eq!(present_state, KnockComeState::SlaveSentKnock, "Invalid slave transition to SlaveGotCome!");
            }
            KnockComeState::SlaveSentDataNowReady => {
                // No assertions needed here
            }
            // Rust enforces that all enum variants must be covered.
            // If new_state is a Master-state, we fail immediately:
            _ => panic!("Slave attempted to transition to an invalid state: {:?}", new_state),
        }
    }

    // Return the new state (no 'return' keyword needed on the last line in Rust)
    new_state
} // fn slave_set_knock_come_state

/// master_set_knock_come_state transitions the master's state from the current value to a new value.
///
/// This helper encapsulates the state machine logic for the master task, verifying
/// and applying updates to the synchronization lifecycle.
///
/// # Arguments
///
/// * `present_state` - The current active state of the master event loop.
/// * `new_state` - The target state to transition into.
///
fn master_set_knock_come_state(present_state: KnockComeState, new_state: KnockComeState) -> KnockComeState {
    // This code only runs when compiling in debug mode (equivalent to #if DEBUG_KNOCKCOME == 1)
    if cfg!(debug_assertions) {
        match new_state {
            KnockComeState::MasterGotKnock => {
                assert_eq!(present_state, KnockComeState::MasterGotDataNowReady, "Invalid master transition to MasterGotKnock!");
            }
            KnockComeState::MasterSentCome => {
                assert_eq!(present_state, KnockComeState::MasterGotKnock, "Invalid master transition to MasterSentCome!");
            }
            KnockComeState::MasterGotDataNowReady => {
                // No code
            }
            // Catch-all to panic if the master attempts to use a Slave state
            _ => panic!("Master attempted to transition to an invalid state: {:?}", new_state),
        }
    }

    // Return the new state implicitly by omitting the semicolon
    new_state
} // fn master_set_knock_come_state

/// task_master receives knocks, responds with come and atomically waits for data. May send sdata any time
///
/// # Fairness
///
/// The "fairness" argument of not having "biased" on `flume::Selector` is the same as Golang not having prioritised select, either.
/// A select branch that may almost never happen may indeed starve. Controlling fairness is indeed possible with "biased".
/// Read at <https://www.teigfam.net/oyvind/home/technology/049-nondeterminism/#-nondeterministic_selective_choice_in_implementations_is_not_good>
///
/// # Arguments
///
/// * `ch_knock_rx` - [channel receiving knock signals]
/// * `ch_come_or_sdata_tx` - [channel sending Come or ms_SpontaneousData messages]
/// * `ch_come_rx` - [channel receiving Come messages]
///
async fn task_master(ch_knock_rx: flume::Receiver<()>, ch_come_or_sdata_tx: flume::Sender<Message>, ch_come_rx: flume::Receiver<Message>) {
    let mut data_from_master: ExchangedDataT = DATA_FIRST_AND_INC;
    let mut data_from_slave: ExchangedDataT = 0; // So that the first received is DATA_FIRST_AND_INC more 
    let mut cnts_per = MasterCntsAndTimerPeriodic::default();
    let mut state = KnockComeState::MasterGotDataNowReady;

    const CURRENT_SEND_MODE: MasterComeSendT = match CURRENT_SEMANTICS {
        TaskSemantics::MasterTrySendSlaveSelect => MasterComeSendT::TrySend,
        TaskSemantics::MasterSendSlaveNestedSelect => MasterComeSendT::SendAsynchAwait,
        TaskSemantics::MasterForceSendSlaveSelect => MasterComeSendT::SendAsynchAwait,
    };

    print_and_clear_master_cnts(0, &mut cnts_per);

    // Helper closure to generate a new duration based on selected models
    let get_next_timeout = || {
        if MS_USE_CONSTANT_TIMEOUT {
            Duration::from_millis(CONST_VAL_MS)
        } else {
            let mut rng = rand::rng();
            Duration::from_millis(rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS))
        }
    };

    // Create the initial timer and pin it to the stack so tokio::select! can use it mutably
    // See https://docs.rs/tokio/latest/tokio/time/struct.Sleep.html
    let local_timer = tokio::time::sleep(Duration::from_millis(RANDOM_VAL_MIN_MS));
    tokio::pin!(local_timer);

    loop {
        // We use tokio::select! instead of flume::Selector to get strict event priority (PRI ALT / [[ordered]] select).
        // Flume's lack of ordering caused race-condition deadlocks when timeouts and channel events overlapped.
        // Additionally, Tokio's native sleep avoids the overhead of spawning background tasks for timers

        local_timer.set(tokio::time::sleep(get_next_timeout()));

        tokio::select! { // flume::Selector::new() not used. It is based on fairness, and does npt have "biased" [**]
            biased;

            // CASE 1: Receive Knock from Slave
            knock_res = ch_knock_rx.recv_async() =>
            {
                if let Ok(()) = knock_res {
                    println_iff(LogLevel::All, format_args!("[Master] Received KNOCK from slave."));
                    state = master_set_knock_come_state(state, KnockComeState::MasterGotKnock);

                    cnts_per.sm_knock_cnt += 1;

                    // Transmit the clean COME signal to the slave without any payload
                    let _ = ch_come_or_sdata_tx.send_async(Message::Come).await; // .try_send not needed here

                    cnts_per.ms_come_cnt += 1;

                    state = master_set_knock_come_state(state, KnockComeState::MasterSentCome);

                    // Receive the synchronous reply from the slave
                    let after_knock_come_data = ch_come_rx.recv_async().await;

                    // Verify packet type and payload
                    match after_knock_come_data {
                        Ok(Message::SlaveData { val }) => {
                            // Verify that incoming slave data matches history + incremental step
                            assert_eq!(
                                val,
                                data_from_slave + DATA_FIRST_AND_INC,
                                "[Master] Data sequence gap detected in SlaveData!"
                            );
                            // Update history tracking for slave data
                            data_from_slave = val;
                            println_iff(LogLevel::All, format_args!("[Master] Handshake complete! Captured SlaveData: {}", data_from_slave));
                            // Update statistics tracking
                            cnts_per.sm_data_cnt += 1;
                            // Calculate and evaluate protocol fairness
                            // Update fairness metrics and check if it's time to print and reset interval counters
                            update_master_view_fairness_cnts(&mut cnts_per);
                            if cnts_per.sm_data_cnt == MAX_SUM_CNT as u32 {
                                print_and_clear_master_cnts(1, &mut cnts_per);
                            } else { }
                        }
                        _ => {
                            // Enforce strict protocol compliance or catch channel closure
                            panic!("[Master] Protocol violation or channel closed during payload rendezvous!");
                        }
                    }
                    // Complete the sequence by returning to the initial ready state
                    state = master_set_knock_come_state(state, KnockComeState::MasterGotDataNowReady);
                } else {
                    break;
                }
            }

            // CASE 2: Local Timer Ticked
            _ = &mut local_timer =>
            {
                // Create the message with the CURRENT value first
                let spontaneous_data = Message::SpontaneousData { val: data_from_master };

                let send_success = match CURRENT_SEND_MODE {
                    MasterComeSendT::TrySend => {
                        ch_come_or_sdata_tx.try_send(spontaneous_data).is_ok()
                    }
                    MasterComeSendT::SendAsynchAwait => {
                        ch_come_or_sdata_tx.send_async(spontaneous_data).await.is_ok()
                    }
                };

                if send_success {
                    println_iff(LogLevel::All, format_args!("[Master] Local timeout tick. Sent spontaneous data: {}", data_from_master));
                    // INCREMENT AFTER SENDING (Matches your protocol requirement)
                    data_from_master += DATA_FIRST_AND_INC;

                    // Update statistics tracking
                    cnts_per.ms_spontaneous_data_cnt += 1;
                    update_master_view_fairness_cnts(&mut cnts_per);
                    if cnts_per.ms_spontaneous_data_cnt == MAX_SUM_CNT as u32 {
                        print_and_clear_master_cnts(2, &mut cnts_per);
                    } else { }
                } else {
                    cnts_per.ms_spontaneous_data_err_cnt += 1;
                }
                // Calculate the next timeout based on the PREVIOUS deadline to prevent timer drift.
                // This ensures executing logic overhead does not delay subsequent intervals.
                let next_timeout = local_timer.deadline() + get_next_timeout();
                local_timer.as_mut().reset(next_timeout);
            }
        }
    }
} // asynch fn task_master

/// after_knock_come_data_send
///
/// # Arguments
///
/// * `state` - Mutable reference to the current handshake state of the slave.
/// * `ch_come_tx` - Reference to the flume channel used for sending the SlaveData message.
/// * `data_from_slave` - Mutable reference to the payload data counter/value sequence.
///
async fn after_knock_come_data_send(state: &mut KnockComeState, ch_come_tx: &flume::Sender<Message>, data_from_slave: &mut ExchangedDataT) {
    *state = slave_set_knock_come_state(*state, KnockComeState::SlaveGotCome);

    let after_knock_come_data = Message::SlaveData { val: *data_from_slave };
    let _ = ch_come_tx.send_async(after_knock_come_data).await;

    println_iff(LogLevel::All, format_args!("[Slave] Handshake complete. Sent SlaveData: {}", *data_from_slave));

    *data_from_slave += DATA_FIRST_AND_INC;
    *state = slave_set_knock_come_state(*state, KnockComeState::SlaveSentDataNowReady);
} // asynch fn after_knock_come_data_send

/// task_slave
///
/// # Fairness
///
/// See `task_master`
///
/// # Arguments
///
/// * `ch_knock_tx` - The flume channel used for sending local timeout knock signals.
/// * `ch_come_or_sdata_rx` - The flume channel receiving incoming messages (Come or ms_SpontaneousData) from the master.
/// * `ch_come_tx` - The flume channel used to transmit the final handshake response (SlaveData).
///
async fn task_slave(ch_knock_tx: flume::Sender<()>, ch_come_or_sdata_rx: flume::Receiver<Message>, ch_come_tx: flume::Sender<Message>) {
    let mut state = KnockComeState::SlaveSentDataNowReady;
    let mut data_from_slave: ExchangedDataT = DATA_FIRST_AND_INC;
    let mut data_from_master: ExchangedDataT = 0; // History variable for ms_SpontaneousData

    const CURRENT_SELECT_MODE: SlaveReceiveT = match CURRENT_SEMANTICS {
        TaskSemantics::MasterTrySendSlaveSelect => SlaveReceiveT::OneSelect,
        TaskSemantics::MasterSendSlaveNestedSelect => SlaveReceiveT::SelectPlusNestedSelect,
        TaskSemantics::MasterForceSendSlaveSelect => SlaveReceiveT::OneSelect,
    };

    let mut cnts_per = SlaveCnts::default();

    print_and_clear_slave_cnts(20, &mut cnts_per);

    // Helper closure to generate a new duration based on selected models
    let get_next_timeout = || {
        if MS_USE_CONSTANT_TIMEOUT {
            Duration::from_millis(CONST_VAL_MS)
        } else {
            let mut rng = rand::rng();
            Duration::from_millis(rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS))
        }
    };

    // Create the initial timer and pin it to the stack so tokio::select! can use it mutably
    // See https://docs.rs/tokio/latest/tokio/time/struct.Sleep.html
    let local_timer = tokio::time::sleep(Duration::from_millis(RANDOM_VAL_MIN_MS));
    tokio::pin!(local_timer);

    loop {
        local_timer.set(tokio::time::sleep(get_next_timeout()));

        tokio::select! { // flume::Selector::new() not used. It is based on fairness, and does npt have "biased" [**]
            biased;

            // CASE 1: Receive from master (Always active)
            spontaneous_data_or_come = ch_come_or_sdata_rx.recv_async() => {
                if let Ok(msg) = spontaneous_data_or_come {
                    match msg {
                        Message::SpontaneousData { val } => {
                            // CORRECTED: Verify sequence only for actual spontaneous data stream
                            assert_eq!(
                                val,
                                data_from_master + DATA_FIRST_AND_INC,
                                "[Slave] Data sequence gap detected in ms_SpontaneousData!"
                            );
                            data_from_master = val;
                            println_iff(LogLevel::All, format_args!("[Slave] Processed spontaneous data from Master: {}", data_from_master));
                            cnts_per.ms_spontaneous_data_cnt_1 += 1;
                        }

                        Message::Come => {
                            if CURRENT_SELECT_MODE == SlaveReceiveT::OneSelect {
                                cnts_per.ms_come_cnt += 1;
                                after_knock_come_data_send(&mut state, &ch_come_tx, &mut data_from_slave).await;
                                cnts_per.sm_data_cnt += 1;
                            } else {
                                panic!(r#"[Slave] No "spontaneous" come here"#); // Raw string avoids backslash for embedded quote
                            }
                        }
                        _ => panic!("[Slave] Unexpected packet type received!"),
                    }
                } else {
                    panic!("[Slave] msg not ok");
                }
            }

            // CASE 2: Local Timer
            _ = &mut local_timer, if state == KnockComeState::SlaveSentDataNowReady => {

                let _ = ch_knock_tx.send_async(()).await; // .try_send not needed here
                cnts_per.sm_knock_cnt += 1;
                state = slave_set_knock_come_state(state, KnockComeState::SlaveSentKnock);
                println_iff(LogLevel::All, format_args!("[Slave] Local timeout tick. Knock signal sent! State -> SlaveSentKnock"));

                // Since SpontaneousData for USE_NESTED_SELECT 0 (with .try_send in task_master) may be "less than 1000" it's plain
                // wrong to count ms_spontaneous_data_cnt_1 or ms_spontaneous_data_cnt_2 for printing:
                //
                if cnts_per.sm_knock_cnt % MAX_SUM_CNT == 0 {
                    print_and_clear_slave_cnts(21, &mut cnts_per);
                } else { }

                if CURRENT_SELECT_MODE == SlaveReceiveT::SelectPlusNestedSelect {
                    'await_come: loop {
                        tokio::select! {
                            biased;
                            spontaneous_data_or_come = ch_come_or_sdata_rx.recv_async() => {
                                if let Ok(msg) = spontaneous_data_or_come {
                                    match msg {
                                        Message::SpontaneousData { val } => {
                                            assert_eq!(
                                                val,
                                                data_from_master + DATA_FIRST_AND_INC,
                                                "[Slave] Data sequence gap detected in ms_SpontaneousData!"
                                            );
                                            data_from_master = val;
                                            cnts_per.ms_spontaneous_data_cnt_2 += 1;
                                            println_iff(LogLevel::All, format_args!("[Slave] Processed spontaneous data from Master: {}", data_from_master));

                                            // NOT break 'await_come; since we must stay tuned until Come has been received
                                        }
                                        Message::Come => {
                                            cnts_per.ms_come_cnt += 1;
                                            after_knock_come_data_send(&mut state, &ch_come_tx, &mut data_from_slave).await;
                                            cnts_per.sm_data_cnt += 1;
                                            break 'await_come;
                                        }
                                        _ => panic!("[Slave] Come or sdata expected!")
                                    }
                                } else {
                                    panic!("[Slave] msg not ok");
                                }
                            }
                        }
                    } // end 'await_come: loop
                }
                // Calculate the next timeout based on the PREVIOUS deadline to prevent timer drift.
                // This ensures executing logic overhead does not delay subsequent intervals.
                let next_timeout = local_timer.deadline() + get_next_timeout();
                local_timer.as_mut().reset(next_timeout);
            }
        }
    }
} // async fn task_slave

const CHAN_STREAMING_CAP_1: usize = 1;
const CHAN_SYNCH_CAP_0: usize = 0;

#[tokio::main]

/// Application entry point that initializes asynchronous channels and spawns concurrent tasks.
///
/// This function sets up the required flume architecture and launches the master and slave
/// event loops to run concurrently on the Tokio runtime.
///
/// # Panics
///
/// This function will panic if either the master or slave task terminates, as the
/// execution handles are unwrapped upon completion.
///
async fn main() {
    print_welcome();
    code_block! {
        let (ch_knock_tx,         ch_knock_rx)         : (flume::Sender<()>,      flume::Receiver<()>)      = flume::bounded(CHAN_STREAMING_CAP_1);
        let (ch_come_or_sdata_tx, ch_come_or_sdata_rx) : (flume::Sender<Message>, flume::Receiver<Message>) = flume::bounded(CHAN_SYNCH_CAP_0);
        let (ch_come_tx,          ch_come_rx)          : (flume::Sender<Message>, flume::Receiver<Message>) = flume::bounded(CHAN_SYNCH_CAP_0);

        let task_slave_handle  : tokio::task::JoinHandle<()> = tokio::spawn(task_slave (ch_knock_tx, ch_come_or_sdata_rx, ch_come_tx));
        let task_master_handle : tokio::task::JoinHandle<()> = tokio::spawn(task_master(ch_knock_rx, ch_come_or_sdata_tx, ch_come_rx));
    }

    println!("task_master and task_slave running in parallel forever\n//");

    // Since I already have done spawn
    task_slave_handle.await.unwrap();
    task_master_handle.await.unwrap();
} // async fn main
