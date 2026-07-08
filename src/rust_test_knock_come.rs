// =============================================================================================
// THE KNOCK-COME DEADLOCK FREE PATTERN
// Øyvind Teig, Trondheim, Norway
//     This was "my" first Rust code. Thanks to pair programming with Google AI!
// Blog note:
//     https://www.teigfam.net/oyvind/home/technology/009-the-knock-come-deadlock-free-pattern/
// GitHub:
//     https://github.com/Aclassifier/rust_test_knock_come
// VERSIONS / COMMITS
//
const VERSION: &str = "0.0.905";
//
// 08Jul2026 0.0.905 USE_NESTED_SELECT 1 seems to work
// 05Jul2026 0.0.904 "/// comments used above tasks
// 05Jul2026 0.0.904 Names of chans in main
// 05Jul2026 0.0.903 Lots of new names! Approaching USE_NESTED_SELECT 1 usage
// 05Jul2026 0.0.902 "Format on save" in VS Code set. Some new comments
// 05Jul2026 0.0.902 Main file main.rs -> rust_test_knock_come with main function inside (see Cargo.toml)
// 05Jul2026 0.0.901 Added USE_NESTED_SELECT, but 0 or 1 equal for this version
// 04Jul2026 0.0.900 Same version but file knock_come_redraw.rs added as a copy-from file
// 22Jun2026 0.0.900 Some left curly brackets moved to start of line to use VS Code folding
// 21Jun2026 0.0.900 Testing clickable URLs (4) as starting with // https:..
//                   Solution: GitHub allows clickable urls only in README.md, not in code,
//                   but they are clickable in VS Code
// 21Jun2026 0.0.900 print_welcome like in XC. Using chrono. Plus some comments on the
//                   "catch" part of try_send in task_b_master
// 21Jun2026 0.0.320 avoid_deadlock_cnt is new. Typically between 1 and 18 (obs random timeouts)
// 20Jun2026 0.0.312 Name of channels changed, and some variables
// 19Jun2026 0.0.310 Delta time printed out for print of CountersOnly
// 19Jun2026 0.0.300 Statistics of fairness printed out with a correct print_and_clear_debug_cnts
//                   ComeData removed because it was simply wrong, since Come always has no data
// 18Jun2026 0.0.210 New heading above (2)
// 18Jun2026 0.0.210 println_iff is new, to control printing
// 18Jun2026 0.0.200 Add strict data sequence verification via asserts and post-send increments
//                   message type more generic so that they don't have the same names as task variables
// 17Jun2026 0.0.101 More comments
// 17Jun2026 0.0.100 More verification etc. Runs
// 16Jun2026 0.0.050 Final functional version using Tokio biased select to match XC hardware priority
// 16Jun2026 0.0.040 Integrated idiomatic Rust enums with data payload and state variables
// 16Jun2026 0.0.030 Knock channel converted to a pure signal channel using unit type ()
// 16Jun2026 0.0.020 Runs with knock-come, but data are not as wanted
// 15Jun2026 0.0.010 First version, runs but no knock-come
// =============================================================================================

use rand::Rng;
use std::time::Duration;
use tokio::time::sleep;

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
}

// Set this to choose what you want to see
const CURRENT_LOG_LEVEL: LogLevel = LogLevel::CountersOnly; // None, CountersOnly or All

// Central logging function that filters everything
fn println_iff(level: LogLevel, args: std::fmt::Arguments) {
    if CURRENT_LOG_LEVEL == LogLevel::All
        && (level == LogLevel::All || level == LogLevel::CountersOnly)
    {
        println!("{}", args);
    } else if CURRENT_LOG_LEVEL == LogLevel::CountersOnly && level == LogLevel::CountersOnly {
        println!("{}", args);
    }
}

// =============================================================================================
// CODE PROPER
// =============================================================================================

const USE_NESTED_SELECT: u32 = 1; // 0 or 1 equal for 0.0.901
const RANDOM_VAL_MIN_MS: u64 = 0;
const RANDOM_VAL_MAX_MS: u64 = 100;
const MAX_SUM_CNT: u32 = 1000;

type ExchangedDataT = u32;
const DATA_FIRST_AND_INC: ExchangedDataT = 1;

// #[derive(Default)] automatically creates an init function under the hood that sets all u32 fields to 0
use std::time::Instant; // Put this with the other imports at the top of src/main.rs

// =============================================================================================
// LOGGING
// =============================================================================================
#[derive(PartialEq)] // So that I may use it in comparisons
enum MeTaskT {
    Master,
    Slave,
}

#[derive(Debug, Clone, Copy)] // Removed Default from here!
#[allow(dead_code)]
struct Cnts {
    sent_cnt: u32,
    rec_cnt: u32,
    rec_sent_cnt: u32,
    rec_gt_sent_cnt: u32,
    rec_eq_sent_cnt: u32,
    rec_lt_sent_cnt: u32,
    sum_sent_cnt: u32,
    sum_rec_cnt: u32,
    avoid_deadlock_cnt: u32,
    last_print_time: Instant,
    // Some would overlap with above, but nice for nested select (started by me in knock_come_redraw.rs)
    knocks: u64,
    comes: u64,
    datas: u64,
    spontaneous_datas: u64,
    spontaneous_datas_2: u64,
}

// This manual block is now the ONLY initialization rule for Cnts
impl Default for Cnts {
    fn default() -> Self {
        Self {
            sent_cnt: 0,
            rec_cnt: 0,
            rec_sent_cnt: 0,
            rec_gt_sent_cnt: 0,
            rec_eq_sent_cnt: 0,
            rec_lt_sent_cnt: 0,
            sum_sent_cnt: 0,
            sum_rec_cnt: 0,
            avoid_deadlock_cnt: 0,
            last_print_time: Instant::now(), // Now this field physically exists!
            knocks: 0,
            comes: 0,
            datas: 0,
            spontaneous_datas: 0,
            spontaneous_datas_2: 0,
        }
    }
}

fn print_welcome() {
    // Fetches the current local time from your iMac during startup
    let local_time = chrono::Local::now();

    // Formats the date to exactly match your XC style (e.g., 21Jun2026)
    let compile_date = local_time.format("%d%b%Y").to_string();
    let compile_time = local_time.format("%H:%M").to_string();

    println!(
        "Rust KNOCK-COME v{} USE_NESTED_SELECT {} on {} {}\n\
         Time random max {} ms, cnt events at {} (Teig)\n",
        VERSION, USE_NESTED_SELECT, compile_date, compile_time, RANDOM_VAL_MAX_MS, MAX_SUM_CNT
    );
}

fn print_and_clear_debug_cnts(caller: u64, me_task: MeTaskT, cnts: &mut Cnts) {
    let current_me_task = if me_task == MeTaskT::Master {
        "Master"
    } else if me_task == MeTaskT::Slave {
        "Slave"
    } else {
        "?"
    };

    let current_sign = if cnts.rec_cnt > cnts.sent_cnt {
        ">"
    } else if cnts.rec_cnt < cnts.sent_cnt {
        "<"
    } else {
        "="
    };

    let sum_sign = if cnts.sum_rec_cnt > cnts.sum_sent_cnt {
        ">"
    } else if cnts.sum_rec_cnt < cnts.sum_sent_cnt {
        "<"
    } else {
        "="
    };

    let catch_uppercase: &str = if cnts.avoid_deadlock_cnt > 0 {
        "CATCH"
    } else {
        "catch"
    };

    // Calculate delta seconds since the last printout
    let now = Instant::now();
    let delta_secs = now.duration_since(cnts.last_print_time).as_secs_f32();

    // Prints the metrics with delta seconds appended to the start or end of the log
    println_iff(
        LogLevel::CountersOnly,
        format_args!(
            "{} [{}] REC {}\t{}\tSENT {}\t(>{}= {} <{})\tSUM (REC {} {} SENT {}) {} {}\tDT {:.2}s knocks {} comes {} datas {} spontaneous_datas {} + spontaneous_datas_2 {} = {}",
            current_me_task,
            caller,
            cnts.rec_cnt,
            current_sign,
            cnts.sent_cnt,
            cnts.rec_gt_sent_cnt,
            cnts.rec_eq_sent_cnt,
            cnts.rec_lt_sent_cnt,
            cnts.sum_rec_cnt,
            sum_sign,
            cnts.sum_sent_cnt,
            catch_uppercase,
            cnts.avoid_deadlock_cnt,
            delta_secs, // Injected into the printout
            //
            cnts.knocks,
            cnts.comes,
            cnts.datas,
            cnts.spontaneous_datas,
            cnts.spontaneous_datas_2,
            cnts.spontaneous_datas + cnts.spontaneous_datas_2
        ),
    );

    // Reset interval counters and update the time benchmark for the next 50-tick
    cnts.sent_cnt = 0;
    cnts.rec_cnt = 0;
    cnts.rec_sent_cnt = 0;
    cnts.rec_gt_sent_cnt = 0;
    cnts.rec_eq_sent_cnt = 0;
    cnts.rec_lt_sent_cnt = 0;
    cnts.avoid_deadlock_cnt = 0; // Also zeroing this, same rule as the others
    cnts.last_print_time = now; // Reset timer benchmark
}

fn update_fairness_cnts(cnts: &mut Cnts) {
    if cnts.rec_cnt > cnts.sent_cnt {
        cnts.rec_gt_sent_cnt += 1;
    } else if cnts.rec_cnt < cnts.sent_cnt {
        cnts.rec_lt_sent_cnt += 1;
    } else {
        cnts.rec_eq_sent_cnt += 1;
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Message {
    // fields are simply named 'val' since the variant tells us the context
    SpontaneousData { val: ExchangedDataT },
    Come, // No data
    SlaveData { val: ExchangedDataT },
}

// =============================================================================================
// STATE TRANSITION HANDLING
// Optional in XC, not so here
// =============================================================================================

#[derive(Copy, Clone, Debug, PartialEq)]
enum KnockComeState {
    SlaveSentDataNowReady, // Also init -> SlaveSentKnock
    SlaveSentKnock, //           -> SlaveGotCome or SlaveGotSpontaneousData (value not needed)
    SlaveGotCome,   //           -> SlaveSentDataNowReady (atomic)
    MasterGotDataNowReady, // Also init -> MasterGotKnock
    MasterGotKnock, //           -> MasterSentCome
    MasterSentCome, //           -> MasterGotDataNowReady (atomic)
}

// In Rust, 'const' in parameters is not used. Variables are immutable by default.
fn slave_set_knock_come_state(
    present_state: KnockComeState,
    new_state: KnockComeState,
) -> KnockComeState {
    // Rust uses 'cfg(debug_assertions)' to automatically enable/disable debug code.
    // This code only runs when compiling in debug mode (like #if DEBUG_KNOCKCOME == 1)
    if cfg!(debug_assertions) {
        match new_state {
            KnockComeState::SlaveSentKnock => {
                assert_eq!(
                    present_state,
                    KnockComeState::SlaveSentDataNowReady,
                    "Invalid slave transition to SlaveSentKnock!"
                );
            }
            KnockComeState::SlaveGotCome => {
                assert_eq!(
                    present_state,
                    KnockComeState::SlaveSentKnock,
                    "Invalid slave transition to SlaveGotCome!"
                );
            }
            KnockComeState::SlaveSentDataNowReady => {
                // No assertions needed here according to your XC code
            }
            // Rust enforces that all enum variants must be covered.
            // If new_state is a Master-state, we fail immediately:
            _ => panic!(
                "Slave attempted to transition to an invalid state: {:?}",
                new_state
            ),
        }
    }

    // Return the new state (no 'return' keyword needed on the last line in Rust)
    new_state
}

fn master_set_knock_come_state(
    present_state: KnockComeState,
    new_state: KnockComeState,
) -> KnockComeState {
    // This code only runs when compiling in debug mode (equivalent to #if DEBUG_KNOCKCOME == 1)
    if cfg!(debug_assertions) {
        match new_state {
            KnockComeState::MasterGotKnock => {
                assert_eq!(
                    present_state,
                    KnockComeState::MasterGotDataNowReady,
                    "Invalid master transition to MasterGotKnock!"
                );
            }
            KnockComeState::MasterSentCome => {
                assert_eq!(
                    present_state,
                    KnockComeState::MasterGotKnock,
                    "Invalid master transition to MasterSentCome!"
                );
            }
            KnockComeState::MasterGotDataNowReady => {
                // No code since ..NOW_READY according to your XC code
            }
            // Catch-all to panic if the master attempts to use a Slave state
            _ => panic!(
                "Master attempted to transition to an invalid state: {:?}",
                new_state
            ),
        }
    }

    // Return the new state implicitly by omitting the semicolon
    new_state
}

/// Implements the slave task in the Knock-Come pattern.
///
/// This task manages randomized timeouts and coordinates the rendezvous-style
/// message exchange with the master task.
///
/// The timer as a case in the tokio:select gives rise to a deadlock with task_master_try_send
/// for the version without the trys_send. With send_async instead, the deadlock appears immediately.
///
/// # Channels
/// * `ch_knock_tx` - Transmits the asynchronous "knock" signal to initiate a transaction.
/// * `ch_come_or_sdata_rx` - Receives either a "come" authorization or spontaneous data from the master.
/// * `ch_come_tx` - Sends the actual payload data back to the master following an approved "come".
///
/// CODE FOR USE_NESTED_SELECT == 0
async fn task_slave(
    ch_knock_tx: flume::Sender<()>,
    ch_come_or_sdata_rx: flume::Receiver<Message>,
    ch_come_tx: flume::Sender<Message>,
) {
    let mut state = KnockComeState::SlaveSentDataNowReady;
    let mut data_from_slave: ExchangedDataT = DATA_FIRST_AND_INC;
    let mut data_from_master: ExchangedDataT = 0; // History variable for SpontaneousData;

    loop {
        let random_millis: u64 = {
            let mut rng = rand::rng();
            rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS)
        };

        let local_timer = sleep(Duration::from_millis(random_millis));

        tokio::select! {
            biased;

            // CASE 1: Receive from master (Always active)
            spontaneous_data_or_come = ch_come_or_sdata_rx.recv_async() => {
                if let Ok(msg) = spontaneous_data_or_come {
                    match msg {
                        Message::SpontaneousData { val } => {
                            assert_eq!(
                                val,
                                data_from_master + DATA_FIRST_AND_INC,
                                "[Slave] Data sequence gap detected in SpontaneousData!"
                            );

                            // Update history tracking for spontaneous data
                            data_from_master = val;
                            println_iff(LogLevel::All, format_args!("[Slave] Processed spontaneous data from Master: {}", data_from_master));
                        }
                        Message::Come => {
                            state = slave_set_knock_come_state(state, KnockComeState::SlaveGotCome);
                            let after_knock_come_the_data = Message::SlaveData { val: data_from_slave }; // .try_send not needed here
                            let _ = ch_come_tx.send_async(after_knock_come_the_data).await; // .try_send not needed here
                            println_iff(LogLevel::All, format_args!("[Slave] Handshake complete. Sent SlaveData: {}", data_from_slave));
                            data_from_slave += DATA_FIRST_AND_INC;
                            state = slave_set_knock_come_state(state, KnockComeState::SlaveSentDataNowReady);
                        }
                        _ => panic!("[Slave] Unexpected packet type received!"),
                    }
                } else {
                    break;
                }
            }

            // CASE 2: Local Timer
            _ = local_timer, if state == KnockComeState::SlaveSentDataNowReady => {
                let _ = ch_knock_tx.send_async(()).await; // .try_send not needed here
                state = slave_set_knock_come_state(state, KnockComeState::SlaveSentKnock);
                println_iff(LogLevel::All, format_args!("[Slave] Local timeout tick. Knock signal sent! State -> SlaveSentKnock"));
            }
        }
    }
}

/// Implements the master task in the Knock-Come pattern using a non-blocking try-send approach.
///
/// This task listens for incoming transaction requests from the slave and handles
/// spontaneous data transmission when its own timeouts expire.
///
/// The try_send is needed to avoid a deadlock that may occur with task_slave since the lower level of
/// the single tokio:select in task_slave are some times not uniquely defined when one component is a timeout.
/// This gives rise to the "fractally reappering problem" as described in
/// https://www.teigfam.net/oyvind/home/technology/009-the-knock-come-deadlock-free-pattern/#fractally_reappearing_problem
/// This is solved with task_slave_nested_select and task_b_master_send
///
/// # Channels
/// * `ch_knock_rx` - Receives the asynchronous "knock" signal from the slave initiating a transaction.
/// * `ch_come_or_sdata_tx` - Transmits either a "come" authorization response or spontaneous data to the slave.
/// * `ch_come_rx` - Receives the actual payload data from the slave after the rendezvous is established.
///
/// CODE FOR USE_NESTED_SELECT == 0
async fn task_master_try_send(
    ch_knock_rx: flume::Receiver<()>,
    ch_come_or_sdata_tx: flume::Sender<Message>,
    ch_come_rx: flume::Receiver<Message>,
) {
    print_welcome(); // Always

    let mut data_from_master: ExchangedDataT = DATA_FIRST_AND_INC;
    let mut data_from_slave: ExchangedDataT = 0; // So that the first received is DATA_FIRST_AND_INC more 
    let mut cnts = Cnts::default();
    let mut state = KnockComeState::MasterGotDataNowReady;

    print_and_clear_debug_cnts(0, MeTaskT::Master, &mut cnts);

    loop {
        let random_millis = {
            let mut rng = rand::rng();
            rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS)
        };

        // Renamed to local_timer to match the slave exactly
        let local_timer = sleep(Duration::from_millis(random_millis));

        // biased; matches your ORDERED_PRI_SELECT from XC perfectly!
        // Incoming Knocks are ALWAYS prioritized over the local timer.

        // We use tokio::select! instead of flume::Selector to get strict event priority (PRI ALT / [[ordered]] select).
        // Flume's lack of ordering caused race-condition deadlocks when timeouts and channel events overlapped.
        // Additionally, Tokio's native sleep avoids the overhead of spawning background tasks for timers.

        tokio::select! {
            biased;

            // CASE 1: Receive Knock from Slave
            knock_res = ch_knock_rx.recv_async() =>
            {
                if let Ok(()) = knock_res {
                    println_iff(LogLevel::All, format_args!("[Master] Received KNOCK from slave."));
                    state = master_set_knock_come_state(state, KnockComeState::MasterGotKnock);

                    // Transmit the clean COME signal to the slave without any payload
                    let _ = ch_come_or_sdata_tx.send_async(Message::Come).await; // .try_send not needed here

                    state = master_set_knock_come_state(state, KnockComeState::MasterSentCome);

                    // Receive the synchronous reply from the slave
                    let after_knock_come_the_data = ch_come_rx.recv_async().await;

                    // Verify packet type and payload (matches xassert logic in XC)
                    match after_knock_come_the_data {
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
                            // Update statistics tracking (equivalent to XC metrics)
                            cnts.rec_cnt += 1;
                            cnts.rec_sent_cnt += 1;
                            cnts.sum_rec_cnt += 1;
                            // Calculate and evaluate protocol fairness
                            // Update fairness metrics and check if it's time to print and reset interval counters
                            update_fairness_cnts(&mut cnts);
                            if data_from_slave % MAX_SUM_CNT == 0 { // was cnts.rec_sent_cnt ==
                                print_and_clear_debug_cnts(1, MeTaskT::Master, &mut cnts);
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
            _ = local_timer =>
            {
                // Create the message with the CURRENT value first
                let spontaneous_data = Message::SpontaneousData { val: data_from_master };

                if let Ok(()) = ch_come_or_sdata_tx.try_send(spontaneous_data) { // Not .send_async().await here, to avoid deadlock, even if slave alwaays is ready
                    println_iff(LogLevel::All, format_args!("[Master] Local timeout tick. Sent spontaneous data: {}", data_from_master));
                    // INCREMENT AFTER SENDING (Matches your protocol requirement)
                    data_from_master += DATA_FIRST_AND_INC;

                    // Update statistics tracking
                    cnts.sent_cnt += 1;
                    cnts.rec_sent_cnt += 1;
                    cnts.sum_sent_cnt += 1;
                    update_fairness_cnts(&mut cnts);
                    if data_from_master % MAX_SUM_CNT == 0 { // was cnts.rec_sent_cnt ==
                        print_and_clear_debug_cnts(2, MeTaskT::Master, &mut cnts);
                    } else { }
                } else {
                    cnts.avoid_deadlock_cnt += 1;
                    // try_send here is the only way to protect against tokio scheduler delays, since it only sees a queue, not a time.
                    // In software simulation, if a simultaneous timeout occurs in task_slave
                    // it might be transitioning between loop iterations and
                    // not actively polling the rendezvous channel at this exact microsecond.
                    // We discard the spontaneous data atomically to avoid a software-induced
                    // deadlock, allowing task_b_master to process the pending KNOCK on the next loop.

                    // See https://www.teigfam.net/oyvind/home/technology/009-the-knock-come-deadlock-free-pattern/#fractally_reappearing_problem
                    // We could have done let sleep(Duration::0)); above be zero here, and the "busy poll send" could have used "newer" data.
                }
            }
        }
    }
}

// =============================================================================================
// task_slave_nested_select <--
// task_b_master_send
// =============================================================================================
//
// CODE FOR USE_NESTED_SELECT == 1
async fn task_slave_nested_select(
    ch_knock_tx: flume::Sender<()>,
    ch_come_or_sdata_rx: flume::Receiver<Message>,
    ch_come_tx: flume::Sender<Message>,
) {
    let mut state = KnockComeState::SlaveSentDataNowReady;
    let mut data_from_slave: ExchangedDataT = DATA_FIRST_AND_INC;
    let mut data_from_master: ExchangedDataT = 0; // History variable for SpontaneousData
    let mut cnts = Cnts::default();

    print_and_clear_debug_cnts(20, MeTaskT::Slave, &mut cnts);

    loop {
        let random_millis: u64 = {
            let mut rng = rand::rng();
            rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS)
        };

        let local_timer = sleep(Duration::from_millis(random_millis));

        tokio::select! {
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
                                "[Slave] Data sequence gap detected in SpontaneousData!"
                            );
                            cnts.spontaneous_datas += 1;
                            // Update history tracking for spontaneous data
                            data_from_master = val;
                            println_iff(LogLevel::All, format_args!("[Slave] Processed spontaneous data from Master: {}", data_from_master));

                            if data_from_master % MAX_SUM_CNT == 0 {
                                print_and_clear_debug_cnts(21, MeTaskT::Slave, &mut cnts);
                            } else { }
                        }
                        Message::Come => {
                            panic!("[Slave] Spontaneous Come not allowed");
                        }
                        _ => panic!("[Slave] Unexpected packet type received!"),
                    }
                } else {
                    panic!("[Slave] msg not ok");
                }
            }

            // CASE 2: Local Timer
            _ = local_timer, if state == KnockComeState::SlaveSentDataNowReady => {

                let _ = ch_knock_tx.send_async(()).await; // .try_send not needed here
                cnts.knocks += 1;
                state = slave_set_knock_come_state(state, KnockComeState::SlaveSentKnock);
                println_iff(LogLevel::All, format_args!("[Slave] Local timeout tick. Knock signal sent! State -> SlaveSentKnock"));

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
                                            "[Slave] Data sequence gap detected in SpontaneousData!"
                                        );
                                        cnts.spontaneous_datas_2 += 1;

                                        if data_from_master % MAX_SUM_CNT == 0{
                                            print_and_clear_debug_cnts(22, MeTaskT::Slave, &mut cnts);
                                        } else { }
                                        // Update history tracking for spontaneous data
                                        data_from_master = val;
                                        println_iff(LogLevel::All, format_args!("[Slave] Processed spontaneous data from Master: {}", data_from_master));
                                        // NOT break 'await_come; since we must stay tuned until Come has been received
                                    }
                                    Message::Come => {
                                        cnts.comes += 1;
                                        state = slave_set_knock_come_state(state, KnockComeState::SlaveGotCome);
                                        let after_knock_come_the_data = Message::SlaveData { val: data_from_slave };
                                        let _ = ch_come_tx.send_async(after_knock_come_the_data).await; // .try_send not needed here
                                        cnts.datas += 1;
                                        println_iff(LogLevel::All, format_args!("[Slave] Handshake complete. Sent SlaveData: {}", data_from_slave));
                                        data_from_slave += DATA_FIRST_AND_INC;
                                        state = slave_set_knock_come_state(state, KnockComeState::SlaveSentDataNowReady);
                                        break 'await_come; // Finished
                                    }
                                    _ => panic!("[Slave] Come or sdata expected!")
                                }
                            } else {
                                panic!("[Slave] msg not ok");
                            }
                        }
                    }
                }
            }
        }
    }
}

// =============================================================================================
// task_slave_nested_select
// task_b_master_send <--
// =============================================================================================
// CODE FOR USE_NESTED_SELECT == 1
async fn task_b_master_send(
    ch_knock_rx: flume::Receiver<()>,
    ch_come_or_sdata_tx: flume::Sender<Message>,
    ch_come_rx: flume::Receiver<Message>,
) {
    // TODO: THE BODY IN THIS CODE WILL BE REPLACED WITH A PROPER VERSION

    print_welcome(); // Always

    let mut data_from_master: ExchangedDataT = DATA_FIRST_AND_INC;
    let mut data_from_slave: ExchangedDataT = 0; // So that the first received is DATA_FIRST_AND_INC more 
    let mut cnts = Cnts::default();
    let mut state = KnockComeState::MasterGotDataNowReady;

    print_and_clear_debug_cnts(30, MeTaskT::Master, &mut cnts);

    loop {
        let random_millis = {
            let mut rng = rand::rng();
            rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS)
        };

        // Renamed to local_timer to match the slave exactly
        let local_timer = sleep(Duration::from_millis(random_millis));

        // biased; matches your ORDERED_PRI_SELECT from XC perfectly!
        // Incoming Knocks are ALWAYS prioritized over the local timer.

        // We use tokio::select! instead of flume::Selector to get strict event priority (PRI ALT / [[ordered]] select).
        // Flume's lack of ordering caused race-condition deadlocks when timeouts and channel events overlapped.
        // Additionally, Tokio's native sleep avoids the overhead of spawning background tasks for timers.

        tokio::select! {
            biased;

            // CASE 1: Receive Knock from Slave
            knock_res = ch_knock_rx.recv_async() =>
            {
                if let Ok(()) = knock_res {
                    println_iff(LogLevel::All, format_args!("[Master] Received KNOCK from slave."));
                    state = master_set_knock_come_state(state, KnockComeState::MasterGotKnock);

                    cnts.knocks += 1;

                    // Transmit the clean COME signal to the slave without any payload
                    let _ = ch_come_or_sdata_tx.send_async(Message::Come).await; // .try_send not needed here

                    cnts.comes += 1;

                    state = master_set_knock_come_state(state, KnockComeState::MasterSentCome);

                    // Receive the synchronous reply from the slave
                    let after_knock_come_the_data = ch_come_rx.recv_async().await;

                    // Verify packet type and payload (matches xassert logic in XC)
                    match after_knock_come_the_data {
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
                            // Update statistics tracking (equivalent to XC metrics)
                            cnts.rec_cnt += 1;
                            cnts.rec_sent_cnt += 1;
                            cnts.sum_rec_cnt += 1;
                            cnts.datas += 1;
                            // Calculate and evaluate protocol fairness
                            // Update fairness metrics and check if it's time to print and reset interval counters
                            update_fairness_cnts(&mut cnts);
                            if cnts.rec_sent_cnt == MAX_SUM_CNT {
                                print_and_clear_debug_cnts(31, MeTaskT::Master, &mut cnts);
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
            _ = local_timer =>
            {
                // Create the message with the CURRENT value first
                let spontaneous_data = Message::SpontaneousData { val: data_from_master };

                if let Ok(()) = ch_come_or_sdata_tx.send_async(spontaneous_data).await { // with CODE FOR USE_NESTED_SELECT == 1 .try_send not needed
                    println_iff(LogLevel::All, format_args!("[Master] Local timeout tick. Sent spontaneous data: {}", data_from_master));
                    // INCREMENT AFTER SENDING (Matches your protocol requirement)
                    data_from_master += DATA_FIRST_AND_INC;

                    // Update statistics tracking
                    cnts.sent_cnt += 1;
                    cnts.rec_sent_cnt += 1;
                    cnts.sum_sent_cnt += 1;
                    cnts.spontaneous_datas += 1;
                    update_fairness_cnts(&mut cnts);
                    if cnts.rec_sent_cnt == MAX_SUM_CNT {
                        print_and_clear_debug_cnts(32, MeTaskT::Master, &mut cnts);
                    } else { }
                } else {
                    cnts.avoid_deadlock_cnt += 1;
                    // try_send here is the only way to protect against tokio scheduler delays, since it only sees a queue, not a time.
                    // In software simulation, if a simultaneous timeout occurs in task_slave
                    // it might be transitioning between loop iterations and
                    // not actively polling the rendezvous channel at this exact microsecond.
                    // We discard the spontaneous data atomically to avoid a software-induced
                    // deadlock, allowing task_b_master to process the pending KNOCK on the next loop.

                    // See https://www.teigfam.net/oyvind/home/technology/009-the-knock-come-deadlock-free-pattern/#fractally_reappearing_problem
                    // We could have done let sleep(Duration::0)); above be zero here, and the "busy poll send" could have used "newer" data.
                }
            }
        }
    }
}

const CHAN_STREAMING_CAP_1: usize = 1;
const CHAN_SYNCH_CAP_0: usize = 0;

#[tokio::main]
async fn main() {
    let (ch_knock_tx, ch_knock_rx) = flume::bounded::<()>(CHAN_STREAMING_CAP_1);
    let (ch_come_or_sdata_tx, ch_come_or_sdata_rx) = flume::bounded::<Message>(CHAN_SYNCH_CAP_0);
    let (ch_come_tx, ch_come_rx) = flume::bounded::<Message>(CHAN_SYNCH_CAP_0);

    if USE_NESTED_SELECT == 0 {
        let task_slave_handle =
            tokio::spawn(task_slave(ch_knock_tx, ch_come_or_sdata_rx, ch_come_tx));

        let task_master_handle_try_send = tokio::spawn(task_master_try_send(
            ch_knock_rx,
            ch_come_or_sdata_tx,
            ch_come_rx,
        ));
        println!(
            "\n task_slave_handle and ntask_master_handle_try_send running in parallel forever\n"
        );

        let _ = tokio::join!(task_slave_handle, task_master_handle_try_send);
    } else if USE_NESTED_SELECT == 1 {
        let task_slave_handle_nested_select = tokio::spawn(task_slave_nested_select(
            ch_knock_tx,
            ch_come_or_sdata_rx,
            ch_come_tx,
        ));
        let task_master_handle_send = tokio::spawn(task_b_master_send(
            ch_knock_rx,
            ch_come_or_sdata_tx,
            ch_come_rx,
        ));
        println!(
            "\ntask_slave_handle_nested_select and task_master_handle_send running in parallel forever\n"
        );

        let _ = tokio::join!(task_slave_handle_nested_select, task_master_handle_send);
    }
}
