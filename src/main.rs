// =============================================================================================
// THE KNOCK-COME DEADLOCK FREE PATTERN
// Øyvind Teig, Trondheim, Norway
//     This was "my" first Rust code. Thanks to pair programming with Google AI!
// Blog note:
//https://www.teigfam.net/oyvind/home/technology/009-the-knock-come-deadlock-free-pattern/
// GitHub:
//https://github.com/Aclassifier/rust_test_knock_come
// VERSIONS / COMMITS
//
const VERSION: &str = "0.0.900";
//
// 21Jun2026 0.0.900 Testing clickable URLs (2) as starting with //https:..
// 21Jun2026 0.0.900 print_welcome_banner like in XC. Using chrono. Plus some comments on the
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

use std::time::Duration;
use tokio::time::sleep;
use rand::Rng;

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
    if CURRENT_LOG_LEVEL == LogLevel::All && (level == LogLevel::All || level == LogLevel::CountersOnly) {
        println!("{}", args);
    } else if CURRENT_LOG_LEVEL == LogLevel::CountersOnly && level == LogLevel::CountersOnly {
        println!("{}", args);
    }
}

// =============================================================================================
// CODE PROPER
// =============================================================================================

const RANDOM_VAL_MIN_MS: u64 =    0; 
const RANDOM_VAL_MAX_MS: u64 =  100; 
const MAX_SUM_CNT:       u32 = 1000;

type ExchangedDataT = u32;
const DATA_FIRST_AND_INC: ExchangedDataT = 1; 

// #[derive(Default)] automatically creates an init function under the hood that sets all u32 fields to 0
use std::time::Instant; // Put this with the other imports at the top of src/main.rs

#[derive(Debug, Clone, Copy)] // Removed Default from here!
struct Cnts {
    pub sent_cnt: u32,
    pub rec_cnt: u32,
    pub rec_sent_cnt: u32,
    pub rec_gt_sent_cnt: u32,
    pub rec_eq_sent_cnt: u32,
    pub rec_lt_sent_cnt: u32,
    pub sum_sent_cnt: u32,
    pub sum_rec_cnt: u32,
    pub avoid_deadlock_cnt: u32,
    pub last_print_time: Instant, // Stores the timestamp of the last printout
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
        }
    }
}

fn print_welcome_banner() {
    // Fetches the current local time from your iMac during startup
    let local_time = chrono::Local::now();
    
    // Formats the date to exactly match your XC style (e.g., 21Jun2026)
    let compile_date = local_time.format("%d%b%Y").to_string();
    let compile_time = local_time.format("%H:%M").to_string();

    println!(
        "Rust KNOCK-COME v{} on date {} {}\n\
         Time random max {} ms, cnt events at {} (Teig)\n",
        VERSION,
        compile_date,
        compile_time,
        RANDOM_VAL_MAX_MS,
        MAX_SUM_CNT
    );
}

fn print_and_clear_debug_cnts(cnts: &mut Cnts) {
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
            "REC {}\t{}\tSENT {}\t(>{}= {} <{})\tSUM (REC {} {} SENT {}) {} {}\tDT {:.2}s",
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
            delta_secs // Injected into the printout
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

#[derive(Copy, Clone, Debug, PartialEq)]
enum KnockComeState {
    SlaveSentDataNowReady, // Also init -> SlaveSentKnock
    SlaveSentKnock,        //           -> SlaveGotCome or SlaveGotSpontaneousData (value not needed)
    SlaveGotCome,          //           -> SlaveSentDataNowReady (atomic)
    MasterGotDataNowReady, // Also init -> MasterGotKnock
    MasterGotKnock,        //           -> MasterSentCome
    MasterSentCome         //           -> MasterGotDataNowReady (atomic)
}

// In Rust, 'const' in parameters is not used. Variables are immutable by default.
fn slave_set_knock_come_state(present_state: KnockComeState, new_state: KnockComeState) -> KnockComeState {
    
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
            _ => panic!("Slave attempted to transition to an invalid state: {:?}", new_state),
        }
    }

    // Return the new state (no 'return' keyword needed on the last line in Rust)
    new_state
}

fn master_set_knock_come_state(present_state: KnockComeState, new_state: KnockComeState) -> KnockComeState {
    
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
            _ => panic!("Master attempted to transition to an invalid state: {:?}", new_state),
        }
    }

    // Return the new state implicitly by omitting the semicolon
    new_state
}

// Equivalent to task_a_slave in XC
async fn task_a_slave(
    ch_ab_knock:        flume::Sender<()>, 
    ch_ba_come_or_data: flume::Receiver<Message>, 
    ch_ab_data:         flume::Sender<Message>,
) {    
    let mut state = KnockComeState::SlaveSentDataNowReady;
    let mut data_from_task_a_slave: ExchangedDataT = DATA_FIRST_AND_INC; 
    let mut data_from_task_b_master: ExchangedDataT = 0; // History variable for SpontaneousData
    
    loop {
        let random_millis: u64 = {
            let mut rng = rand::rng();
            rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS)
        };

        let local_timer = sleep(Duration::from_millis(random_millis));

        tokio::select! {
            biased;

            // CASE 1: Receive from master (Always active)
            spontaneous_data_or_come = ch_ba_come_or_data.recv_async() => {
                if let Ok(msg) = spontaneous_data_or_come {
                    match msg {
                        Message::SpontaneousData { val } => {
                            // CORRECTED: Verify sequence only for actual spontaneous data stream
                            assert_eq!(
                                val, 
                                data_from_task_b_master + DATA_FIRST_AND_INC,
                                "[Slave] Data sequence gap detected in SpontaneousData!"
                            );
                            
                            // Update history tracking for spontaneous data
                            data_from_task_b_master = val;
                            println_iff(LogLevel::All, format_args!("[Slave] Processed spontaneous data from Master: {}", data_from_task_b_master));
                        }
                        Message::Come => {
                            state = slave_set_knock_come_state(state, KnockComeState::SlaveGotCome);                           
                            let after_knock_come_the_data = Message::SlaveData { val: data_from_task_a_slave }; // .try_send not needed here
                            let _ = ch_ab_data.send_async(after_knock_come_the_data).await; // .try_send not needed here        
                            println_iff(LogLevel::All, format_args!("[Slave] Handshake complete. Sent SlaveData: {}", data_from_task_a_slave));                 
                            data_from_task_a_slave += DATA_FIRST_AND_INC; 
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
                let _ = ch_ab_knock.send_async(()).await; // .try_send not needed here
                state = slave_set_knock_come_state(state, KnockComeState::SlaveSentKnock);
                println_iff(LogLevel::All, format_args!("[Slave] Local timeout tick. Knock signal sent! State -> SlaveSentKnock"));
            }
        }
    }
}

// Equivalent to task_b_master in XC
async fn task_b_master(
    ch_ab_knock_rx:     flume::Receiver<()>, 
    ch_ab_data:         flume::Sender<Message>, 
    ch_ba_come_or_data: flume::Receiver<Message>, 
) {
    print_welcome_banner(); // Always

    let mut data_from_task_b_master: ExchangedDataT = DATA_FIRST_AND_INC; 
    let mut data_from_task_a_slave: ExchangedDataT =   0; // So that the first received is DATA_FIRST_AND_INC more 
    let mut cnts = Cnts::default(); 
    let mut state = KnockComeState::MasterGotDataNowReady;

    print_and_clear_debug_cnts(&mut cnts);

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
            knock_res = ch_ab_knock_rx.recv_async() => {
                if let Ok(()) = knock_res {
                    println_iff(LogLevel::All, format_args!("[Master] Received KNOCK from slave."));
                    state = master_set_knock_come_state(state, KnockComeState::MasterGotKnock);
                    
                    // Transmit the clean COME signal to the slave without any payload
                    let _ = ch_ab_data.send_async(Message::Come).await; // .try_send not needed here

                    state = master_set_knock_come_state(state, KnockComeState::MasterSentCome);

                    // Receive the synchronous reply from the slave
                    let after_knock_come_the_data = ch_ba_come_or_data.recv_async().await;

                    // Verify packet type and payload (matches xassert logic in XC)
                    match after_knock_come_the_data {
                        Ok(Message::SlaveData { val }) => {
                            // Verify that incoming slave data matches history + incremental step
                            assert_eq!(
                                val,
                                data_from_task_a_slave + DATA_FIRST_AND_INC,
                                "[Master] Data sequence gap detected in SlaveData!"
                            );
                            // Update history tracking for slave data
                            data_from_task_a_slave = val;
                            println_iff(LogLevel::All, format_args!("[Master] Handshake complete! Captured SlaveData: {}", data_from_task_a_slave));                          
                            // Update statistics tracking (equivalent to XC metrics)
                            cnts.rec_cnt += 1;
                            cnts.rec_sent_cnt += 1;
                            cnts.sum_rec_cnt += 1;                            
                            // Calculate and evaluate protocol fairness
                                              // Update fairness metrics and check if it's time to print and reset interval counters
                            update_fairness_cnts(&mut cnts);
                            if cnts.rec_sent_cnt == MAX_SUM_CNT {
                                print_and_clear_debug_cnts(&mut cnts);
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
            _ = local_timer => {
                // Create the message with the CURRENT value first
                let spontaneous_data = Message::SpontaneousData { val: data_from_task_b_master };
                
                if let Ok(()) = ch_ab_data.try_send(spontaneous_data) { // Not .send_async().await here, to avoid deadlock, even if slave alwaays is ready
                    println_iff(LogLevel::All, format_args!("[Master] Local timeout tick. Sent spontaneous data: {}", data_from_task_b_master));                    
                    // INCREMENT AFTER SENDING (Matches your protocol requirement)
                    data_from_task_b_master += DATA_FIRST_AND_INC;
                    
                    // Update statistics tracking
                    cnts.sent_cnt += 1;
                    cnts.rec_sent_cnt += 1;
                    cnts.sum_sent_cnt += 1;
                    update_fairness_cnts(&mut cnts);
                    if cnts.rec_sent_cnt == MAX_SUM_CNT {
                        print_and_clear_debug_cnts(&mut cnts);
                    } else { }
                } else {
                    cnts.avoid_deadlock_cnt += 1; 
                    // try_send here is the only way to protect against tokio scheduler delays, since it only sees a queue, not a time.
                    // In software simulation, if a simultaneous timeout occurs in task_a_slave
                    // it might be transitioning between loop iterations and 
                    // not actively polling the rendezvous channel at this exact microsecond.
                    // We discard the spontaneous data atomically to avoid a software-induced 
                    // deadlock, allowing task_b_master to process the pending KNOCK on the next loop.

                    // See
                    //https://www.teigfam.net/oyvind/home/technology/009-the-knock-come-deadlock-free-pattern/#fractally_reappearing_problem
                    // We could have done let sleep(Duration::0)); above be zero here, and the "busy poll send" could have used "newer" data.
                }

            }
        }
    }
}


const CHAN_STREAMING_CAP_1: usize = 1;
const CHAN_SYNCH_CAP_0:     usize = 0; 

#[tokio::main]
async fn main() {
    let (slave_to_master_knock_tx, slave_to_master_knock_rx) = flume::bounded::<()>     (CHAN_STREAMING_CAP_1);
    let (master_to_slave_tx,  master_to_slave_rx)  = flume::bounded::<Message>(CHAN_SYNCH_CAP_0);
    let (slave_to_master_tx,  slave_to_master_rx)  = flume::bounded::<Message>(CHAN_SYNCH_CAP_0);

    let task_a_slave_handle = tokio::spawn(task_a_slave(
        slave_to_master_knock_tx, 
        master_to_slave_rx, 
        slave_to_master_tx
    ));
    
    let task_b_master_handle = tokio::spawn(task_b_master(
        slave_to_master_knock_rx,
        master_to_slave_tx, 
        slave_to_master_rx
    ));

    println!("\ntask_a_slave_handle and task_b_master_handle running in parallel forever\n");

    let _ = tokio::join!(task_a_slave_handle, task_b_master_handle);
}
