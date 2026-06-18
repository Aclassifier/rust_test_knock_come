// =============================================================================================
// VERSIONS / COMMITS
// Øyvind Teig, Trondheim, Norway
// This was "my" first Rust code. Thanks to pair programming with Google AI!
// See https://www.teigfam.net/oyvind/home/technology/009-the-knock-come-deadlock-free-pattern/
// =============================================================================================
// 
const VERSION: &str = "0.0.210";
//
// 18Jun2026 0.0.210 New heading above
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
const CURRENT_LOG_LEVEL: LogLevel = LogLevel::All;

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

const RANDOM_VAL_MIN_MS:  u64 =   0; 
const RANDOM_VAL_MAX_MS:  u64 = 100; 

type ExchangedDataT = u32;
const DATA_FIRST_AND_INC: ExchangedDataT = 1; 

// #[derive(Default)] automatically creates an init function under the hood that sets all u32 fields to 0
#[derive(Default, Debug, Clone, Copy)]
struct Cnts {
    pub sent_cnt: u32,
    pub rec_cnt: u32,
    pub rec_sent_cnt: u32,
    pub rec_gt_sent_cnt: u32,
    pub rec_eq_sent_cnt: u32,
    pub rec_lt_sent_cnt: u32,
    pub sum_sent_cnt: u32,
    pub sum_rec_cnt: u32,
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
    Come,                                             
    ComeData { val: ExchangedDataT }, 
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
    ch_ab_knock_tx: flume::Sender<()>, 
    ch_ab_bidir_rx: flume::Receiver<Message>, 
    ch_ab_bidir_tx: flume::Sender<Message>,
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
            msg_res = ch_ab_bidir_rx.recv_async() => {
                if let Ok(msg) = msg_res {
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
                            let reply = Message::SlaveData { val: data_from_task_a_slave };
                            let _ = ch_ab_bidir_tx.send_async(reply).await;         
                            println_iff(LogLevel::All, format_args!("[Slave] Handshake complete. Sent SlaveData: {}", data_from_task_a_slave));                 
                            data_from_task_a_slave += DATA_FIRST_AND_INC; 
                            state = slave_set_knock_come_state(state, KnockComeState::SlaveSentDataNowReady);
                        }
                        Message::ComeData { val } => {
                            // Piggy-backed data is uninteresting, skip assert and history tracking  
                            println_iff(LogLevel::All, format_args!("[Slave] Received COME_DATA. Piggy-backed value {} ignored.", val));                        
                            state = slave_set_knock_come_state(state, KnockComeState::SlaveGotCome);                           
                            let reply = Message::SlaveData { val: data_from_task_a_slave };
                            let _ = ch_ab_bidir_tx.send_async(reply).await;
                            println_iff(LogLevel::All, format_args!("[Slave] Handshake complete (COME_DATA). Sent SlaveData: {}", data_from_task_a_slave));                                                                                                
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
                let _ = ch_ab_knock_tx.send_async(()).await; 
                state = slave_set_knock_come_state(state, KnockComeState::SlaveSentKnock);
                println_iff(LogLevel::All, format_args!("[Slave] Local timeout tick. Knock signal sent! State -> SlaveSentKnock"));
            }
        }
    }
}


// Equivalent to task_b_master in XC
async fn task_b_master(
    ch_ab_knock_rx: flume::Receiver<()>, 
    ch_ab_bidir_tx: flume::Sender<Message>, 
    ch_ab_bidir_rx: flume::Receiver<Message>, 
) {
    let mut data_from_task_b_master: ExchangedDataT = DATA_FIRST_AND_INC; 
    let mut data_from_task_a_slave: ExchangedDataT =   0; // So that the first received is DATA_FIRST_AND_INC more 
    let mut my_cnts = Cnts::default(); 
    let mut state = KnockComeState::MasterGotDataNowReady;

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
                    let response = if random_millis % 2 == 0 {
                        let data_from_task_b_dummy_for_come: ExchangedDataT = 0;
                        Message::ComeData { val: data_from_task_b_dummy_for_come }
                    } else {
                        Message::Come
                    };

                    // Transmit the COME / COME_DATA response to the slave
                    let _ = ch_ab_bidir_tx.send_async(response).await; 
                    state = master_set_knock_come_state(state, KnockComeState::MasterSentCome);

                    // Receive the synchronous reply from the slave
                    let received_res = ch_ab_bidir_rx.recv_async().await;

                    // Verify packet type and payload (matches xassert logic in XC)
                    match received_res {
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
                            my_cnts.rec_cnt += 1;
                            my_cnts.rec_sent_cnt += 1;
                            my_cnts.sum_rec_cnt += 1;                            
                            // Calculate and evaluate protocol fairness
                            update_fairness_cnts(&mut my_cnts);
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
                let spontaneous_msg = Message::SpontaneousData { val: data_from_task_b_master };
                
                if let Ok(()) = ch_ab_bidir_tx.try_send(spontaneous_msg) {
                    println_iff(LogLevel::All, format_args!("[Master] Local timeout tick. Sent spontaneous data: {}", data_from_task_b_master));                    
                    // INCREMENT AFTER SENDING (Matches your protocol requirement)
                    data_from_task_b_master += DATA_FIRST_AND_INC;
                    
                    // Update statistics tracking
                    my_cnts.sent_cnt += 1;
                    my_cnts.rec_sent_cnt += 1;
                    my_cnts.sum_sent_cnt += 1;
                    update_fairness_cnts(&mut my_cnts);
                } else {
                    // Discard silently if slave is busy, avoiding structural deadlocks
                }
            }
        }
    }
}


const CHAN_STREAMING_CAP_1: usize = 1;
const CHAN_SYNCH_CAP_0:     usize = 0; 

#[tokio::main]
async fn main() {
    let (ch_ab_knock_tx, ch_ab_knock_rx) = flume::bounded::<()>(CHAN_STREAMING_CAP_1);
    let (master_to_slave_tx, master_to_slave_rx) = flume::bounded::<Message>(CHAN_SYNCH_CAP_0);
    let (slave_to_master_tx, slave_to_master_rx) = flume::bounded::<Message>(CHAN_SYNCH_CAP_0);

    let task_a_slave_handle = tokio::spawn(task_a_slave(
        ch_ab_knock_tx, 
        master_to_slave_rx, 
        slave_to_master_tx
    ));
    
    let task_b_master_handle = tokio::spawn(task_b_master(
        ch_ab_knock_rx, 
        master_to_slave_tx, 
        slave_to_master_rx
    ));

    println!("\nKnock-come running v{}. Tasks started in a PAR-equivalent block\n", VERSION);

    let _ = tokio::join!(task_a_slave_handle, task_b_master_handle);
}
