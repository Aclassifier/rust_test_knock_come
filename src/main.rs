// =============================================================================================
// VERSIONS / COMMITS
// =============================================================================================
// 
const VERSION: &str = "0.0.101"; 
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

const RANDOM_VAL_MIN_MS: u64 =   0; 
const RANDOM_VAL_MAX_MS: u64 = 100; 

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
    // KnockNoData not needed. Usage ch_ab_knock_tx.send_async(()).await :> ch_ab_knock_rx.recv_async() with unit type
    SpontaneousData { data_from_task_b_master: u32 }, 
    Come,                                             
    ComeData { data_from_task_b_master: u32 }, 
    SlaveData { data_from_task_a_slave: u32 }, 
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
    let mut data_from_task_a_slave: u32 = 10; 
    let mut _data_from_task_b_master: u32; 
    
    loop {
        let random_millis: u64 = {
            let mut rng = rand::rng();
            rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS)
        };

        // We use a native Tokio timer instead of a heavy background task
        let local_timer = sleep(Duration::from_millis(random_millis));

        // biased; matches your ORDERED_PRI_SELECT from XC perfectly!
        // Channel reception is ALWAYS prioritized over the local timer.
        tokio::select! {
            biased;

            // CASE 1: Receive from master (Always active)
            msg_res = ch_ab_bidir_rx.recv_async() => {
                if let Ok(msg) = msg_res {
                    match msg {
                        Message::SpontaneousData { data_from_task_b_master } => {
                            _data_from_task_b_master = data_from_task_b_master;
                            println!("[Slave] Processed spontaneous data from Master: {}", _data_from_task_b_master);
                        }
                        Message::Come => {
                            // Check previous state and transition to SlaveGotCome
                            state = slave_set_knock_come_state(state, KnockComeState::SlaveGotCome);
                            
                            let reply = Message::SlaveData { data_from_task_a_slave };
                            let _ = ch_ab_bidir_tx.send_async(reply).await;
                            println!("[Slave] Handshake complete (Pure COME). Sent SlaveData: {}", data_from_task_a_slave);
                            
                            data_from_task_a_slave += 10; 
                            
                            // Check previous state and return to initial ready state
                            state = slave_set_knock_come_state(state, KnockComeState::SlaveSentDataNowReady);
                        }
                        Message::ComeData { data_from_task_b_master } => {
                            _data_from_task_b_master = data_from_task_b_master;
                            println!("[Slave] Processed piggy-backed data from Master: {}", _data_from_task_b_master);
                            
                            // Check previous state and transition to SlaveGotCome
                            state = slave_set_knock_come_state(state, KnockComeState::SlaveGotCome);
                            
                            let reply = Message::SlaveData { data_from_task_a_slave };
                            let _ = ch_ab_bidir_tx.send_async(reply).await;
                            println!("[Slave] Handshake complete (COME_DATA). Sent SlaveData: {}", data_from_task_a_slave);
                            
                            data_from_task_a_slave += 10; 
                            
                            // Check previous state and return to initial ready state
                            state = slave_set_knock_come_state(state, KnockComeState::SlaveSentDataNowReady);
                        }
                        _ => panic!("[Slave] Unexpected packet type received!"),
                    }
                } else {
                    break; // Channel closed
                }
            }

            // CASE 2: Local Timer (Only triggers if we haven't sent a knock yet, matching XC)
            _ = local_timer, if state == KnockComeState::SlaveSentDataNowReady => {
                let _ = ch_ab_knock_tx.send_async(()).await; 
                
                // Check previous state and transition to SlaveSentKnock
                state = slave_set_knock_come_state(state, KnockComeState::SlaveSentKnock);
                println!("[Slave] Local timeout tick. Knock signal sent! State -> SlaveSentKnock");
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
    let mut master_data_counter: u32 = 100;
    let mut my_cnts = Cnts::default(); 
    let mut state = KnockComeState::MasterGotDataNowReady;

    loop {
        let random_millis = {
            let mut rng = rand::rng();
            rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS) 
        };

        let local_timer = sleep(Duration::from_millis(random_millis));

        // biased; matches your ORDERED_PRI_SELECT from XC perfectly!
        // Incoming Knocks are ALWAYS prioritized over the watchdog timer.

        // We use tokio::select! instead of flume::Selector to get strict event priority (PRI ALT / [[ordered]] select). 
        // Flume's lack of ordering caused race-condition deadlocks when timeouts and channel events overlapped.
        // Additionally, Tokio's native sleep avoids the overhead of spawning background tasks for timers.

        tokio::select! {
            biased;

            // CASE 1: Receive Knock from Slave
            knock_res = ch_ab_knock_rx.recv_async() => {
                if let Ok(()) = knock_res {
                    println!("[Master] Received KNOCK from slave.");
                    state = master_set_knock_come_state(state, KnockComeState::MasterGotKnock);
                    
                    let response = if random_millis % 2 == 0 {
                        master_data_counter += 10;
                        Message::ComeData { data_from_task_b_master: master_data_counter }
                    } else {
                        Message::Come
                    };

                    // FIXED: Actually transmit the COME / COME_DATA response to the slave
                    let _ = ch_ab_bidir_tx.send_async(response).await; 
                    state = master_set_knock_come_state(state, KnockComeState::MasterSentCome);

                    // Receive the synchronous reply from the slave
                    let received_res = ch_ab_bidir_rx.recv_async().await;

                    // Verify packet type and payload (matches xassert logic in XC)
                    match received_res {
                        Ok(Message::SlaveData { data_from_task_a_slave }) => {
                            println!("[Master] Handshake complete! Captured SlaveData: {}", data_from_task_a_slave);
                            
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

            // CASE 2: Watchdog Timer Ticked
            _ = local_timer => {
                master_data_counter += 10;
                let spontaneous_msg = Message::SpontaneousData { data_from_task_b_master: master_data_counter };
                
                // Use try_send to match the non-blocking nature of spontaneous data delivery
                if let Ok(()) = ch_ab_bidir_tx.try_send(spontaneous_msg) {
                    println!("[Master] Watchdog timeout. Sent spontaneous data: {}", master_data_counter);
                    
                    // Update statistics tracking (equivalent to XC metrics)
                    my_cnts.sent_cnt += 1;
                    my_cnts.rec_sent_cnt += 1;
                    my_cnts.sum_sent_cnt += 1;
                    
                    // Calculate and evaluate protocol fairness
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
