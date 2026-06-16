// =============================================================================================
// VERSIONS / COMMITS
// =============================================================================================
// 
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

#[derive(Clone, Debug, PartialEq)]
enum Message {
    SpontaneousData { data_from_task_b_master: u32 }, 
    Come,                                             
    ComeData { data_from_task_b_master: u32 }, 
    SlaveData { data_from_task_a_slave: u32 }, 
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum KnockComeState {
    SlaveSentDataNowReady = 0x1A, 
    SlaveSentKnock        = 0x1B, 
    SlaveGotCome          = 0x1C, 
}

// Equivalent to task_a_slave in XC
async fn task_a_slave(
    ch_ab_knock_tx: flume::Sender<()>, 
    ch_ab_bidir_rx: flume::Receiver<Message>, 
    ch_ab_bidir_tx: flume::Sender<Message>,
) {    
    let mut knock_come_state = KnockComeState::SlaveSentDataNowReady;
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
                            knock_come_state = KnockComeState::SlaveGotCome; 
                            // value assigned to `knock_come_state` is never read
                            // `#[warn(unused_assignments)]` (part of `#[warn(unused)]`) on by default
                            
                            let reply = Message::SlaveData { data_from_task_a_slave };
                            let _ = ch_ab_bidir_tx.send_async(reply).await;
                            println!("[Slave] Handshake complete (Pure COME). Sent SlaveData: {}", data_from_task_a_slave);
                            
                            data_from_task_a_slave += 10; 
                            knock_come_state = KnockComeState::SlaveSentDataNowReady;
                        }
                        Message::ComeData { data_from_task_b_master } => {
                            _data_from_task_b_master = data_from_task_b_master;
                            // value assigned to `knock_come_state` is never read
                            // this value is reassigned later and never used
                            println!("[Slave] Processed piggy-backed data from Master: {}", _data_from_task_b_master);
                            
                            knock_come_state = KnockComeState::SlaveGotCome;
                            
                            let reply = Message::SlaveData { data_from_task_a_slave };
                            let _ = ch_ab_bidir_tx.send_async(reply).await;
                            println!("[Slave] Handshake complete (COME_DATA). Sent SlaveData: {}", data_from_task_a_slave);
                            
                            data_from_task_a_slave += 10; 
                            knock_come_state = KnockComeState::SlaveSentDataNowReady;
                        }
                        _ => panic!("[Slave] Unexpected packet type received!"),
                    }
                } else {
                    break; // Channel closed
                }
            }

            // CASE 2: Local Timer (Only triggers if we haven't sent a knock yet, matching XC)
            _ = local_timer, if knock_come_state == KnockComeState::SlaveSentDataNowReady => {
                let _ = ch_ab_knock_tx.send_async(()).await; 
                knock_come_state = KnockComeState::SlaveSentKnock;
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

    loop {
        let random_millis = {
            let mut rng = rand::rng();
            rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS) 
        };

        let local_timer = sleep(Duration::from_millis(random_millis));

        // biased; matches your ORDERED_PRI_SELECT from XC perfectly!
        // Incoming Knocks are ALWAYS prioritized over the watchdog timer.
        tokio::select! {
            biased;

            // CASE 1: Receive Knock from Slave
            knock_res = ch_ab_knock_rx.recv_async() => {
                if let Ok(()) = knock_res {
                    println!("[Master] Received KNOCK from slave.");
                    
                    let response = if random_millis % 2 == 0 {
                        master_data_counter += 10;
                        Message::ComeData { data_from_task_b_master: master_data_counter }
                    } else {
                        Message::Come
                    };

                    let _ = ch_ab_bidir_tx.send_async(response).await; 

                    if let Ok(Message::SlaveData { data_from_task_a_slave }) = ch_ab_bidir_rx.recv_async().await {
                        println!("[Master] Handshake complete! Captured SlaveData: {}", data_from_task_a_slave);
                    } else {
                        println!("[Master] Protocol violation during payload rendezvous!");
                        break;
                    }
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

    println!("System running. Tasks joined in a PAR-equivalent block.");

    let _ = tokio::join!(task_a_slave_handle, task_b_master_handle);
}
