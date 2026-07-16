// =============================================================================================
// VERSIONS / COMMITS
// =============================================================================================
// 16Jul2026 0.0.030.1 This deadlocks immediately. See AI-analysis this day. Prints only:
//                     System running. Tasks joined in a PAR-equivalent block.
// 16Jun2026 0.0.030   Knock channel converted to a pure signal channel using unit type (). (No data)
//                     (Google AI mode added most of the above line itself!)
// 16Jun2026 0.0.020   Runs with knock-come, but data are not as wanted
// 15Jun2026 0.0.010   First version, runs but no knock-come

use rand::Rng;
use std::time::Duration;
use tokio::time::sleep;

const RANDOM_VAL_MIN_MS: u64 = 0;
const RANDOM_VAL_MAX_MS: u64 = 100;

// Since tasks run forever, we only need to pass actual data
#[derive(Clone)] // Since the compiler does not know the full functionality of flume::Selector
enum Message {
    SensorData(i32),
}

// Internal enums to map which branch won the selection
enum SlaveEvent {
    ComeOrDataReceived(Result<Message, flume::RecvError>),
    TimeoutOccurred,
}

enum MasterEvent {
    KnockReceived(Result<(), flume::RecvError>), // No data
    TimeoutOccurred,
}

// Equivalent to an XC process running on an XMOS hardware logical core (1 of 8 cores per tile)
async fn task_a_slave(
    ch_ab_knock_tx: flume::Sender<()>, // No data
    ch_ba_come_or_data_rx: flume::Receiver<Message>,
    ch_ab_data_tx: flume::Sender<Message>,
) {
    let mut counter = 0;
    let mut pending_message = Message::SensorData(counter);

    loop {
        let random_millis: u64 = {
            let mut rng = rand::rng();
            rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS)
        };

        let (tx_timer, rx_timer) = flume::bounded::<()>(0);
        tokio::spawn(async move {
            sleep(Duration::from_millis(random_millis)).await;
            let _ = tx_timer.send_async(()).await;
        });

        let event = flume::Selector::new()
            .recv(&ch_ba_come_or_data_rx, |res| {
                SlaveEvent::ComeOrDataReceived(res)
            })
            .recv(&rx_timer, |_| SlaveEvent::TimeoutOccurred)
            .wait();

        match event {
            SlaveEvent::ComeOrDataReceived(Ok(Message::SensorData(data))) => {
                println!("[Slave] Received COME or DATA: {}", data);
                let _ = ch_ab_data_tx.send_async(pending_message.clone()).await;

                counter += 1;
                pending_message = Message::SensorData(counter);
            }
            SlaveEvent::ComeOrDataReceived(Err(_)) => {
                break;
            }
            SlaveEvent::TimeoutOccurred => {
                // Sends a pure signal () instead of the data message
                let _ = ch_ab_knock_tx.send_async(()).await; // No data
                println!(
                    "[Slave] Local house-keeping tick... (Knock signal sent, message {} saved)",
                    counter
                );
            }
        }
    }
}

// Equivalent to the master/coordinating process
async fn task_b_master(
    ch_ab_knock_rx: flume::Receiver<()>, // No data
    ch_ba_come_or_data_tx: flume::Sender<Message>,
    ch_ab_data_rx: flume::Receiver<Message>,
) {
    let counter = 0;
    let pending_message = Message::SensorData(counter);

    loop {
        let random_millis = {
            let mut rng = rand::rng();
            rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS)
        };

        let (tx_timer, rx_timer) = flume::bounded::<()>(0);
        tokio::spawn(async move {
            sleep(Duration::from_millis(random_millis)).await;
            let _ = tx_timer.send_async(()).await;
        });

        let event = flume::Selector::new()
            .recv(&ch_ab_knock_rx, |res| MasterEvent::KnockReceived(res))
            .recv(&rx_timer, |_| MasterEvent::TimeoutOccurred)
            .wait();

        match event {
            MasterEvent::KnockReceived(Ok(())) => {
                // Matches the pure signal ()
                println!("[Master] Received KNOCK signal from slave");

                let _ = ch_ba_come_or_data_tx
                    .send_async(pending_message.clone())
                    .await;

                match ch_ab_data_rx.recv_async().await {
                    Ok(Message::SensorData(data)) => {
                        println!("[Master] Handshake complete! Received data: {}", data);
                    }
                    Err(_) => {
                        println!("[Master] Protocol broken: Slave channel was closed!");
                        break;
                    }
                }
            }
            MasterEvent::KnockReceived(Err(_)) => {
                break;
            }
            MasterEvent::TimeoutOccurred => {
                println!("[Master] Watchdog warning: No knock received lately!");
            }
        }
    }
}

const CHAN_STREAMING_CAP_1: usize = 1; // Even if it contains no data
const CHAN_SYNCH_CAP_0: usize = 0;

#[tokio::main]
async fn main() {
    // The knock channel is now defined with the unit type ()
    let (ch_ab_knock_tx, ch_ab_knock_rx) = flume::bounded::<()>(CHAN_STREAMING_CAP_1);
    let (ch_ba_come_or_data_tx, ch_ba_come_or_data_rx) =
        flume::bounded::<Message>(CHAN_SYNCH_CAP_0);
    let (ch_ab_data_tx, ch_ab_data_rx) = flume::bounded::<Message>(CHAN_SYNCH_CAP_0);

    let task_a_slave_handle = tokio::spawn(task_a_slave(
        ch_ab_knock_tx,
        ch_ba_come_or_data_rx,
        ch_ab_data_tx,
    ));
    let task_b_master_handle = tokio::spawn(task_b_master(
        ch_ab_knock_rx,
        ch_ba_come_or_data_tx,
        ch_ab_data_rx,
    ));

    println!("System running. Tasks joined in a PAR-equivalent block.");

    let _ = tokio::join!(task_a_slave_handle, task_b_master_handle);
}
