// =============================================================================================
// VERSIONS / COMMITS
// =============================================================================================
// 15Jun2026 0.0.010 Fisrt version, runs but no knock-come

use std::time::Duration;
use tokio::time::sleep;
use rand::Rng;

const RANDOM_VAL_MIN_MS: u64 =   0; 
const RANDOM_VAL_MAX_MS: u64 = 100; 

// Since tasks run forever, we only need to pass actual data
#[derive(Clone)] // Since the compiler does not know the full functionality of flume::Selector
enum Message {
    SensorData(i32),
}

// Internal enums to map which branch won the selection
enum SlaveEvent {
    HandshakeCompleted,
    TimeoutOccurred,
}

enum MasterEvent {
    DataReceived(Result<Message, flume::RecvError>),
    TimeoutOccurred,
}


// Equivalent to an occam process running on a hardware tile
async fn task_a_slave(sender: flume::Sender<Message>) {
    let mut counter = 0;
    // We instantiate the first message BEFORE the loop starts
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

        // We try to send the 'pending_message' that we have standing ready
        let event = flume::Selector::new()
            .send(&sender, pending_message.clone(), |_| SlaveEvent::HandshakeCompleted)
            .recv(&rx_timer, |_| SlaveEvent::TimeoutOccurred)
            .wait();

        match event {
            SlaveEvent::HandshakeCompleted => {
                // Since the message was delivered, we prepare a NEW message for the next round
                counter += 1;
                pending_message = Message::SensorData(counter);
            }
            SlaveEvent::TimeoutOccurred => {
                // The timer won! We do nothing with 'pending_message'.
                // It remains exactly as it is, and is carried into the next iteration of the loop.
                println!("[Slave] Local house-keeping tick... (Message {} is saved for retry)", counter);
            }
        }

        sleep(Duration::from_secs(1)).await;
    }
}


// Equivalent to the master/coordinating process
async fn task_b_master(receiver: flume::Receiver<Message>) {
    loop {
        let random_millis = {
            let mut rng = rand::rng();
            rng.random_range(RANDOM_VAL_MIN_MS..=RANDOM_VAL_MAX_MS) 
        };

        // 1. Encapsulate the master watchdog timer as a CSP event
        let (tx_timer, rx_timer) = flume::bounded::<()>(0);
        tokio::spawn(async move {
            sleep(Duration::from_millis(random_millis)).await;
            let _ = tx_timer.send_async(()).await;
        });

        // 2. CSP Selector for the master
        let event = flume::Selector::new()
            .recv(&receiver, |res| MasterEvent::DataReceived(res))
            .recv(&rx_timer, |_| MasterEvent::TimeoutOccurred)
            .wait();

        // 3. Handle the won branch deterministically
        match event {
            MasterEvent::DataReceived(Ok(Message::SensorData(data))) => {
                println!("[Master] Received sensor reading: {}", data);
            }
            MasterEvent::DataReceived(Err(_)) => {
                // Channel was closed (should not happen since tasks run forever)
                break;
            }
            MasterEvent::TimeoutOccurred => {
                println!("[Master] Watchdog warning: No data received lately!");
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // A true 0-capacity rendezvous channel
    let (sender, receiver) = flume::bounded::<Message>(0);

    // 1. Start the tasks and capture their JoinHandles
    let task_a_slave_handle = tokio::spawn(task_a_slave(sender));
    let task_b_master_handle = tokio::spawn(task_b_master(receiver));

    println!("System running. Tasks joined in a PAR-equivalent block.");

    // 2. This is the exact equivalent to a PAR block in occam.
    let _ = tokio::join!(task_a_slave_handle, task_b_master_handle);
}