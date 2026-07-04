// =================================================================================
// Function from a project as worked into life with Claude and PJV, based on
// 0.0.900 or older of the test_knock_come project, as seen in
// https://github.com/Aclassifier/rust_test_knock_come
// This runs without the try_send, but with a NESTED select instead,
// which avoids the deadlock caused by send_asynch in task_b_master.
// THIS FILE NOW SERVES AS A COPY-FROM FILE ONLY
// =================================================================================

// knock_come_redraw.rs — REDRAW variant (current behaviour), fully reconciled.
//
// Same algorithm as knock_come.rs / knock_come_instrumented.rs: a fresh random
// timeout is drawn on EVERY select! pass and a brand-new tokio::time::sleep is
// built from it. This file only adds time accounting that *reconciles*:
//
//     runtime  ==  sleeping  +  preempted_sleep  +  committed     (per task)
//
//   sleeping        : select-wait on passes where the TIMER fired (completed sleep)
//   preempted_sleep : select-wait on passes where a NON-timer arm won — i.e. the
//                     already-elapsed part of a sleep that is then DISCARDED because
//                     the next pass draws a brand-new timer from zero.
//   committed       : all post-select work in the branch body (rendezvous sends,
//                     the data3 recv, and the slave's inner await-come loop).
//
// HANDOVER.md step 2: this fixes the under-count in knock_come_instrumented.rs
// (whose `sleeping` ignored preempted partial sleeps) so the ~19.5 s/side of
// discarded sleep is measured directly instead of inferred from a budget gap.
//
// Cargo.toml: tokio = { version="1", features=["rt-multi-thread","macros","time"] }
//             flume = "0.11"
// Run:        cargo run --release

use flume::{Receiver, Sender, bounded};
use std::time::{Duration, Instant};

#[inline]
fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[inline]
fn next_draw_ms(state: &mut u64) -> u64 {
    xorshift64(state) % 100
}

const TARGET_MASTER_EVENTS: u64 = 1000;
const INTENDED_MEAN_MS: f64 = 49.5;

#[derive(Default)]
struct Stats {
    draws_n: u64,
    draws_mean: f64,
    draws_m2: f64,
    fired_n: u64,
    fired_mean: f64,
    sends: u64,
    recvs: u64,
    sleeping: Duration,
    preempted_sleep: Duration,
    committed: Duration,
    runtime: Duration,
    // Teig added:
    knocks: u64,
    comes: u64,
    datas: u64,
    spontaneous_datas: u64,
    spontaneous_datas_2: u64,
}

impl Stats {
    #[inline]
    fn observe_draw(&mut self, x: f64) {
        self.draws_n += 1;
        let d = x - self.draws_mean;
        self.draws_mean += d / self.draws_n as f64;
        self.draws_m2 += d * (x - self.draws_mean);
    }
    #[inline]
    fn observe_fired(&mut self, x: f64) {
        self.fired_n += 1;
        self.fired_mean += (x - self.fired_mean) / self.fired_n as f64;
    }
    fn draws_sd(&self) -> f64 {
        if self.draws_n > 1 {
            (self.draws_m2 / (self.draws_n as f64 - 1.0)).sqrt()
        } else {
            0.0
        }
    }
    fn report(&self, who: &str) {
        let preempted = self.draws_n.saturating_sub(self.fired_n);
        let pct = if self.draws_n > 0 {
            100.0 * preempted as f64 / self.draws_n as f64
        } else {
            0.0
        };
        let accounted = self.sleeping + self.preempted_sleep + self.committed;
        let unaccounted = self.runtime.saturating_sub(accounted);
        let timer_time = (self.sleeping + self.preempted_sleep).as_secs_f64();
        let per_fire = if self.fired_n > 0 {
            timer_time * 1e3 / self.fired_n as f64
        } else {
            0.0
        };
        println!("[{who}]");
        println!(
            "  draws            n={} mean={:.2} ms sd={:.2} ms (intended 49.50)",
            self.draws_n,
            self.draws_mean,
            self.draws_sd()
        );
        println!(
            "  timer fired      {}/{}  ({:.1}% preempted)  mean-of-fired={:.2} ms",
            self.fired_n, self.draws_n, pct, self.fired_mean
        );
        println!("  sends {}  recvs {}", self.sends, self.recvs);
        println!(
            "  knocks {} comes {} datas {} spontaneous_datas {} + spontaneous_datas_2 {} = {}",
            self.knocks,
            self.comes,
            self.datas,
            self.spontaneous_datas,
            self.spontaneous_datas_2,
            self.spontaneous_datas + self.spontaneous_datas_2
        ); // Teig
        println!(
            "  TIME  runtime {:.2}s = sleeping {:.2}s + preempted_sleep {:.2}s + committed {:.2}s  (unaccounted {:.3}s)",
            self.runtime.as_secs_f64(),
            self.sleeping.as_secs_f64(),
            self.preempted_sleep.as_secs_f64(),
            self.committed.as_secs_f64(),
            unaccounted.as_secs_f64(),
        );
        println!(
            "  timer-time/fire  {:.2} ms   (ideal ~49.50; >49.50 ==> elapsed sleep discarded on redraw)",
            per_fire
        );
    }
}

async fn master(
    knock_rx: Receiver<()>,
    come_tx: Sender<()>,
    spont_tx: Sender<u64>,
    data3_rx: Receiver<u64>,
    seed: u64,
) -> (u64, Stats) {
    let mut rng = seed;
    let mut st = Stats::default();
    let mut count: u64 = 0;
    let t_start = Instant::now();

    while count < TARGET_MASTER_EVENTS {
        let ms = next_draw_ms(&mut rng); // fresh draw EVERY pass
        st.observe_draw(ms as f64);
        let pause = Duration::from_millis(ms);
        let t_iter = Instant::now();

        tokio::select! {
            biased;

            knock = knock_rx.recv_async() => {
                st.preempted_sleep += t_iter.elapsed(); // partial sleep, about to be discarded
                if knock.is_err() { break; }
                let t_body = Instant::now();
                st.knocks += 1;
                if come_tx.send_async(()).await.is_err() { break; }   // 2.1
                st.sends += 1;
                st.comes += 1;
                match data3_rx.recv_async().await {                   // 3
                    Ok(_v) => { st.recvs += 1; count += 1; st.datas += 1; }
                    Err(_) => break,
                }
                st.committed += t_body.elapsed();
            }

            _ = tokio::time::sleep(pause) => {
                st.sleeping += t_iter.elapsed();   // sleep ran to completion
                st.observe_fired(ms as f64);
                let t_body = Instant::now();
                if spont_tx.send_async(count).await.is_err() { break; } // 2.2
                st.spontaneous_datas += 1;
                st.sends += 1;
                count += 1;
                st.committed += t_body.elapsed();
            }
        }
    }
    st.runtime = t_start.elapsed();
    (count, st)
}

async fn slave(
    knock_tx: Sender<()>,
    come_rx: Receiver<()>,
    spont_rx: Receiver<u64>,
    data3_tx: Sender<u64>,
    seed: u64,
) -> (u64, Stats) {
    let mut rng = seed;
    let mut st = Stats::default();
    let mut count: u64 = 0;
    let t_start = Instant::now();

    loop {
        let ms = next_draw_ms(&mut rng); // fresh draw EVERY pass
        st.observe_draw(ms as f64);
        let pause = Duration::from_millis(ms);
        let t_iter = Instant::now();

        tokio::select! {
            biased;

            data = spont_rx.recv_async() => {
                st.preempted_sleep += t_iter.elapsed(); // partial sleep, about to be discarded
                match data { Ok(_v) => count += 1, Err(_) => break }
                st.spontaneous_datas += 1;
            }

            _ = tokio::time::sleep(pause) => {
                st.sleeping += t_iter.elapsed();
                st.observe_fired(ms as f64);
                let t_body = Instant::now();

                if knock_tx.send_async(()).await.is_err() { break; }
                st.knocks += 1;
                st.sends += 1;

                'await_come: loop {
                    tokio::select! {
                        biased;
                        come = come_rx.recv_async() => {
                            if come.is_err() { break 'await_come; }
                            st.comes += 1;
                            if data3_tx.send_async(count).await.is_err() { break 'await_come; }
                            st.datas += 1;
                            st.sends += 1;
                            count += 1;
                            break 'await_come;
                        }
                        data = spont_rx.recv_async() => {
                            match data { Ok(_v) => count += 1, Err(_) => break 'await_come }
                            st.spontaneous_datas_2 += 1;
                        }
                    }
                }
                st.committed += t_body.elapsed();
            }
        }
    }
    st.runtime = t_start.elapsed();
    (count, st)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    println!("knock_come_redraw.rs (Teig mod 29May2026 14.05)");
    let (knock_tx, knock_rx) = bounded::<()>(1);
    let (come_tx, come_rx) = bounded::<()>(0);
    let (spont_tx, spont_rx) = bounded::<u64>(0);
    let (data3_tx, data3_rx) = bounded::<u64>(0);

    let master_seed: u64 = 0x9E3779B97F4A7C15;
    let slave_seed: u64 = 0xD1B54A32D192ED03;

    let t0 = Instant::now();
    let m = tokio::spawn(master(knock_rx, come_tx, spont_tx, data3_rx, master_seed));
    let s = tokio::spawn(slave(knock_tx, come_rx, spont_rx, data3_tx, slave_seed));
    let (m_events, m_stats) = m.await.expect("master panicked");
    let (s_events, s_stats) = s.await.expect("slave panicked");
    let dt = t0.elapsed().as_secs_f64();

    let base = m_events as f64 * INTENDED_MEAN_MS * 1e-3; // N * 49.5 ms
    let k = base / dt;

    println!("========== run summary :: REDRAW (timer redrawn every select pass) ==========");
    println!("master_events {m_events}  slave_events {s_events}  DT {dt:.2}s");
    println!(
        "ideal k=2 {:.2}s | serial k=1 {:.2}s | observed k = {k:.3}",
        base / 2.0,
        base
    );
    println!("-----------------------------------------------------------------------------");
    m_stats.report("master");
    s_stats.report("slave");
    println!("=============================================================================");
}

// LOG 29Jun2026. Timeout not solved.
/*
knock_come_redraw.rs
========== run summary :: REDRAW (timer redrawn every select pass) ==========
master_events 1000  slave_events 1000  DT 34.81s
ideal k=2 24.75s | serial k=1 49.50s | observed k = 1.422
-----------------------------------------------------------------------------
[master]
  draws            n=1000 mean=50.17 ms sd=28.67 ms (intended 49.50)
  timer fired      509/1000  (49.1% preempted)  mean-of-fired=35.18 ms
  sends 1000  recvs 491
  TIME  runtime 34.81s = sleeping 19.69s + preempted_sleep 15.11s + committed 0.02s  (unaccounted 0.001s)
  timer-time/fire  68.36 ms   (ideal ~49.50; >49.50 ==> elapsed sleep discarded on redraw)
[slave]
  draws            n=950 mean=49.56 ms sd=29.11 ms (intended 49.50)
  timer fired      491/950  (48.3% preempted)  mean-of-fired=34.48 ms
  sends 982  recvs 0
  TIME  runtime 34.81s = sleeping 18.74s + preempted_sleep 16.05s + committed 0.02s  (unaccounted 0.001s)
  timer-time/fire  70.87 ms   (ideal ~49.50; >49.50 ==> elapsed sleep discarded on redraw)
=============================================================================
*/
/*
knock_come_redraw.rs (Teig mod 29May2026 14.05)
========== run summary :: REDRAW (timer redrawn every select pass) ==========
master_events 1000  slave_events 1000  DT 34.56s
ideal k=2 24.75s | serial k=1 49.50s | observed k = 1.432
-----------------------------------------------------------------------------
[master]
  draws            n=1000 mean=50.17 ms sd=28.67 ms (intended 49.50)
  timer fired      502/1000  (49.8% preempted)  mean-of-fired=33.44 ms
  sends 1000  recvs 498
  knocks 498 comes 498 datas 498 spontaneous_datas 502 + spontaneous_datas_2 0 = 502
  TIME  runtime 34.55s = sleeping 18.47s + preempted_sleep 16.07s + committed 0.01s  (unaccounted 0.001s)
  timer-time/fire  68.80 ms   (ideal ~49.50; >49.50 ==> elapsed sleep discarded on redraw)
[slave]
  draws            n=965 mean=49.41 ms sd=29.05 ms (intended 49.50)
  timer fired      499/965  (48.3% preempted)  mean-of-fired=33.55 ms
  sends 997  recvs 0
  knocks 499 comes 498 datas 498 spontaneous_datas 465 + spontaneous_datas_2 37 = 502
  TIME  runtime 34.55s = sleeping 18.45s + preempted_sleep 16.09s + committed 0.02s  (unaccounted 0.001s)
  timer-time/fire  69.22 ms   (ideal ~49.50; >49.50 ==> elapsed sleep discarded on redraw)
=============================================================================
*/
