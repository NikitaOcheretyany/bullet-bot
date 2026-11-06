//! Optional copy of market data and execution messages for a file writer.
//! The bounded queue never waits on disk in an exchange's receive loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::mpsc;

#[derive(Debug, Serialize)]
pub struct RecordedMessage {
    pub recv_unix_ns: u64,
    pub recv_monotonic_ns: u64,
    pub source: &'static str,
    pub kind: &'static str,
    pub payload: String,
    /// Cumulative queue overflows in this recording session.
    pub dropped_total: u64,
}

impl RecordedMessage {
    fn new(
        source: &'static str,
        kind: &'static str,
        started: Instant,
        dropped_total: u64,
        received_at: Instant,
    ) -> Self {
        let now = Instant::now();
        let unix_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
        Self {
            recv_unix_ns: unix_ns
                .saturating_sub(now.saturating_duration_since(received_at).as_nanos())
                .try_into()
                .unwrap_or(u64::MAX),
            recv_monotonic_ns: received_at
                .saturating_duration_since(started)
                .as_nanos()
                .try_into()
                .unwrap_or(u64::MAX),
            source,
            kind,
            payload: String::new(),
            dropped_total,
        }
    }
}

/// `None` in the channel ends the recording after all preceding messages.
#[derive(Clone)]
pub struct RecordingSender {
    tx: mpsc::Sender<Option<RecordedMessage>>,
    started: Instant,
    dropped: Arc<AtomicU64>,
}

impl RecordingSender {
    pub fn channel(capacity: usize) -> (Self, mpsc::Receiver<Option<RecordedMessage>>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx, started: Instant::now(), dropped: Arc::default() }, rx)
    }

    /// Timestamp before serialization; copying a message must not stall trading.
    pub fn record(
        &self,
        source: &'static str,
        kind: &'static str,
        payload: impl FnOnce() -> Result<String, serde_json::Error>,
    ) {
        self.record_at(Instant::now(), source, kind, payload);
    }

    /// Keep the receive timestamp when selection/conversion precedes recording.
    /// Payload construction still happens only after a nonblocking reservation.
    pub fn record_at(
        &self,
        received_at: Instant,
        source: &'static str,
        kind: &'static str,
        payload: impl FnOnce() -> Result<String, serde_json::Error>,
    ) {
        let permit = match self.tx.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(())) => {
                let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if dropped.is_power_of_two() {
                    tracing::warn!(
                        dropped,
                        "Recording queue full; records lost, trading continues"
                    );
                }
                return;
            }
            Err(mpsc::error::TrySendError::Closed(())) => return,
        };
        let mut message = RecordedMessage::new(
            source,
            kind,
            self.started,
            self.dropped.load(Ordering::Relaxed),
            received_at,
        );
        match payload() {
            Ok(payload) => {
                message.payload = payload;
                permit.send(Some(message));
            }
            Err(error) => tracing::warn!(%error, source, "Cannot serialize record"),
        }
    }

    /// Build the final marker in the writer using the shared clock/loss count.
    /// Capture no sender: the writer must not keep its own input channel alive.
    pub fn final_record(&self) -> impl Fn() -> RecordedMessage + Send + 'static {
        let started = self.started;
        let dropped = self.dropped.clone();
        move || {
            RecordedMessage::new(
                "recorder",
                "session_end",
                started,
                dropped.load(Ordering::Relaxed),
                Instant::now(),
            )
        }
    }

    pub async fn finish(&self) {
        let _ = self.tx.send(None).await;
        tracing::info!(dropped = self.dropped.load(Ordering::Relaxed), "Recording input closed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn delayed_recording_preserves_receive_time() {
        let (sender, mut rx) = RecordingSender::channel(2);
        let received = Instant::now();
        sender.record_at(received, "bullet", "depthUpdate", || Ok("first".into()));
        let first = rx.try_recv().unwrap().unwrap();
        // Equal receive Instants retain equal monotonic timestamps regardless
        // of conversion/merge work done before the second reservation.
        sender.record_at(received, "bullet", "depthUpdate", || Ok("second".into()));
        let second = rx.try_recv().unwrap().unwrap();
        assert_eq!(first.recv_monotonic_ns, second.recv_monotonic_ns);
        assert_eq!(
            first.recv_monotonic_ns,
            u64::try_from(received.duration_since(sender.started).as_nanos()).unwrap()
        );
    }

    #[tokio::test]
    async fn full_queue_does_not_block_and_reports_loss() {
        let (sender, mut rx) = RecordingSender::channel(1);
        sender.record("binance", "bookTicker", || Ok("first".into()));
        sender.record("binance", "bookTicker", || panic!("full queue must not serialize"));
        assert_eq!(rx.recv().await.unwrap().unwrap().payload, "first");
        sender.record("binance", "bookTicker", || Ok("third".into()));
        let record = rx.recv().await.unwrap().unwrap();
        assert_eq!(record.payload, "third");
        assert_eq!(record.dropped_total, 1);
        sender.finish().await;
        assert!(rx.recv().await.unwrap().is_none());
    }
}
