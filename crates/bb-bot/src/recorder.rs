//! File writer for market data and execution messages from the existing adapters.
//! No sockets, subscriptions, reconnect loop or independent run duration.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bb_core::recording::{RecordedMessage, RecordingSender};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const QUEUE_CAPACITY: usize = 4_096;
const BLOCK_BYTES: usize = 64 * 1024;
const ROTATION_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    output_dir: PathBuf,
    #[serde(default = "default_max_bytes")]
    max_bytes: u64,
}

const fn default_max_bytes() -> u64 {
    20_000_000_000
}

impl Config {
    pub fn validate(&self) -> io::Result<()> {
        if self.output_dir.as_os_str().is_empty() || self.max_bytes == 0 {
            return Err(invalid("output_dir must be nonempty; max_bytes must be positive"));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub struct Recorder {
    input: RecordingSender,
    writer: tokio::task::JoinHandle<io::Result<()>>,
}

impl Recorder {
    pub fn start(config: &Config, metadata: &serde_json::Value) -> io::Result<Self> {
        let session_id = format!(
            "{}-{}",
            SystemTime::now().duration_since(UNIX_EPOCH).map_err(io::Error::other)?.as_nanos(),
            std::process::id()
        );
        let destination = Destination::open(config, session_id.clone())?;
        let (input, rx) = RecordingSender::channel(QUEUE_CAPACITY);
        input.record("recorder", "session_start", || {
            serde_json::to_string(&serde_json::json!({"recording":config,"run":metadata}))
        });
        tracing::info!(%session_id, output_dir = %config.output_dir.display(), "Recording started");
        let final_record = input.final_record();
        let writer = tokio::task::spawn_blocking(move || {
            let result = destination.consume(rx, final_record);
            if let Err(error) = &result {
                tracing::error!(%session_id, %error, "Recording stopped; trading continues");
            }
            result
        });
        Ok(Self { input, writer })
    }

    pub fn sender(&self) -> RecordingSender {
        self.input.clone()
    }

    pub async fn finish(self) {
        self.input.finish().await;
        if let Err(error) = self.writer.await {
            tracing::error!(%error, "Recording writer task failed");
        }
    }
}

#[derive(Serialize)]
struct StoredRecord<'a> {
    session_id: &'a str,
    record_seq: u64,
    #[serde(flatten)]
    message: RecordedMessage,
}

struct Destination {
    directory: PathBuf,
    session_id: String,
    quota: u64,
    segment_bytes: u64,
    segment_index: u64,
    file: Option<File>,
}

impl Destination {
    fn open(config: &Config, session_id: String) -> io::Result<Self> {
        fs::create_dir_all(&config.output_dir)?;
        Ok(Self {
            directory: config.output_dir.clone(),
            session_id,
            quota: config.max_bytes,
            segment_bytes: 0,
            segment_index: 0,
            file: None,
        })
    }

    fn write_block(&mut self, jsonl: &[u8]) -> io::Result<()> {
        if jsonl.is_empty() {
            return Ok(());
        }
        let compressed = zstd::stream::encode_all(jsonl, 3)?;
        let size = u64::try_from(compressed.len()).map_err(io::Error::other)?;
        if self.segment_bytes + size > ROTATION_BYTES {
            self.close_segment()?;
        }
        if self.file.is_none() {
            // Approximate quota: check only before opening a segment. The
            // current segment may take the directory above the limit.
            let total_bytes = directory_bytes(&self.directory)?;
            if total_bytes >= self.quota {
                return Err(io::Error::other(format!(
                    "recording quota reached: {total_bytes} bytes used, limit {}; existing files preserved",
                    self.quota
                )));
            }
            let path = self
                .directory
                .join(format!("{}-{:06}.jsonl.zst", self.session_id, self.segment_index));
            // A collision must fail, never truncate an earlier recording.
            self.file = Some(OpenOptions::new().write(true).create_new(true).open(path)?);
            self.segment_index += 1;
        }
        if let Some(file) = &mut self.file {
            file.write_all(&compressed)?;
        }
        self.segment_bytes += size;
        Ok(())
    }

    fn close_segment(&mut self) -> io::Result<()> {
        if let Some(file) = self.file.take() {
            file.sync_all()?;
        }
        self.segment_bytes = 0;
        Ok(())
    }

    fn consume(
        mut self,
        mut rx: mpsc::Receiver<Option<RecordedMessage>>,
        final_record: impl Fn() -> RecordedMessage,
    ) -> io::Result<()> {
        // This future runs on the existing blocking writer thread. Only waiting
        // for input/timer uses Tokio; compression and disk I/O stay off its workers.
        tokio::runtime::Handle::current().block_on(async {
            let mut pending = Vec::new();
            let mut seq = 0;
            let mut flush = tokio::time::interval(Duration::from_secs(1));
            flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            flush.reset(); // The first deadline is one second from now, not an immediate tick.
            loop {
                let message = tokio::select! {
                    biased;
                    _ = flush.tick() => {
                        self.write_block(&pending)?;
                        pending.clear();
                        continue;
                    }
                    message = rx.recv() => message,
                };
                let finishing = !matches!(message, Some(Some(_)));
                let message = if let Some(Some(message)) = message {
                    message
                } else {
                    rx.close();
                    // No lossy enqueue for the final marker; include the final drop count.
                    final_record()
                };
                let record =
                    StoredRecord { session_id: &self.session_id, record_seq: seq, message };
                seq += 1;
                let mut line = serde_json::to_vec(&record)?;
                line.push(b'\n');
                if !pending.is_empty() && pending.len() + line.len() > BLOCK_BYTES {
                    self.write_block(&pending)?;
                    pending.clear();
                }
                pending.extend_from_slice(&line);
                if pending.len() >= BLOCK_BYTES {
                    self.write_block(&pending)?;
                    pending.clear();
                }
                if finishing {
                    break;
                }
            }
            self.write_block(&pending)?;
            self.close_segment()?;
            tracing::info!(session_id = %self.session_id, records = seq, "recording finished");
            Ok(())
        })
    }
}

fn directory_bytes(directory: &Path) -> io::Result<u64> {
    let mut total = 0_u64;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDirectory(PathBuf);
    impl TestDirectory {
        fn new() -> io::Result<Self> {
            static ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "bullet-recorder-test-{}-{}-{}",
                std::process::id(),
                SystemTime::now().duration_since(UNIX_EPOCH).map_err(io::Error::other)?.as_nanos(),
                ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            fs::create_dir(&path)?;
            Ok(Self(path))
        }
    }
    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn config(path: &Path) -> Config {
        Config { output_dir: path.into(), max_bytes: 100_000 }
    }

    #[test]
    fn quota_is_checked_at_the_next_file_boundary() -> Result<(), Box<dyn std::error::Error>> {
        let directory = TestDirectory::new()?;
        fs::write(directory.0.join("existing.bin"), [1_u8; 11])?;
        let mut settings = config(&directory.0);
        settings.max_bytes = 12;
        let mut writer = Destination::open(&settings, "session".into())?;
        writer.write_block(b"{\"a\":1}\n")?;
        writer.write_block(b"{\"b\":2}\n")?;
        assert!(directory_bytes(&directory.0)? > settings.max_bytes);
        writer.close_segment()?;
        assert!(writer.write_block(b"{\"c\":3}\n").is_err());
        assert!(!directory.0.join("session-000001.jsonl.zst").exists());
        assert_eq!(
            zstd::stream::decode_all(File::open(directory.0.join("session-000000.jsonl.zst"))?)?,
            b"{\"a\":1}\n{\"b\":2}\n"
        );
        assert_eq!(fs::read(directory.0.join("existing.bin"))?, [1; 11]);
        Ok(())
    }

    #[test]
    fn rotated_files_decode() -> Result<(), Box<dyn std::error::Error>> {
        let directory = TestDirectory::new()?;
        let mut writer = Destination::open(&config(&directory.0), "session".into())?;
        writer.write_block(b"{\"a\":1}\n")?;
        // Put the byte counter at the boundary without generating 64 MiB of test data.
        writer.segment_bytes = ROTATION_BYTES;
        writer.write_block(b"{\"b\":2}\n")?;
        writer.close_segment()?;
        for (index, expected) in [b"{\"a\":1}\n", b"{\"b\":2}\n"].iter().enumerate() {
            let file = File::open(directory.0.join(format!("session-{index:06}.jsonl.zst")))?;
            assert_eq!(zstd::stream::decode_all(file)?, *expected);
        }
        Ok(())
    }

    #[test]
    fn file_collision_does_not_overwrite() -> io::Result<()> {
        let directory = TestDirectory::new()?;
        let path = directory.0.join("session-000000.jsonl.zst");
        fs::write(&path, b"original")?;
        let mut writer = Destination::open(&config(&directory.0), "session".into())?;
        assert!(writer.write_block(b"{}\n").is_err());
        assert_eq!(fs::read(path)?, b"original");
        Ok(())
    }

    #[test]
    fn existing_directory_at_quota_is_not_modified() -> io::Result<()> {
        let directory = TestDirectory::new()?;
        let path = directory.0.join("previous.jsonl.zst");
        fs::write(&path, b"previous recording")?;
        let mut settings = config(&directory.0);
        settings.max_bytes = fs::metadata(&path)?.len();
        let mut writer = Destination::open(&settings, "new-session".into())?;
        assert!(writer.write_block(b"{}\n").is_err());
        assert_eq!(fs::read(path)?, b"previous recording");
        assert_eq!(directory_bytes(&directory.0)?, settings.max_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_flushes_while_exchange_still_holds_sender()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = TestDirectory::new()?;
        let recorder = Recorder::start(&config(&directory.0), &serde_json::json!({}))?;
        let exchange_sender = recorder.sender();
        let raw = r#"{ "e":"bookTicker", "E":1779600413218, "T":1779600413211, "s":"BTCUSDT", "b":"67001.0000", "B":"0.0100", "a":"67002", "A":"1" }"#;
        exchange_sender.record("binance", "bookTicker", || Ok(raw.into()));
        tokio::time::timeout(Duration::from_secs(5), recorder.finish()).await?;
        let path = fs::read_dir(&directory.0)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| path.extension().is_some_and(|ext| ext == "zst"))
            .ok_or_else(|| invalid("missing recording"))?;
        let jsonl = String::from_utf8(zstd::stream::decode_all(File::open(path)?)?)?;
        let records: Vec<serde_json::Value> =
            jsonl.lines().map(serde_json::from_str).collect::<Result<_, _>>()?;
        let record = records.iter().find(|r| r["source"] == "binance").unwrap();
        assert_eq!(record["payload"], raw);
        assert!(record["recv_unix_ns"].as_u64().is_some_and(|n| n > 0));
        assert!(record["recv_monotonic_ns"].as_u64().is_some());
        assert_eq!(records.last().unwrap()["kind"], "session_end");
        // A stopped writer does not break the still-running exchange adapter.
        exchange_sender.record("binance", "bookTicker", || panic!("writer is closed"));
        Ok(())
    }

    #[test]
    fn invalid_storage_settings_rejected() -> io::Result<()> {
        let directory = TestDirectory::new()?;
        let mut settings = config(&directory.0);
        assert!(settings.validate().is_ok());
        settings.max_bytes = 0;
        assert!(settings.validate().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn quiet_stream_flushes_without_waiting_for_another_message()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = TestDirectory::new()?;
        let metadata = serde_json::json!({"strategy":{"symbol":"ETH-USD","dry_run":false}});
        let recorder = Recorder::start(&config(&directory.0), &metadata)?;
        recorder.sender().record("test", "last_message", || Ok("{}".into()));
        let records = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                for file in fs::read_dir(&directory.0).unwrap() {
                    let bytes = fs::read(file.unwrap().path()).unwrap();
                    if let Ok(jsonl) = zstd::stream::decode_all(bytes.as_slice()) {
                        let records: Vec<serde_json::Value> = jsonl
                            .split(|b| *b == b'\n')
                            .filter(|line| !line.is_empty())
                            .map(serde_json::from_slice)
                            .collect::<Result<_, _>>()
                            .unwrap();
                        if records.last().is_some_and(|r| r["kind"] == "last_message") {
                            return records;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let start: serde_json::Value =
            serde_json::from_str(records[0]["payload"].as_str().unwrap())?;
        assert_eq!(start["run"], metadata);
        assert_eq!(start["recording"]["max_bytes"], 100_000);
        assert_eq!(records[0]["session_id"], records[1]["session_id"]);
        recorder.finish().await;
        Ok(())
    }

    #[tokio::test]
    async fn full_queue_cannot_drop_final_marker_or_final_loss_count()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = TestDirectory::new()?;
        let destination = Destination::open(&config(&directory.0), "full-queue".into())?;
        let (input, rx) = RecordingSender::channel(1);
        input.record("test", "kept", || Ok("{}".into()));
        input.record("test", "dropped", || panic!("full queue must not serialize"));
        let final_record = input.final_record();
        let writer = tokio::task::spawn_blocking(move || destination.consume(rx, final_record));
        input.finish().await;
        writer.await??;
        let bytes =
            zstd::stream::decode_all(File::open(directory.0.join("full-queue-000000.jsonl.zst"))?)?;
        let records: Vec<serde_json::Value> = bytes
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(serde_json::from_slice)
            .collect::<Result<_, _>>()?;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["kind"], "kept");
        assert_eq!(records[1]["kind"], "session_end");
        assert_eq!(records[1]["record_seq"], 1);
        assert_eq!(records[1]["dropped_total"], 1);
        Ok(())
    }

    #[tokio::test]
    async fn writer_exits_when_all_senders_are_dropped() -> Result<(), Box<dyn std::error::Error>> {
        let directory = TestDirectory::new()?;
        let destination = Destination::open(&config(&directory.0), "closed-input".into())?;
        let (input, rx) = RecordingSender::channel(1);
        input.record("test", "kept", || Ok("{}".into()));
        let final_record = input.final_record();
        drop(input);
        let writer = tokio::task::spawn_blocking(move || destination.consume(rx, final_record));
        tokio::time::timeout(Duration::from_secs(2), writer).await???;
        Ok(())
    }
}
