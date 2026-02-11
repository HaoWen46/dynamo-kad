//! Write-Ahead Log (WAL) for durability.
//!
//! Format: each entry is `[4-byte CRC32][4-byte length][JSON payload]\n`.
//! On recovery, replay all valid entries. Corrupt tail entries are truncated.

use crate::record::StorageRecord;
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("WAL I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("WAL serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("WAL CRC mismatch: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch { expected: u32, actual: u32 },
    #[error("WAL corrupt entry at line {line}")]
    Corrupt { line: usize },
}

/// Fsync policy for the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Fsync after every write.
    Always,
    /// Fsync periodically (caller controls).
    Batch,
    /// Never explicitly fsync (OS decides).
    None,
}

impl FsyncPolicy {
    pub fn from_str_config(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "always" => Self::Always,
            "none" => Self::None,
            _ => Self::Batch,
        }
    }
}

/// An append-only write-ahead log.
#[derive(Debug)]
pub struct Wal {
    writer: BufWriter<File>,
    #[allow(dead_code)] // will be used for WAL rotation
    path: PathBuf,
    fsync: FsyncPolicy,
    entries_written: u64,
}

impl Wal {
    /// Open or create a WAL file at the given path.
    pub fn open(path: &Path, fsync: FsyncPolicy) -> Result<Self, WalError> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            path: path.to_path_buf(),
            fsync,
            entries_written: 0,
        })
    }

    /// Append a record to the WAL.
    pub fn append(&mut self, record: &StorageRecord) -> Result<(), WalError> {
        let payload = serde_json::to_vec(record)?;

        // Compute CRC32 of the JSON payload
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();

        let len = payload.len() as u32;

        // Write: CRC(4) + LEN(4) + PAYLOAD + \n
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;

        if self.fsync == FsyncPolicy::Always {
            self.writer.get_ref().sync_all()?;
        }

        self.entries_written += 1;
        Ok(())
    }

    /// Explicitly fsync the WAL (for batch mode).
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Number of entries written since open.
    pub fn entries_written(&self) -> u64 {
        self.entries_written
    }

    /// Replay all valid entries from a WAL file.
    /// Returns the records in order. Stops at the first corrupt entry.
    pub fn replay(path: &Path) -> Result<Vec<StorageRecord>, WalError> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(WalError::Io(e)),
        };

        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        let mut line_num = 0u64;

        loop {
            // Read CRC (4 bytes)
            let mut crc_buf = [0u8; 4];
            match io::Read::read_exact(&mut reader, &mut crc_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // clean EOF
                Err(e) => return Err(WalError::Io(e)),
            }
            let expected_crc = u32::from_le_bytes(crc_buf);

            // Helper macro: treat UnexpectedEof as truncated entry (stop replay).
            macro_rules! read_or_break {
                ($reader:expr, $buf:expr) => {
                    match io::Read::read_exact($reader, $buf) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                            tracing::warn!(
                                "WAL truncated mid-entry at entry {}; stopping replay",
                                line_num
                            );
                            break;
                        }
                        Err(e) => return Err(WalError::Io(e)),
                    }
                };
            }

            // Read length (4 bytes)
            let mut len_buf = [0u8; 4];
            read_or_break!(&mut reader, &mut len_buf);
            let len = u32::from_le_bytes(len_buf) as usize;

            // Read payload
            let mut payload = vec![0u8; len];
            read_or_break!(&mut reader, &mut payload);

            // Read trailing newline
            let mut nl = [0u8; 1];
            read_or_break!(&mut reader, &mut nl);

            // Verify CRC
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let actual_crc = hasher.finalize();

            if actual_crc != expected_crc {
                tracing::warn!(
                    "WAL CRC mismatch at entry {}: expected {:#010x}, got {:#010x}; stopping replay",
                    line_num, expected_crc, actual_crc
                );
                break; // Truncate at corruption
            }

            let record: StorageRecord = serde_json::from_slice(&payload)?;
            records.push(record);
            line_num += 1;
        }

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn test_record(key: &str, value: &str) -> StorageRecord {
        StorageRecord::new(key.to_string(), value.as_bytes().to_vec(), HashMap::new())
    }

    #[test]
    fn test_wal_write_and_replay() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write
        {
            let mut wal = Wal::open(&wal_path, FsyncPolicy::Always).unwrap();
            wal.append(&test_record("k1", "v1")).unwrap();
            wal.append(&test_record("k2", "v2")).unwrap();
            wal.append(&test_record("k3", "v3")).unwrap();
            assert_eq!(wal.entries_written(), 3);
        }

        // Replay
        let records = Wal::replay(&wal_path).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key, "k1");
        assert_eq!(records[1].key, "k2");
        assert_eq!(records[2].key, "k3");
        assert_eq!(records[0].value, b"v1");
    }

    #[test]
    fn test_wal_replay_empty() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("empty.wal");
        let records = Wal::replay(&wal_path).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_wal_replay_truncated() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("trunc.wal");

        // Write 3 entries
        {
            let mut wal = Wal::open(&wal_path, FsyncPolicy::Always).unwrap();
            wal.append(&test_record("k1", "v1")).unwrap();
            wal.append(&test_record("k2", "v2")).unwrap();
            wal.append(&test_record("k3", "v3")).unwrap();
        }

        // Corrupt the file by truncating the last few bytes
        {
            let file = OpenOptions::new().write(true).open(&wal_path).unwrap();
            let len = file.metadata().unwrap().len();
            file.set_len(len - 5).unwrap(); // chop off end
        }

        // Replay should return the first 2 valid entries
        let records = Wal::replay(&wal_path).unwrap();
        assert_eq!(
            records.len(),
            2,
            "should recover 2 of 3 entries after truncation"
        );
        assert_eq!(records[0].key, "k1");
        assert_eq!(records[1].key, "k2");
    }

    #[test]
    fn test_wal_tombstone_record() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("tomb.wal");

        let tombstone = StorageRecord::tombstone("k1".to_string(), HashMap::new());

        {
            let mut wal = Wal::open(&wal_path, FsyncPolicy::Always).unwrap();
            wal.append(&tombstone).unwrap();
        }

        let records = Wal::replay(&wal_path).unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].tombstone);
        assert!(records[0].value.is_empty());
    }

    #[test]
    fn test_wal_with_vclock() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("vclock.wal");

        let mut vclock = HashMap::new();
        vclock.insert("node-a".to_string(), 3);
        vclock.insert("node-b".to_string(), 1);

        let record = StorageRecord::new("k1".to_string(), b"value".to_vec(), vclock.clone());

        {
            let mut wal = Wal::open(&wal_path, FsyncPolicy::Always).unwrap();
            wal.append(&record).unwrap();
        }

        let records = Wal::replay(&wal_path).unwrap();
        assert_eq!(records[0].vclock, vclock);
    }
}
