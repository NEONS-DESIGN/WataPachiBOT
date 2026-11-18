//! Per-driver counters describing how outgoing voice frames were protected by DAVE.
//!
//! Local patch: the mixer sends frames without E2EE while the DAVE session is
//! not ready, and receiving clients silently discard them (issue #310).
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};

/// Shared counters written by the mixer.
#[derive(Debug, Default)]
pub(crate) struct DaveTxStats {
    protocol_version: AtomicU16,
    last_frame_encrypted: AtomicBool,
    encrypted_frames: AtomicU64,
    plaintext_frames: AtomicU64,
}

impl DaveTxStats {
    /// Records one frame handed to the UDP socket.
    pub(crate) fn record(&self, protocol_version: u16, encrypted: bool) {
        self.protocol_version
            .store(protocol_version, Ordering::Relaxed);
        self.last_frame_encrypted.store(encrypted, Ordering::Relaxed);
        let counter = if encrypted {
            &self.encrypted_frames
        } else {
            &self.plaintext_frames
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> DaveTxSnapshot {
        DaveTxSnapshot {
            protocol_version: self.protocol_version.load(Ordering::Relaxed),
            last_frame_encrypted: self.last_frame_encrypted.load(Ordering::Relaxed),
            encrypted_frames: self.encrypted_frames.load(Ordering::Relaxed),
            plaintext_frames: self.plaintext_frames.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time copy of the outgoing DAVE counters of one driver.
///
/// Counters accumulate for the lifetime of the [`Driver`](super::Driver).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DaveTxSnapshot {
    /// DAVE protocol version seen on the most recent frame (`0` means DAVE is inactive).
    pub protocol_version: u16,
    /// Whether the most recent frame was E2EE protected.
    pub last_frame_encrypted: bool,
    /// Frames sent with DAVE encryption applied.
    pub encrypted_frames: u64,
    /// Frames sent without DAVE encryption applied.
    pub plaintext_frames: u64,
}
