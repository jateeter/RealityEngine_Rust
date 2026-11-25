//! A participant's clock (RealityEngine_CI#296, #600): `{instance, lamport,
//! commit, step}`.
//!
//! A UUID belongs to a *participant* -- an instance's RE (rust-1) or its PE --
//! never to an engine type or an image, and no two participants may share one.
//! CI allocates it (`INSTANCE_UUID` for an RE, `PE_INSTANCE_UUID` for a PE); a
//! participant launched without one mints a version-7 UUID at boot.
//!
//! - `lamport` is the full Lamport clock: it ticks on every committed step,
//!   every send and every receive, and a receive is `max(local, received) + 1`.
//! - `commit` counts committed steps: what `lamport` counted before #600.
//!
//! Neither ever resets, and `lamport >= commit` always holds (`step` is the
//! caller's: it restarts on reset). For an allocated participant both hold
//! across restarts: `INSTANCE_CLOCK_DIR` (default `~/.reality-engine/clock/`)
//! keeps two high-water marks, `<uuid>.lamport` and `<uuid>.commit`, each
//! reserved [`LAMPORT_RESERVATION`] ticks ahead. A boot resumes from the marks
//! and reserves the next blocks before issuing any tick; a tick past a persisted
//! mark -- a receive that jumps past it included -- is always preceded by the
//! write of the next mark, never followed by it. Writes are temp + rename, so a
//! crash leaves the old mark or the new one. A directory written before #600
//! holds only `<uuid>.lamport`, which counted committed steps; the first boot
//! under this rule seeds both marks from it.
//!
//! The participant holds an exclusive `fcntl` lock on `<uuid>.lock` for its
//! whole life, so a second live process -- of any engine type -- presenting the
//! same UUID refuses to boot. A POSIX record lock, as C++ and LSP (`lockf`) and
//! Scala (`FileChannel.tryLock`) take, because that is what
//! `instance_uuids.py live` asks about with `F_GETLK`; `File::try_lock` is
//! `flock`, which neither sees. A separate file, because the marks are replaced
//! by rename and a lock on a replaced inode guards nothing.
//!
//! Thread-safe: receives and sends arrive on every request task.
//!
//! The semantics, error text and file names follow the three voters
//! (C++ `instance_clock.cpp`); SURFACE_SPEC.md, "The instance clock", governs.

use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    os::fd::AsRawFd,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

pub const LAMPORT_RESERVATION: u64 = 1024;
/// The largest lamport an `RE-Clock` header may carry: 2^53 - 1, the largest
/// integer every runtime's JSON number represents exactly.
pub const MAX_LAMPORT: u64 = 9_007_199_254_740_991;

/// An inbound request's `RE-Clock` header, as counted on the clock routes.
#[derive(Debug, Clone, Copy)]
pub enum HeaderKind {
    Received,
    Missing,
    Rejected,
}

#[derive(Debug, Default)]
struct Counters {
    lamport: u64,
    commit: u64,
    lamport_reserved: u64,
    commit_reserved: u64,
    received: u64,
    missing: u64,
    rejected: u64,
}

#[derive(Debug)]
pub struct InstanceClock {
    uuid: String,
    /// `<uuid>.lamport` and `<uuid>.commit`; `None` for a minted, unpersisted clock.
    lamport_file: Option<PathBuf>,
    commit_file: Option<PathBuf>,
    counters: Mutex<Counters>,
    /// Holds the participant lock for as long as the clock lives; dropping it
    /// closes the descriptor, which releases the lock.
    _lock: Option<File>,
}

impl Default for InstanceClock {
    fn default() -> Self {
        Self::minted()
    }
}

impl InstanceClock {
    /// An RE's clock for this boot, from `INSTANCE_UUID` / `INSTANCE_CLOCK_DIR`.
    /// Fails when an allocated participant cannot keep its clock or another live
    /// process holds its UUID: either way ticks could be issued twice, which is
    /// the one thing the clock must not do.
    pub fn boot() -> Result<Self, String> {
        Self::boot_var("INSTANCE_UUID")
    }

    /// The clock allocated under `variable` (`PE_INSTANCE_UUID` for a PE).
    pub fn boot_var(variable: &str) -> Result<Self, String> {
        Self::boot_from_named(
            std::env::var(variable).ok().as_deref(),
            &clock_dir(),
            variable,
        )
    }

    /// [`boot`](Self::boot) with its inputs given (tests).
    pub fn boot_from(allocated: Option<&str>, dir: &Path) -> Result<Self, String> {
        Self::boot_from_named(allocated, dir, "INSTANCE_UUID")
    }

    fn boot_from_named(
        allocated: Option<&str>,
        dir: &Path,
        variable: &str,
    ) -> Result<Self, String> {
        let allocated = allocated.unwrap_or("");
        if !canonical_uuid(allocated) {
            if !allocated.is_empty() {
                eprintln!("{variable} \"{allocated}\" is not a canonical UUID; minting one");
            }
            return Ok(Self::minted());
        }
        let uuid = allocated.to_ascii_lowercase();
        let file = dir.join(format!("{uuid}.lamport"));
        Self::persisted(&uuid, &file)
    }

    /// A minted, unpersisted clock -- what a participant without an allocation runs.
    pub fn minted() -> Self {
        Self {
            uuid: mint_v7(),
            lamport_file: None,
            commit_file: None,
            counters: Mutex::new(Counters::default()),
            _lock: None,
        }
    }

    /// An allocated clock whose lamport mark is `file` (`<dir>/<uuid>.lamport`)
    /// and commit mark the same path with extension `.commit`.
    pub fn persisted(uuid: &str, file: &Path) -> Result<Self, String> {
        let lock = lock_instance(file, uuid)?; // before anything is read or written
        let commit_file = file.with_extension("commit");
        let lamport_mark = read_reservation(file)?;
        // Before #600 the one mark counted committed steps: it seeds both.
        let commit_mark = if commit_file.exists() {
            read_reservation(&commit_file)?
        } else {
            lamport_mark
        };
        let lamport = lamport_mark.max(commit_mark);
        let clock = Self {
            uuid: uuid.to_owned(),
            lamport_file: Some(file.to_owned()),
            commit_file: Some(commit_file),
            counters: Mutex::new(Counters {
                lamport,
                commit: commit_mark,
                ..Counters::default()
            }),
            _lock: Some(lock),
        };
        {
            let mut c = clock.lock();
            clock.reserve_commit(&mut c, commit_mark)?;
            clock.reserve_lamport(&mut c, lamport)?;
        }
        Ok(clock)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Counters> {
        self.counters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// A committed step: ticks commit and lamport together; returns both.
    pub fn commit_step(&self) -> Result<(u64, u64), String> {
        let mut c = self.lock();
        let next_commit = c.commit + 1;
        let next_lamport = c.lamport + 1;
        // Persist first, then issue: a mark is never behind what was handed out.
        if self.commit_file.is_some() && next_commit > c.commit_reserved {
            self.reserve_commit(&mut c, next_commit)?;
        }
        if self.lamport_file.is_some() && next_lamport > c.lamport_reserved {
            self.reserve_lamport(&mut c, next_lamport)?;
        }
        c.commit = next_commit;
        c.lamport = next_lamport;
        Ok((next_lamport, next_commit))
    }

    /// A send -- a response or an outbound call: ticks lamport; returns it.
    pub fn send(&self) -> Result<u64, String> {
        let mut c = self.lock();
        let next = c.lamport + 1;
        self.advance(&mut c, next)
    }

    /// A receive, applied before the request is handled: `max(local, received)
    /// + 1`, or `local + 1` when no header was presented; returns the new lamport.
    pub fn receive(&self, received: Option<u64>) -> Result<u64, String> {
        let mut c = self.lock();
        let next = c.lamport.max(received.unwrap_or(0)) + 1;
        self.advance(&mut c, next)
    }

    /// A tick of a stored or forwarded message: a send, as `{instance, lamport}`.
    pub fn event(&self) -> Result<(String, u64), String> {
        Ok((self.uuid.clone(), self.send()?))
    }

    pub fn lamport(&self) -> u64 {
        self.lock().lamport
    }

    pub fn commit(&self) -> u64 {
        self.lock().commit
    }

    pub fn instance(&self) -> &str {
        &self.uuid
    }

    /// The `RE-Clock` header value for `lamport`.
    pub fn header(&self, lamport: u64) -> String {
        format!("instance={}; lamport={lamport}", self.uuid)
    }

    pub fn count_header(&self, kind: HeaderKind) {
        let mut c = self.lock();
        match kind {
            HeaderKind::Received => c.received += 1,
            HeaderKind::Missing => c.missing += 1,
            HeaderKind::Rejected => c.rejected += 1,
        }
    }

    /// `(received, missing, rejected)` since boot.
    pub fn header_counts(&self) -> (u64, u64, u64) {
        let c = self.lock();
        (c.received, c.missing, c.rejected)
    }

    // A jump past the reserved mark is made durable before it is used: one jump
    // can exceed the reservation, and adopting it first could reissue a tick
    // after a crash.
    fn advance(&self, c: &mut Counters, next: u64) -> Result<u64, String> {
        if self.lamport_file.is_some() && next > c.lamport_reserved {
            self.reserve_lamport(c, next)?;
        }
        c.lamport = next;
        Ok(next)
    }

    fn reserve_lamport(&self, c: &mut Counters, through: u64) -> Result<(), String> {
        let next = through + LAMPORT_RESERVATION;
        if let Some(file) = &self.lamport_file {
            write_reservation(file, next)?;
        }
        c.lamport_reserved = next;
        Ok(())
    }

    fn reserve_commit(&self, c: &mut Counters, through: u64) -> Result<(), String> {
        let next = through + LAMPORT_RESERVATION;
        if let Some(file) = &self.commit_file {
            write_reservation(file, next)?;
        }
        c.commit_reserved = next;
        Ok(())
    }
}

/// An `RE-Clock` header value, parsed: the sender's lamport, or `None` when the
/// value is malformed (SURFACE_SPEC "The header": a canonical lower-case UUID
/// and an integer in `[0, 2^53 - 1]` with no sign or leading zeros, each once,
/// in that order, separated by `;` with optional spaces).
pub fn parse_header(value: &str) -> Option<u64> {
    let mut parts = value.split(';');
    let (first, second) = (parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let uuid = first.trim_matches(' ').strip_prefix("instance=")?;
    let digits = second.trim_matches(' ').strip_prefix("lamport=")?;
    if !canonical_uuid(uuid) || uuid.bytes().any(|c| c.is_ascii_uppercase()) {
        return None;
    }
    if digits.is_empty()
        || digits.len() > 16
        || !digits.bytes().all(|c| c.is_ascii_digit())
        || (digits.len() > 1 && digits.starts_with('0'))
    {
        return None;
    }
    digits.parse::<u64>().ok().filter(|n| *n <= MAX_LAMPORT)
}

pub fn canonical_uuid(s: &str) -> bool {
    s.len() == 36
        && s.bytes().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == b'-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// The mark in `file`; 0 when the instance has never run.
pub fn read_reservation(file: &Path) -> Result<u64, String> {
    if !file.exists() {
        return Ok(0);
    }
    let text = fs::read_to_string(file)
        .map_err(|_| format!("Lamport clock file {} cannot be read", file.display()))?;
    let text: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    if text.is_empty() || !text.bytes().all(|c| c.is_ascii_digit()) {
        return Err(format!(
            "Lamport clock file {} does not hold a whole number: \"{text}\"",
            file.display()
        ));
    }
    text.parse().map_err(|_| {
        format!(
            "Lamport clock file {} does not hold a whole number: \"{text}\"",
            file.display()
        )
    })
}

fn write_reservation(file: &Path, value: u64) -> Result<(), String> {
    // <uuid>.lamport-tmp / <uuid>.commit-tmp: one temp file per mark.
    let extension = file.extension().and_then(|e| e.to_str()).unwrap_or("mark");
    let tmp = file.with_extension(format!("{extension}-tmp"));
    let unwritable = || format!("Lamport clock file {} cannot be written", tmp.display());
    if let Some(dir) = file.parent() {
        fs::create_dir_all(dir).map_err(|_| unwritable())?;
    }
    fs::write(&tmp, format!("{value}\n")).map_err(|_| unwritable())?;
    // Atomic: the old mark or the new one, never a torn one.
    fs::rename(&tmp, file).map_err(|_| unwritable())
}

fn lock_instance(file: &Path, uuid: &str) -> Result<File, String> {
    let lock = file.with_extension("lock");
    let unopenable = || format!("Instance lock {} cannot be opened", lock.display());
    if let Some(dir) = lock.parent() {
        fs::create_dir_all(dir).map_err(|_| unopenable())?;
    }
    let handle = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o644)
        .open(&lock)
        .map_err(|_| unopenable())?;
    // SAFETY: a zeroed `flock` is a valid value; the fields that matter are set
    // below, and `l_start` / `l_len` 0 cover the whole file.
    let mut request: libc::flock = unsafe { std::mem::zeroed() };
    request.l_type = libc::F_WRLCK as _;
    request.l_whence = libc::SEEK_SET as _;
    // SAFETY: `handle` is an open descriptor for the life of the call and
    // `request` is a valid `flock`.
    if unsafe { libc::fcntl(handle.as_raw_fd(), libc::F_SETLK, &request) } != 0 {
        return Err(format!(
            "Instance {uuid} is already live: another process holds {}. Two instances may not share a UUID (RealityEngine_CI#296).",
            lock.display()
        ));
    }
    Ok(handle)
}

fn clock_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("INSTANCE_CLOCK_DIR").filter(|dir| !dir.is_empty()) {
        return dir.into();
    }
    let home = std::env::var_os("HOME").filter(|home| !home.is_empty());
    home.map_or_else(|| PathBuf::from("."), PathBuf::from)
        .join(".reality-engine")
        .join("clock")
}

/// A version-7 UUID (RFC 9562): 48 bits of Unix milliseconds, then random bits.
fn mint_v7() -> String {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
        & 0xFFFF_FFFF_FFFF;
    let mut random = [0u8; 10];
    let urandom = File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut random));
    if urandom.is_err() {
        use std::hash::{BuildHasher, RandomState};
        let fill = [
            RandomState::new().hash_one(ms),
            RandomState::new().hash_one(std::process::id()),
        ];
        for (byte, source) in random
            .iter_mut()
            .zip(fill.iter().flat_map(|word| word.to_be_bytes()))
        {
            *byte = source;
        }
    }
    let rand_a = u16::from_be_bytes([random[0], random[1]]) & 0x0FFF;
    let variant = (u16::from_be_bytes([random[2], random[3]]) & 0x3FFF) | 0x8000;
    let node = random[4..]
        .iter()
        .fold(0u64, |acc, byte| (acc << 8) | u64::from(*byte));
    format!(
        "{:08x}-{:04x}-{:04x}-{variant:04x}-{node:012x}",
        ms >> 16,
        ms & 0xFFFF,
        0x7000 | rand_a,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn scratch_dir() -> PathBuf {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "re-rust-clock-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    const UUID: &str = "0192f3a4-5b6c-7d8e-9f01-23456789abcd";

    #[test]
    fn minted_is_a_canonical_version_7_uuid() {
        let a = InstanceClock::minted();
        let b = InstanceClock::minted();
        assert!(canonical_uuid(a.instance()), "{}", a.instance());
        assert_eq!(&a.instance()[14..15], "7");
        assert!("89ab".contains(&a.instance()[19..20]), "{}", a.instance());
        assert_ne!(a.instance(), b.instance());
        assert_eq!((a.lamport(), a.commit()), (0, 0));
    }

    #[test]
    fn without_an_allocation_the_clock_is_minted_and_unpersisted() {
        let dir = scratch_dir();
        for allocated in [None, Some(""), Some("not-a-uuid")] {
            let clock = InstanceClock::boot_from(allocated, &dir).unwrap();
            assert!(canonical_uuid(clock.instance()));
        }
        assert!(!dir.exists(), "a minted clock writes nothing");
    }

    #[test]
    fn an_allocated_uuid_is_lowercased_and_reserves_before_issuing() {
        let dir = scratch_dir();
        let clock = InstanceClock::boot_from(Some(&UUID.to_uppercase()), &dir).unwrap();
        assert_eq!(clock.instance(), UUID);
        let file = dir.join(format!("{UUID}.lamport"));
        let commit = dir.join(format!("{UUID}.commit"));
        assert!(dir.join(format!("{UUID}.lock")).exists());
        assert_eq!(read_reservation(&file).unwrap(), LAMPORT_RESERVATION);
        assert_eq!(read_reservation(&commit).unwrap(), LAMPORT_RESERVATION);
        for expected in 1..=LAMPORT_RESERVATION {
            assert_eq!(clock.commit_step().unwrap(), (expected, expected));
        }
        assert_eq!(read_reservation(&file).unwrap(), LAMPORT_RESERVATION);
        assert_eq!(
            clock.commit_step().unwrap(),
            (LAMPORT_RESERVATION + 1, LAMPORT_RESERVATION + 1)
        );
        assert_eq!(
            read_reservation(&file).unwrap(),
            2 * LAMPORT_RESERVATION + 1
        );
        assert_eq!(
            read_reservation(&commit).unwrap(),
            2 * LAMPORT_RESERVATION + 1
        );
    }

    #[test]
    fn sends_and_receives_tick_lamport_only() {
        let clock = InstanceClock::minted();
        assert_eq!(clock.send().unwrap(), 1);
        assert_eq!(clock.receive(None).unwrap(), 2);
        assert_eq!(clock.receive(Some(10)).unwrap(), 11);
        assert_eq!(
            clock.receive(Some(3)).unwrap(),
            12,
            "max(local, received) + 1"
        );
        assert_eq!(clock.commit_step().unwrap(), (13, 1));
        assert!(clock.lamport() >= clock.commit());
    }

    #[test]
    fn a_receive_past_the_mark_is_persisted_before_it_is_used() {
        let dir = scratch_dir();
        let file = dir.join(format!("{UUID}.lamport"));
        let clock = InstanceClock::persisted(UUID, &file).unwrap();
        assert_eq!(clock.receive(Some(5_000_000)).unwrap(), 5_000_001);
        assert_eq!(
            read_reservation(&file).unwrap(),
            5_000_001 + LAMPORT_RESERVATION
        );
    }

    #[test]
    fn a_restart_resumes_from_the_marks_so_no_tick_repeats() {
        let dir = scratch_dir();
        let file = dir.join(format!("{UUID}.lamport"));
        let issued = {
            let clock = InstanceClock::persisted(UUID, &file).unwrap();
            clock.commit_step().unwrap();
            clock.send().unwrap();
            clock.commit_step().unwrap()
        };
        let clock = InstanceClock::persisted(UUID, &file).unwrap();
        assert_eq!(
            (clock.lamport(), clock.commit()),
            (LAMPORT_RESERVATION, LAMPORT_RESERVATION)
        );
        let next = clock.commit_step().unwrap();
        assert!(next.0 > issued.0 && next.1 > issued.1);
    }

    #[test]
    fn a_pre_600_directory_seeds_both_marks() {
        let dir = scratch_dir();
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join(format!("{UUID}.lamport"));
        fs::write(&file, "4096\n").unwrap();
        let clock = InstanceClock::persisted(UUID, &file).unwrap();
        assert_eq!((clock.lamport(), clock.commit()), (4096, 4096));
        assert_eq!(
            read_reservation(&dir.join(format!("{UUID}.commit"))).unwrap(),
            4096 + LAMPORT_RESERVATION
        );
    }

    #[test]
    fn a_clock_file_that_is_not_a_whole_number_refuses_the_boot() {
        let dir = scratch_dir();
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join(format!("{UUID}.lamport"));
        fs::write(&file, "12x\n").unwrap();
        let error = InstanceClock::persisted(UUID, &file).unwrap_err();
        assert!(
            error.contains("does not hold a whole number: \"12x\""),
            "{error}"
        );
    }

    #[test]
    fn the_header_parses_as_the_spec_says() {
        let ok = "instance=0192f3a4-5b6c-7d8e-9f01-23456789abcd; lamport=42";
        assert_eq!(parse_header(ok), Some(42));
        assert_eq!(
            parse_header("instance=0192f3a4-5b6c-7d8e-9f01-23456789abcd;lamport=0"),
            Some(0)
        );
        assert_eq!(
            parse_header("instance=0192f3a4-5b6c-7d8e-9f01-23456789abcd; lamport=9007199254740991"),
            Some(MAX_LAMPORT)
        );
        for bad in [
            "",
            "bad",
            "lamport=1; instance=0192f3a4-5b6c-7d8e-9f01-23456789abcd",
            "instance=0192F3A4-5B6C-7D8E-9F01-23456789ABCD; lamport=1",
            "instance=0192f3a4-5b6c-7d8e-9f01-23456789abcd; lamport=01",
            "instance=0192f3a4-5b6c-7d8e-9f01-23456789abcd; lamport=-1",
            "instance=0192f3a4-5b6c-7d8e-9f01-23456789abcd; lamport=9007199254740992",
            "instance=0192f3a4-5b6c-7d8e-9f01-23456789abcd; lamport=1; x=2",
            "instance=0192f3a4-5b6c-7d8e-9f01-23456789abcd",
        ] {
            assert_eq!(parse_header(bad), None, "{bad:?}");
        }
    }
}
