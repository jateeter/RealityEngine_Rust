//! The instance's Lamport clock (RealityEngine_CI#296): `{instance, lamport, step}`.
//!
//! A UUID belongs to an *instance* -- cpp-1, rust-1 -- never to an engine type or
//! an image, and no two instances of any engine type may share one. The instance
//! registry allocates it and passes it as `INSTANCE_UUID`; an instance launched
//! without one mints a version-7 UUID at boot, which is unique by construction.
//!
//! `lamport` ticks once per committed step and is never reset, so
//! `(instance, lamport)` names one step uniquely. For an allocated instance that
//! holds across restarts too: `INSTANCE_CLOCK_DIR` (default
//! `~/.reality-engine/clock/`) keeps `<uuid>.lamport`, a high-water mark reserved
//! [`LAMPORT_RESERVATION`] ticks ahead. A boot resumes from the mark and reserves
//! the next block before issuing any tick, and a tick past the persisted mark is
//! always preceded by the write of the next block, never followed by it. The
//! write is temp + rename, so a crash leaves the old mark or the new one.
//!
//! The instance also holds an exclusive `fcntl` lock on `<uuid>.lock` for its
//! whole life, so a second live process -- of any engine type -- presenting the
//! same UUID refuses to boot. A POSIX record lock, as C++ and LSP (`lockf`) and
//! Scala (`FileChannel.tryLock`) take, because that is what
//! `instance_uuids.py live` asks about with `F_GETLK`; `File::try_lock` is
//! `flock`, which neither sees. The kernel releases it when the process exits,
//! so a crash leaves no stale lock. A separate file, because the clock file is
//! replaced by rename and a lock on a replaced inode guards nothing.
//!
//! The semantics, error text and file names follow C++ `instance_clock.cpp`;
//! SURFACE_SPEC.md, "The instance clock", governs.

use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    os::fd::AsRawFd,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub const LAMPORT_RESERVATION: u64 = 1024;

#[derive(Debug)]
pub struct InstanceClock {
    uuid: String,
    lamport: u64,
    reserved: u64,
    /// `<uuid>.lamport`; `None` for a minted, unpersisted clock.
    file: Option<PathBuf>,
    /// Holds the instance lock for as long as the clock lives; dropping it
    /// closes the descriptor, which releases the lock.
    _lock: Option<File>,
}

impl Default for InstanceClock {
    fn default() -> Self {
        Self::minted()
    }
}

impl InstanceClock {
    /// The clock for this boot, from `INSTANCE_UUID` / `INSTANCE_CLOCK_DIR`.
    /// Fails when an allocated instance cannot keep its clock or another live
    /// process holds its UUID: either way ticks could be issued twice, which is
    /// the one thing the clock must not do.
    pub fn boot() -> Result<Self, String> {
        Self::boot_from(std::env::var("INSTANCE_UUID").ok().as_deref(), &clock_dir())
    }

    /// [`boot`](Self::boot) with its inputs given (tests).
    pub fn boot_from(allocated: Option<&str>, dir: &Path) -> Result<Self, String> {
        let allocated = allocated.unwrap_or("");
        if !canonical_uuid(allocated) {
            if !allocated.is_empty() {
                eprintln!("INSTANCE_UUID \"{allocated}\" is not a canonical UUID; minting one");
            }
            return Ok(Self::minted());
        }
        let uuid = allocated.to_ascii_lowercase();
        let file = dir.join(format!("{uuid}.lamport"));
        Self::persisted(&uuid, &file)
    }

    /// A minted, unpersisted clock -- what an instance without an allocation runs.
    pub fn minted() -> Self {
        Self {
            uuid: mint_v7(),
            lamport: 0,
            reserved: 0,
            file: None,
            _lock: None,
        }
    }

    /// An allocated clock kept in `file`.
    pub fn persisted(uuid: &str, file: &Path) -> Result<Self, String> {
        let lock = lock_instance(file, uuid)?; // before anything is read or written
        let lamport = read_reservation(file)?;
        let mut clock = Self {
            uuid: uuid.to_owned(),
            lamport,
            reserved: 0,
            file: Some(file.to_owned()),
            _lock: Some(lock),
        };
        clock.reserve(lamport)?;
        Ok(clock)
    }

    /// Advance for a committed step; returns the new value.
    pub fn tick(&mut self) -> Result<u64, String> {
        let next = self.lamport + 1;
        if self.file.is_some() && next > self.reserved {
            self.reserve(next)?; // persist first, then issue
        }
        self.lamport = next;
        Ok(next)
    }

    pub fn lamport(&self) -> u64 {
        self.lamport
    }

    pub fn instance(&self) -> &str {
        &self.uuid
    }

    fn reserve(&mut self, through: u64) -> Result<(), String> {
        let next = through + LAMPORT_RESERVATION;
        if let Some(file) = &self.file {
            write_reservation(file, next)?;
        }
        self.reserved = next;
        Ok(())
    }
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
    let tmp = file.with_extension("lamport-tmp");
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
        assert_eq!(a.lamport(), 0);
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
        let mut clock = InstanceClock::boot_from(Some(&UUID.to_uppercase()), &dir).unwrap();
        assert_eq!(clock.instance(), UUID);
        let file = dir.join(format!("{UUID}.lamport"));
        assert!(dir.join(format!("{UUID}.lock")).exists());
        assert_eq!(read_reservation(&file).unwrap(), LAMPORT_RESERVATION);
        for expected in 1..=LAMPORT_RESERVATION {
            assert_eq!(clock.tick().unwrap(), expected);
        }
        assert_eq!(read_reservation(&file).unwrap(), LAMPORT_RESERVATION);
        assert_eq!(clock.tick().unwrap(), LAMPORT_RESERVATION + 1);
        assert_eq!(
            read_reservation(&file).unwrap(),
            2 * LAMPORT_RESERVATION + 1
        );
    }

    #[test]
    fn a_restart_resumes_from_the_mark_so_no_tick_repeats() {
        let dir = scratch_dir();
        let file = dir.join(format!("{UUID}.lamport"));
        let issued = {
            let mut clock = InstanceClock::persisted(UUID, &file).unwrap();
            clock.tick().unwrap();
            clock.tick().unwrap()
        };
        let mut clock = InstanceClock::persisted(UUID, &file).unwrap();
        assert_eq!(clock.lamport(), LAMPORT_RESERVATION);
        assert!(clock.tick().unwrap() > issued);
        assert_eq!(read_reservation(&file).unwrap(), 2 * LAMPORT_RESERVATION);
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
}
