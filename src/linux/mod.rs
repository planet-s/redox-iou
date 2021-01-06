use std::fmt;

use libc::c_int;
use parking_lot::{Mutex, MutexGuard};

pub use iou;

/// The consumer instance type for Linux, providing a thin wrapper above [`iou::IoUring`].
pub struct ConsumerInstance {
    // TODO: Rwlocks?
    io_uring: Mutex<iou::IoUring>,
}

impl ConsumerInstance {
    /// Wrap an io_uring from `iou`.
    pub fn from_iou(io_uring: iou::IoUring) -> Self {
        Self {
            io_uring: Mutex::new(io_uring),
        }
    }
    /// Lock the mutex guard of the consumer instance, retrieving a temporary guard with exclusive
    /// access to the ring.
    pub fn lock(&self) -> MutexGuard<'_, iou::IoUring> {
        self.io_uring.lock()
    }
}

impl fmt::Debug for ConsumerInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerInstance")
            // TODO
            .finish()
    }
}

bitflags::bitflags! {
    /// Linux-specific read flags, which can be used in `preadv2(2)`, `libaio`, and obviously
    /// `io_uring`. These have call-specific granularity, and do not require fcntl calls in
    /// between.
    pub struct ReadFlags: c_int {
        /// This particular call is considered high-priority, making use of additional system
        /// resources to improve latency. (`RWF_HIPRI`)
        const HIPRI = uapi::c::RWF_HIPRI;
        /// Do not wait for any data to become available; instead, return `EAGAIN` if blocking
        /// would otherwise happen.
        const NOWAIT = uapi::c::RWF_NOWAIT;
    }
}
bitflags::bitflags! {
    /// Linux-specific write flags, which can be used in `preadv2(2)`, `libaio`, and obviously
    /// `io_uring`. These have call-specific granularity, and do not require fcntl calls in
    /// between.
    ///
    /// Refer to `preadv2(2)` for further information about these flags.
    pub struct WriteFlags: c_int {
        /// Ensure that data is flushed to disk before the system call is complete, but not
        /// metadata unnecessesary for data integrity (that is, things like file sizes are updated,
        /// but not e.g. timestamps). (`RWF_DSYNC`)
        const DSYNC = uapi::c::RWF_DSYNC;
        /// This particular call is considered high-priority, making use of additional system
        /// resources to improve latency. (`RWF_HIPRI`)
        const HIPRI = uapi::c::RWF_HIPRI;
        /// Ensure that data is flushed to disk before the system call is complete, including other
        /// metadata. (`RWF_SYNC`)
        const SYNC = uapi::c::RWF_SYNC;
        /// Append the data to the end of the file, regardless of what offset has been set. If the
        /// offset is -1, the file offset is also updated. (`RWF_APPEND`)
        ///
        /// (Note that `preadv2` acts as regular `readv` if the offset is -1, which this flag
        /// overrides in that it pre-seeks to the end of the file.) 
        const APPEND = uapi::c::RWF_APPEND;
    }
}
