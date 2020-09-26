use std::fmt;

use parking_lot::{Mutex, MutexGuard};

pub use iou;

/// The consumer instance type for Linux, providing a thin wrapper above [`iou::IoUring`].
pub struct ConsumerInstance {
    // TODO: Rwlocks?
    io_uring: Mutex<iou::IoUring>,
}

impl ConsumerInstance {
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
