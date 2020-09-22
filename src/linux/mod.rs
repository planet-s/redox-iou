use std::{io, iter, fmt};

use iou::CompletionQueueEvent;
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};

pub struct ConsumerInstance {
    io_uring: Mutex<iou::IoUring>,
}

impl ConsumerInstance {
    pub fn lock(&self) -> MutexGuard<'_, iou::IoUring> {
        self.io_uring.lock()
    }
}

impl fmt::Debug for ConsumerInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerInstance")
            .finish()
    }
}
