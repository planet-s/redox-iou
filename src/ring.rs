use std::sync::atomic::Ordering;
use std::{fmt, mem};

use syscall::error::ESHUTDOWN;
use syscall::error::{Error, Result};

pub use syscall::io_uring::v1::{Ring, RingPopError, RingPushError, RingStatus};

/// A safe wrapper over the raw `Ring` interface, that takes care of the mmap offset, as well
/// as the global `Ring` structure. Only allows sending items.
///
/// The `SpscSender` is `Send`, and can thus be transferred between threads. However, it is
/// `!Sync`, since it does not take multiple atomic senders into account. Thus, it will have to
/// be wrapped in something like a `Mutex`, if that is necessary.
pub struct SpscSender<T> {
    /// An internally reference counted pointer to the shared ring state struct.
    ///
    /// Must be an exact offset that was mmapped.
    pub(crate) ring: *const Ring<T>,

    /// A pointer to the entries of the ring, must also be mmapped.
    pub(crate) entries_base: *mut T,
}

unsafe impl<T: Send> Send for SpscSender<T> {}
unsafe impl<T: Send> Sync for SpscSender<T> {}

impl<T> SpscSender<T> {
    /// Construct this high-level sender wrapper, from raw pointers to the ring header and entries.
    ///
    /// # Safety
    ///
    /// This method is unsafe, since it allows creating a wrapper that implicitly dereferences
    /// these pointers, without any validations whatsoever. While there is no practial necessity
    /// for the ring header and entries to come directly from an mmap for the `io_uring:` scheme,
    /// they still have to safely dereferencable.
    pub unsafe fn from_raw(ring: *const Ring<T>, entries_base: *mut T) -> Self {
        Self { ring, entries_base }
    }
    pub fn try_send(&mut self, item: T) -> Result<(), RingPushError<T>> {
        unsafe {
            let ring = self.ring_header();
            ring.push_back_spsc(self.entries_base, item)
        }
    }
    pub fn spin_on_send(&mut self, mut item: T) -> Result<(), RingSendError<T>> {
        loop {
            match self.try_send(item) {
                Ok(()) => return Ok(()),
                Err(RingPushError::Full(i)) => {
                    item = i;
                    core::sync::atomic::spin_loop_hint();
                    continue;
                }
                Err(RingPushError::Shutdown(i)) => return Err(RingSendError::Shutdown(i)),
            }
        }
    }
    pub fn deallocate(self) -> Result<()> {
        unsafe {
            // the entries_base pointer is coupled to the ring itself. hence, when the ring is
            // deallocated, so will the entries.
            let Self { ring, entries_base } = self;
            mem::forget(self);

            let ring = &*ring;

            syscall::funmap(ring as *const _ as usize)?;
            syscall::funmap(entries_base as usize)?;
            Ok(())
        }
    }
    /// Retrieve the ring header, which stores head and tail pointers, and epochs.
    ///
    /// # Safety
    /// This is unsafe because it allows arbitrarily changing the head and tail pointers
    /// (indices). While the only allowed entries thus far have a valid repr, and thus allow
    /// any bytes to be reinterpreted, this can produce invalid commands that may corrupt the
    /// memory of the current process.
    pub unsafe fn ring_header(&self) -> &Ring<T> {
        &*self.ring
    }

    /// Wake the receiver up if it was blocking on a new message, without sending anything.
    /// This is useful when building a [`core::future::Future`] executor, for the
    /// [`core::task::Waker`].
    pub fn notify(&self) {
        let ring = unsafe { self.ring_header() };
        let _ = ring.push_epoch.fetch_add(1, Ordering::Relaxed);
        // TODO: Syscall here?
    }
}
impl<T> Drop for SpscSender<T> {
    fn drop(&mut self) {
        unsafe {
            let ring = self.ring_header();
            ring.sts
                .fetch_or(RingStatus::DROP.bits(), Ordering::Release);

            let _ = syscall::funmap(self.ring as *const _ as usize);
            let _ = syscall::funmap(self.entries_base as usize);
        }
    }
}
impl<T> fmt::Debug for SpscSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: More useful information
        f.debug_struct("SpscSender")
            .field("ring_header", &self.ring)
            .field("entries_base", &self.entries_base)
            .finish()
    }
}

/// A safe wrapper over the raw [`Ring`] interface, that takes care of the mmap offset, as well
/// as the global [`Ring`] structure. Only allows receiving items.
///
/// The wrapper is [`Send`], and can thus be transferred between threads. However, it is
/// [`!Sync]`, since it does not take multiple atomic receivers into account. Thus, it will
/// have to be wrapped in something like a mutex if that is necessary.
///
/// [`Ring`]: ./struct.Ring.html
pub struct SpscReceiver<T> {
    ring: *const Ring<T>,
    entries_base: *const T,
}
unsafe impl<T: Send> Send for SpscReceiver<T> {}
unsafe impl<T: Send> Sync for SpscReceiver<T> {}

impl<T> SpscReceiver<T> {
    /// Construct this high-level receiver wrapper, from raw points of the ring header and entries.
    ///
    /// # Safety
    ///
    /// The same exact same invariants as with [`SpscSender::from_raw`] apply here as well.
    pub unsafe fn from_raw(ring: *const Ring<T>, entries_base: *const T) -> Self {
        Self { ring, entries_base }
    }
    pub fn try_recv(&mut self) -> Result<T, RingPopError> {
        unsafe {
            let ring = &*self.ring;
            ring.pop_front_spsc(self.entries_base)
        }
    }
    pub fn spin_on_recv(&mut self) -> Result<T, RingRecvError> {
        loop {
            match self.try_recv() {
                Ok(item) => return Ok(item),
                Err(RingPopError::Empty { .. }) => {
                    core::sync::atomic::spin_loop_hint();
                    continue;
                }
                Err(RingPopError::Shutdown) => return Err(RingRecvError::Shutdown),
            }
        }
    }
    pub fn try_iter(&mut self) -> impl Iterator<Item = T> + '_ {
        core::iter::from_fn(move || self.try_recv().ok())
    }

    pub fn deallocate(self) -> Result<()> {
        unsafe {
            // the entries_base pointer is coupled to the ring itself. hence, when the ring is
            // deallocated, so will the entries.
            let Self { ring, entries_base } = self;
            mem::forget(self);

            let ring = &*ring;

            syscall::funmap(ring as *const _ as usize)?;
            syscall::funmap(entries_base as usize)?;
            Ok(())
        }
    }
    /// Retrieve the ring header, which stores head and tail pointers, and epochs.
    ///
    /// # Safety
    /// Unsafe for the same reasons as with [`SpscSender`].
    ///
    /// [`SpscSender`]: ./enum.SpscSender.html
    pub unsafe fn ring_header(&self) -> &Ring<T> {
        &*self.ring
    }
}
impl<T> Drop for SpscReceiver<T> {
    fn drop(&mut self) {
        unsafe {
            let _ = syscall::funmap(self.ring as *const _ as usize);
            let _ = syscall::funmap(self.entries_base as usize);
        }
    }
}
impl<T> fmt::Debug for SpscReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: More useful information
        f.debug_struct("SpscReceiver")
            .field("ring_header", &self.ring)
            .field("entries_base", &self.entries_base)
            .finish()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum RingSendError<T> {
    Shutdown(T),
}
impl<T> From<RingSendError<T>> for Error {
    fn from(error: RingSendError<T>) -> Error {
        match error {
            RingSendError::Shutdown(_) => Error::new(ESHUTDOWN),
        }
    }
}
impl<T> core::fmt::Display for RingSendError<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Shutdown(_) => write!(f, "receiver side has shut down"),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum RingRecvError {
    Shutdown,
}
impl From<RingRecvError> for Error {
    fn from(error: RingRecvError) -> Error {
        match error {
            RingRecvError::Shutdown => Error::new(ESHUTDOWN),
        }
    }
}
impl core::fmt::Display for RingRecvError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Shutdown => write!(f, "sender side has shut down"),
        }
    }
}

#[cfg(test)]
mod tests {

    //
    // These are mainly tests for the wrapper types in this file. See also the lower-level tests in
    // redox_syscall.
    //

    use super::{SpscReceiver, SpscSender};
    use std::{mem, sync::atomic::AtomicUsize};
    use syscall::io_uring::v1::{CachePadded, CqEntry64, Ring, RingPopError, RingPushError};

    fn setup_ring(count: usize) -> (Ring<CqEntry64>, *mut CqEntry64) {
        use std::alloc::{alloc, Layout};
        let base = unsafe {
            alloc(
                Layout::from_size_align(
                    count * mem::size_of::<CqEntry64>(),
                    mem::align_of::<CqEntry64>(),
                )
                .unwrap(),
            ) as *mut CqEntry64
        };

        (
            Ring {
                size: count,
                push_epoch: CachePadded(AtomicUsize::new(0)),
                pop_epoch: CachePadded(AtomicUsize::new(0)),

                head_idx: CachePadded(AtomicUsize::new(0)),
                tail_idx: CachePadded(AtomicUsize::new(0)),
                sts: CachePadded(AtomicUsize::new(0)),

                _marker: core::marker::PhantomData,
            },
            base,
        )
    }

    macro_rules! simple_multithreaded_test(($sender:expr, $receiver:expr) => {{
        let second = std::thread::spawn(move || {
            let mut i = 0;
            'pushing: loop {
                if i > 4096 { break 'pushing }
                let value = CqEntry64 {
                    user_data: i,
                    status: 1337,
                    flags: 0xDEADBEEF,
                    extra: 127,
                };

                'retry: loop {
                    match $sender.try_send(value) {
                        Ok(()) => {
                            i += 1;
                            continue 'pushing;
                        }
                        Err(RingPushError::Full(_)) => {
                            std::thread::yield_now();
                            continue 'retry;
                        }
                        Err(RingPushError::Shutdown(_)) => break 'pushing,
                    }
                }
            }
        });
        let mut i = 0;
        'popping: loop {
            if i > 4096 { break 'popping }
            'retry: loop {
                match $receiver.try_recv() {
                    Ok(c) => {
                        assert_eq!(c, CqEntry64 {
                            user_data: i,
                            status: 1337,
                            flags: 0xDEADBEEF,
                            extra: 127,
                        });
                        i += 1;
                        continue 'popping;
                    }
                    Err(RingPopError::Empty { .. }) => {
                        std::thread::yield_now();
                        continue 'retry;
                    }
                    Err(RingPopError::Shutdown) => break 'popping,
                }
            }
        }
        second.join().unwrap();
    }});

    #[test]
    fn multithreaded_spsc() {
        let (ring, entries_base) = setup_ring(64);
        let ring = &ring;
        let mut sender = SpscSender { ring, entries_base };
        let mut receiver = SpscReceiver { ring, entries_base };

        simple_multithreaded_test!(sender, receiver);
    }
}
