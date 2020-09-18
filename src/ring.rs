use std::convert::{TryFrom, TryInto};
use std::sync::atomic::Ordering;
use std::{fmt, mem};

use syscall::error::{EIO, ESHUTDOWN};
use syscall::error::{Error, Result};

pub use syscall::io_uring::v1::{BrokenRing, Ring, RingPopError, RingPushError, RingStatus};

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
    ring: *const Ring<T>,

    /// A pointer to the entries of the ring, must also be mmapped.
    entries_base: *mut T,

    /// Size of the ring header, in bytes.
    ring_size: u32,

    /// The log2 of the size in bytes of the entries array.
    log2_entry_count: u32,
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
    ///
    /// Since munmap is called in the destructor, the sender must either be dropped manually, or be
    /// allocated using mmap.
    ///
    /// The `log2_entry_count` must be the exact base-two logarithm of the entry count; that is,
    /// the number of bits to shift one by, to obtain the number of entries. That number,
    /// multiplied by the size of T, _must not_ overflow isize when added with the `entries_base`
    /// pointer.
    #[inline]
    pub unsafe fn from_raw(
        ring: *const Ring<T>,
        ring_size: usize,
        entries_base: *mut T,
        log2_entry_count: u32,
    ) -> Self {
        debug_assert!(!ring.is_null());
        debug_assert!((ring as usize)
            .checked_add(ring_size)
            .map_or(false, |added| isize::try_from(added).is_ok()));
        debug_assert!(!entries_base.is_null());
        {
            let entries_size = 1usize.checked_shl(log2_entry_count).expect(
                "expected log2_entry_count not to be larger than the pointer width in bits",
            );
            debug_assert!((entries_base as usize)
                .checked_add(entries_size)
                .map_or(false, |added| isize::try_from(added).is_ok()));
        }
        debug_assert_ne!(mem::size_of::<T>(), 0);

        let ring_size = u32::try_from(ring_size)
            .expect("expected system page size (ring size) to be smaller than 2^32");

        Self {
            ring,
            entries_base,
            ring_size,
            log2_entry_count,
        }
    }
    /// Attempt to send a new item to the ring, failing if the ring is shut down, or if the ring is
    /// full.
    #[inline]
    pub fn try_send(&mut self, item: T) -> Result<(), RingPushError<T>> {
        unsafe {
            let ring = self.ring_header();
            ring.push_back(self.entries_base, self.log2_entry_count as usize, item)
        }
    }
    /// Busy-wait for the ring to no longer be full.
    #[inline]
    pub fn spin_on_send(&mut self, mut item: T) -> Result<(), RingSendError<T>> {
        loop {
            match self.try_send(item) {
                Ok(()) => return Ok(()),
                Err(RingPushError::Full(i)) => {
                    item = i;
                    core::sync::atomic::spin_loop_hint();
                    continue;
                }
                Err(RingPushError::Shutdown(item)) => return Err(RingSendError::Shutdown(item)),
                Err(RingPushError::Broken(item)) => return Err(RingSendError::Broken(item)),
            }
        }
    }
    /// Deallocate and shut down the ring, freeing the underlying memory.
    #[cold]
    pub fn deallocate(self) -> Result<()> {
        unsafe {
            // the entries_base pointer is coupled to the ring itself. hence, when the ring is
            // deallocated, so will the entries.
            let Self {
                ring,
                entries_base,
                ring_size,
                log2_entry_count,
            } = self;
            mem::forget(self);

            let ring = &*ring;
            let _ = ring
                .sts
                .fetch_or(RingStatus::DROP.bits(), Ordering::Relaxed);

            let entries_size = 1usize
                .checked_shl(log2_entry_count)
                .unwrap()
                .checked_mul(mem::size_of::<T>())
                .unwrap();

            syscall::funmap(ring as *const _ as usize, ring_size as usize)?;
            syscall::funmap(entries_base as usize, entries_size)?;
            Ok(())
        }
    }
    /// Retrieve the ring header, which stores head and tail pointers, and epochs.
    ///
    /// # Safety
    ///
    /// This is unsafe because it allows arbitrarily changing the head and tail pointers
    /// (indices). While the only allowed entries thus far have a valid repr, and thus allow
    /// any bytes to be reinterpreted, this can produce invalid commands that may corrupt the
    /// memory of the current process.
    #[inline]
    pub unsafe fn ring_header(&self) -> &Ring<T> {
        &*self.ring
    }

    /// Wake the receiver up if it was blocking on a new message, without sending anything.
    /// This is useful when building a [`core::future::Future`] executor, for the
    /// [`core::task::Waker`].
    #[inline]
    pub fn notify(&self) {
        let ring = unsafe { self.ring_header() };
        let _ = ring.push_epoch.fetch_add(1, Ordering::Relaxed);
        // TODO: Syscall here?
    }

    /// Get the number of free entry slots that can be pushed to, at the time the function was
    /// called.
    ///
    /// This is fallible, and an error is returned if the ring is no longer in a correct state.
    #[inline]
    pub fn free_entry_count(&self) -> Result<usize> {
        unsafe {
            self.ring_header()
                .available_entry_count(self.log2_entry_count as usize)
                .map_err(|_| Error::new(EIO))
        }
    }
}
impl<T> Drop for SpscSender<T> {
    #[cold]
    fn drop(&mut self) {
        unsafe {
            let ring = self.ring_header();
            ring.sts
                .fetch_or(RingStatus::DROP.bits(), Ordering::Release);

            let entries_size = 1usize
                .wrapping_shl(self.log2_entry_count)
                .wrapping_mul(mem::size_of::<T>());

            let _ = syscall::funmap(self.ring as *const _ as usize, self.ring_size as usize);
            let _ = syscall::funmap(self.entries_base as usize, entries_size);
        }
    }
}
impl<T> fmt::Debug for SpscSender<T> {
    #[cold]
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
    ring_size: u32,
    log2_entry_count: u32,
}
unsafe impl<T: Send> Send for SpscReceiver<T> {}
unsafe impl<T: Send> Sync for SpscReceiver<T> {}

impl<T> SpscReceiver<T> {
    /// Construct this high-level receiver wrapper, from raw points of the ring header and entries.
    ///
    /// # Safety
    ///
    /// Exactly the same invariants as with [`SpscSender::from_raw`] apply here as well.
    #[cold]
    pub unsafe fn from_raw(
        ring: *const Ring<T>,
        ring_size: usize,
        entries_base: *const T,
        log2_entry_count: u32,
    ) -> Self {
        debug_assert!(!ring.is_null());
        debug_assert!((ring as usize)
            .checked_add(ring_size)
            .map_or(false, |added| isize::try_from(added).is_ok()));
        debug_assert!(!entries_base.is_null());
        {
            let entries_size = 1usize.checked_shl(log2_entry_count).expect(
                "expected log2_entry_count not to be larger than the pointer width in bits",
            );
            debug_assert!((entries_base as usize)
                .checked_add(entries_size)
                .map_or(false, |added| isize::try_from(added).is_ok()));
        }
        debug_assert_ne!(mem::size_of::<T>(), 0);

        let ring_size = u32::try_from(ring_size)
            .expect("expected the system page size to be smaller than 2^32");

        Self {
            ring,
            entries_base,
            ring_size,
            log2_entry_count,
        }
    }

    /// Try to receive a new item from the ring, failing immediately if the ring was empty.
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, RingPopError> {
        unsafe {
            let ring = &*self.ring;
            ring.pop_front(self.entries_base, self.log2_entry_count.try_into().unwrap())
        }
    }
    /// Busy-wait while trying to receive a new item from the ring, or until shutdown.
    #[inline]
    pub fn spin_on_recv(&mut self) -> Result<T, RingRecvError> {
        loop {
            match self.try_recv() {
                Ok(item) => return Ok(item),
                Err(RingPopError::Empty { .. }) => {
                    core::sync::atomic::spin_loop_hint();
                    continue;
                }
                Err(RingPopError::Shutdown) => return Err(RingRecvError::Shutdown),
                Err(RingPopError::Broken) => return Err(RingRecvError::Broken),
            }
        }
    }
    /// Create an iterator over the currently available items, that does not block.
    #[inline]
    pub fn try_iter(&mut self) -> impl Iterator<Item = T> + '_ {
        core::iter::from_fn(move || self.try_recv().ok())
    }

    /// Deallocate the receiver, unmapping the memory used by it, together with a shutdown.
    #[cold]
    pub fn deallocate(self) -> Result<()> {
        unsafe {
            // the entries_base pointer is coupled to the ring itself. hence, when the ring is
            // deallocated, so will the entries.
            let Self {
                ring,
                entries_base,
                log2_entry_count,
                ring_size,
            } = self;
            mem::forget(self);

            let ring = &*ring;

            let _ = ring
                .sts
                .fetch_or(RingStatus::DROP.bits(), Ordering::Relaxed);

            let entries_size = 1usize
                .wrapping_shl(log2_entry_count)
                .wrapping_mul(mem::size_of::<T>());

            syscall::funmap(ring as *const _ as usize, ring_size as usize)?;
            syscall::funmap(entries_base as usize, entries_size)?;
            Ok(())
        }
    }
    /// Retrieve the ring header, which stores head and tail pointers, and epochs.
    ///
    /// # Safety
    ///
    /// Unsafe for the same reasons as with [`SpscSender`].
    ///
    /// [`SpscSender`]: ./enum.SpscSender.html
    #[inline]
    pub unsafe fn ring_header(&self) -> &Ring<T> {
        &*self.ring
    }

    /// Get the number of available entries to pop, at the time this method was called.
    #[inline]
    pub fn available_entry_count(&self) -> Result<usize, BrokenRing> {
        unsafe {
            self.ring_header()
                .available_entry_count(self.log2_entry_count as usize)
        }
    }
}
impl<T> Drop for SpscReceiver<T> {
    #[cold]
    fn drop(&mut self) {
        unsafe {
            let entries_size = 1usize
                .wrapping_shl(self.log2_entry_count)
                .wrapping_mul(mem::size_of::<T>());

            let _ = syscall::funmap(self.ring as *const _ as usize, self.ring_size as usize);
            let _ = syscall::funmap(self.entries_base as usize, entries_size);
        }
    }
}
impl<T> fmt::Debug for SpscReceiver<T> {
    #[cold]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: More useful information
        f.debug_struct("SpscReceiver")
            .field("ring_header", &self.ring)
            .field("entries_base", &self.entries_base)
            .finish()
    }
}

/// An error that can occur when sending to a ring.
#[derive(Debug, Eq, PartialEq)]
pub enum RingSendError<T> {
    /// Pushing a new entry to the ring was impossible, due to a shutdown, most likely due to ring
    /// deinitialization.
    ///
    /// The value from the send attempt is preserved here, for reusage purposes.
    Shutdown(T),

    /// Pushing a new entry to the ring was impossible, due to the ring having entered an
    /// inconsistent state, typically caused by buggy or undefined behavior in either of the
    /// producer or consumer.
    Broken(T),
}
impl<T> From<RingSendError<T>> for Error {
    fn from(error: RingSendError<T>) -> Error {
        match error {
            RingSendError::Shutdown(_) => Error::new(ESHUTDOWN),
            RingSendError::Broken(_) => Error::new(EIO),
        }
    }
}
impl<T> core::fmt::Display for RingSendError<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Shutdown(_) => write!(f, "receiver side has shut down"),
            Self::Broken(_) => write!(f, "broken ring"),
        }
    }
}

/// An error that can occur when receiving from the ring.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RingRecvError {
    /// Popping from the ring was impossible, since the ring had been shutdown from the other side,
    /// most likely due to ring deinitialization.
    Shutdown,

    /// Popping was impossible since the ring had entered an invalid state.
    Broken,
}
impl From<RingRecvError> for Error {
    fn from(error: RingRecvError) -> Error {
        match error {
            RingRecvError::Shutdown => Error::new(ESHUTDOWN),
            RingRecvError::Broken => Error::new(EIO),
        }
    }
}
impl core::fmt::Display for RingRecvError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Shutdown => write!(f, "sender side has shut down"),
            Self::Broken => write!(f, "broken ring"),
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

    fn setup_ring(count: usize) -> (Ring<CqEntry64>, *mut CqEntry64, u32, u32) {
        use std::alloc::{alloc, Layout};

        let base_size = count.checked_mul(mem::size_of::<CqEntry64>()).unwrap();

        assert!(count.is_power_of_two());
        let log2_entry_count = count.trailing_zeros();

        let base = unsafe {
            alloc(Layout::from_size_align(base_size, mem::align_of::<CqEntry64>()).unwrap())
                as *mut CqEntry64
        };

        (
            Ring {
                push_epoch: CachePadded(AtomicUsize::new(0)),
                pop_epoch: CachePadded(AtomicUsize::new(0)),

                head_idx: CachePadded(AtomicUsize::new(0)),
                tail_idx: CachePadded(AtomicUsize::new(0)),
                sts: CachePadded(AtomicUsize::new(0)),

                _marker: core::marker::PhantomData,
            },
            base,
            4096,
            log2_entry_count,
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
                        Err(RingPushError::Broken(_)) => unreachable!(),
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
                    Err(RingPopError::Broken) => unreachable!(),
                }
            }
        }
        second.join().unwrap();
    }});

    #[test]
    fn multithreaded_spsc() {
        let (ring, entries_base, ring_size, log2_entry_count) = setup_ring(64);
        let ring = &ring;
        let mut sender = SpscSender {
            ring,
            entries_base,
            ring_size,
            log2_entry_count,
        };
        let mut receiver = SpscReceiver {
            ring,
            entries_base,
            ring_size,
            log2_entry_count,
        };

        simple_multithreaded_test!(sender, receiver);
    }
}
