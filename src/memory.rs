use std::borrow::{Borrow, BorrowMut};
use std::convert::{TryFrom, TryInto};
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::Arc;
use std::{fmt, mem, ops, slice};

use syscall::data::Map as Mmap;
use syscall::error::{Error, Result};
use syscall::error::{EADDRINUSE, ENOMEM, EOVERFLOW};
use syscall::flag::MapFlags;
use syscall::io_uring::v1::operation::DupFlags;
use syscall::io_uring::v1::{PoolFdEntry, Priority};

use parking_lot::Mutex;

pub use redox_buffer_pool as pool;

use crate::future::{CommandFuture, CommandFutureRepr, State as CommandFutureState};
use crate::reactor::{Handle, SecondaryRingId, SubmissionContext, SubmissionSync};

/// A buffer pool, with the default options for use by userspace-to-userspace rings.
pub type BufferPool<I = u32, H = BufferPoolHandle, E = ()> = pool::BufferPool<I, H, E>;
/// A slice of the [`BufferPool'] type.
pub type BufferSlice<
    'pool,
    I = u32,
    E = BufferPoolHandle,
    G = CommandFutureGuard,
    H = BufferPoolHandle,
> = pool::BufferSlice<'pool, I, H, E, G>;

/// The handle type managing the raw allocations in use by the buffer pool. This particular one
/// uses io_uring mmaps.
#[derive(Debug)]
pub struct BufferPoolHandle {
    fd: usize,
    reactor: Option<Handle>,
}
impl pool::Handle for BufferPoolHandle {
    fn close(self) -> Result<(), pool::CloseError<()>> {
        let _ = syscall::close(self.fd);
        Ok(())
    }
}
impl BufferPoolHandle {
    /// Destroy every mmap allocation that has been used by the buffer pool. This is safe because
    /// the handle can only be moved out when all guarded slices have been released.
    // TODO: Make sure this really is the case.
    pub async fn destroy_all(self) -> Result<()> {
        match self.reactor {
            Some(ref h) => unsafe {
                // Closing will automagically unmap all mmaps.
                h.close(
                    h.reactor().primary_instance(),
                    SubmissionContext::new().with_sync(SubmissionSync::Drain),
                    self.fd,
                    false,
                )
                .await?;
            },
            None => {
                syscall::close(self.fd)?;
            }
        }
        Ok(())
    }
}

impl CommandFuture {
    /// Protect a slice with a future guard, preventing the memory from being reclaimed until the
    /// future has completed. This will cause the buffer slice to leak memory if dropped too early,
    /// but prevents undefined behavior.
    pub fn guard<G>(&self, slice: &mut G)
    where
        G: Guardable<CommandFutureGuard>,
    {
        let arc = match self.inner.repr {
            CommandFutureRepr::Direct { ref state, .. } => Arc::clone(state),
            CommandFutureRepr::Tagged { tag, .. } => {
                if let Some(reactor) = self.inner.reactor.upgrade() {
                    if let Some(state) = reactor.tag_map.read().get(&tag) {
                        Arc::clone(state)
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
        };
        let guard = CommandFutureGuard { inner: arc };
        slice
            .try_guard(guard)
            .expect("cannot guard using future: another guard already present");
    }
}
impl Handle {
    async fn create_buffer_pool_inner<I: pool::Integer, E: Copy>(
        &self,
        secondary_instance: SecondaryRingId,
        producer: bool,
    ) -> Result<pool::BufferPool<I, BufferPoolHandle, E>> {
        let reactor = self
            .reactor
            .upgrade()
            .expect("can't create_buffer_pool: reactor is dead");

        assert_eq!(reactor.id(), secondary_instance.reactor);

        let ringfd = {
            let secondary_instances = reactor.secondary_instances.read();
            let instance = secondary_instances
                .instances
                .get(secondary_instance.inner.get() - 1)
                .expect("invalid secondary ring id");

            if producer {
                instance
                    .as_producer_instance()
                    .expect(
                        "ring id represents consumer instance, but expected a producer instance",
                    )
                    .producer_instance
                    .read()
                    .ringfd()
            } else {
                instance
                    .as_consumer_instance()
                    .expect(
                        "ring id represents producer instance, but expected a consumer instance",
                    )
                    .consumer_instance
                    .read()
                    .ringfd()
            }
        };

        log::debug!("Running dup");
        let fd = unsafe {
            self.dup(
                reactor.primary_instance(),
                SubmissionContext::default(),
                ringfd,
                DupFlags::PARAM,
                Some(b"pool"),
            )
        }
        .await?;
        log::debug!("Ran dup");

        Ok(pool::BufferPool::new(Some(BufferPoolHandle {
            reactor: Some(Handle::clone(self)),
            fd,
        })))
    }
    /// Create a new buffer pool meant for use by producers. This will ask the kernel for the
    /// offsets the consumer has already preallocated.
    pub async fn create_producer_buffer_pool<I: pool::Integer + TryFrom<u64> + TryInto<usize>>(
        &self,
        secondary_instance: SecondaryRingId,
        _creation_command_priority: Priority,
    ) -> Result<pool::BufferPool<I, BufferPoolHandle, ()>> {
        let pool = self
            .create_buffer_pool_inner(secondary_instance, true)
            .await?;
        import(pool.handle().unwrap(), &pool).await?;
        Ok(pool)
    }
    /// Create a buffer pool meant for consumers. The buffer pool will be semi-managed by the
    /// kernel; the kernel will keep track of all mmap ranges that have been allocated, and allow
    /// the consumer to check for new offsets, so that the pool can be expanded correctly.
    pub async fn create_buffer_pool<E: Copy, I: pool::Integer + TryInto<usize> + TryInto<u64>>(
        &self,
        secondary_instance: SecondaryRingId,
        _creation_command_priority: Priority,
        initial_len: I,
        initial_extra: E,
    ) -> Result<pool::BufferPool<I, BufferPoolHandle, E>> {
        let pool = self
            .create_buffer_pool_inner(secondary_instance, false)
            .await?;

        let expansion = pool.begin_expand(initial_len)?;
        let len_usize: usize = expansion.len().try_into().or(Err(Error::new(EOVERFLOW)))?;
        let offset_u64: u64 = expansion
            .offset()
            .try_into()
            .or(Err(Error::new(EOVERFLOW)))?;

        let pointer = expand(&pool.handle().unwrap(), offset_u64, len_usize).await?;
        unsafe {
            expansion.initialize(NonNull::new(pointer).unwrap(), initial_extra);
        }
        Ok(pool)
    }
}
/// Expand the buffer pool, creating a new mmap.
pub async fn expand(handle: &BufferPoolHandle, offset: u64, len: usize) -> Result<*mut u8> {
    let map_flags = MapFlags::MAP_SHARED | MapFlags::PROT_READ | MapFlags::PROT_WRITE;

    match handle.reactor {
        Some(ref h) => unsafe {
            h.mmap(
                h.reactor().primary_instance(),
                SubmissionContext::default(),
                handle.fd,
                map_flags,
                len,
                offset,
            )
            .await
            .map(|addr| addr as *mut u8)
        },
        None => unsafe { expand_blocking(handle.fd, offset, len, map_flags) },
    }
}
/// Import various new ranges that the consumer has opened, into this buffer pool.
pub async fn import<I: pool::Integer + TryFrom<u64> + TryInto<usize>>(
    handle: &BufferPoolHandle,
    pool: &pool::BufferPool<I, BufferPoolHandle, ()>,
) -> Result<I> {
    let mut range_list = [PoolFdEntry::default(); 4];

    let mut additional_bytes = I::zero();

    loop {
        let slice = &mut range_list[..];
        let byte_slice = unsafe {
            slice::from_raw_parts_mut(
                slice.as_mut_ptr() as *mut u8,
                slice.len() * mem::size_of::<PoolFdEntry>(),
            )
        };

        #[allow(clippy::all)]
        let bytes_read = match handle.reactor {
            // TODO

            /*Some(ref h) => unsafe {
                h.read(
                    h.reactor().primary_instance(),
                    handle.fd,
                    byte_slice,
                ).await?
            },*/
            _ => syscall::read(handle.fd, byte_slice)?,
        };
        if bytes_read % mem::size_of::<PoolFdEntry>() != 0 {
            log::warn!("Somehow the io_uring poolfd read a byte count not divisible by the size of PoolFd. Ignoring extra bytes.");
        }
        let structs_read = bytes_read / mem::size_of::<PoolFdEntry>();
        let structs = &range_list[..structs_read];

        if structs.is_empty() {
            break;
        }

        // TODO: Use some kind of join!, maybe.
        for entry in structs {
            let offset = entry.offset;
            let len_usize = usize::try_from(entry.size).or(Err(Error::new(EOVERFLOW)))?;
            let len = I::try_from(entry.size).or(Err(Error::new(EOVERFLOW)))?;

            match pool.begin_expand(len) {
                Ok(expansion_handle) => {
                    let pointer = NonNull::new(expand(handle, offset, len_usize).await?)
                        .expect("expand yielded a null pointer");
                    log::debug!("importing at {} len {} => {:p}", offset, len, pointer);
                    additional_bytes += len;
                    unsafe { expansion_handle.initialize(pointer, ()) }
                }
                Err(pool::BeginExpandError) => {
                    if additional_bytes == I::zero() {
                        return Err(Error::new(ENOMEM));
                    } else {
                        break;
                    }
                }
            }
        }
    }
    Ok(additional_bytes)
}

unsafe fn expand_blocking<I: TryInto<usize>>(
    fd: usize,
    offset: I,
    len: usize,
    map_flags: MapFlags,
) -> Result<*mut u8> {
    syscall::fmap(
        fd,
        &Mmap {
            offset: offset.try_into().or(Err(Error::new(EOVERFLOW)))?,
            size: len,
            flags: map_flags,
        },
    )
    .map(|addr| addr as *mut u8)
}

/// A guard type that protects a buffer until a future has been canceled (and the cancel has been
/// acknowledged by the producer), or finished.
#[derive(Debug)]
pub struct CommandFutureGuard {
    inner: Arc<Mutex<CommandFutureState>>,
}
impl pool::Guard for CommandFutureGuard {
    fn try_release(&self) -> bool {
        // Only allow reclaiming buffer slices when their guarded future has actually
        // completed.
        matches!(&*self.inner.lock(), CommandFutureState::Cancelled | CommandFutureState::Completed(_))
    }
}

/// A wrapper for types that can be "guarded", meaning that the memory they reference cannot be
/// safely reclaimed or even moved out, until the guard frees it.
pub struct Guarded<G: pool::Guard, T: 'static> {
    inner: MaybeUninit<T>,
    guard: Option<G>,
}
impl<G: pool::Guard, T: 'static> Guarded<G, T> {
    /// Create a new guard, preventing the inner value from dropping or being moved out safely,
    /// until the guard releases itself.
    pub fn new(inner: T) -> Self {
        Self {
            inner: MaybeUninit::new(inner),
            guard: None,
        }
    }
    /// Apply a guard to the wrapper, which will make it impossible for the inner value to be moved
    /// out (unless using unsafe of course). The memory will be leaked completely if the destructor
    /// is called when the guard cannot release itself.
    pub fn guard(&mut self, guard: G) {
        assert!(self.guard.is_none());
        self.guard = Some(guard);
    }
    /// Query whether this wrapper possesses an active guard.
    pub fn has_guard(&self) -> bool {
        self.guard.is_some()
    }
    /// Remove the guard, bypassing any safety guarantees provided by this wrapper.
    ///
    /// # Safety
    ///
    /// Since this removes the guard, this will allow the memory to be reclaimed when some other
    /// entity could be using it simultaneously. For this not to lead to UB, the caller must not
    /// reclaim the memory owned here unless it can absolutely be sure that the guard is no longer
    /// needed.
    pub unsafe fn force_unguard(&mut self) -> Option<G> {
        self.guard.take()
    }

    /// Try to remove the guard together with the inner value, returning the wrapper if the guard
    /// was not able to be safely released.
    pub fn try_into_inner(mut self) -> Result<(T, Option<G>), Self> {
        match self.try_unguard() {
            Ok(guard_opt) => Ok({
                let inner = unsafe { self.uninitialize_inner() };
                mem::forget(self);
                (inner, guard_opt)
            }),
            Err(_) => Err(self),
        }
    }
    /// Move out the inner of this wrapper, together with the guard if there was one. The guard
    /// will not be released, and is instead up to the caller.
    ///
    /// # Safety
    ///
    /// This is unsafe for the same reasons as with [`force_unguard`]; as the guard is left
    /// untouched, it's completely up to the caller to ensure that the invariants required by the
    /// guard be upheld.
    ///
    /// [`force_unguard`]: #method.force_unguard
    pub unsafe fn force_into_inner(mut self) -> (T, Option<G>) {
        let guard = self.guard.take();
        let inner = self.uninitialize_inner();
        mem::forget(self);
        (inner, guard)
    }

    /// Try removing the guard in-place, failing with `EADDRINUSE` if that weren't possible.
    pub fn try_unguard(&mut self) -> Result<Option<G>> {
        let guard = match self.guard.as_ref() {
            Some(g) => g,
            None => return Ok(None),
        };

        if guard.try_release() {
            Ok(unsafe { self.force_unguard() })
        } else {
            Err(Error::new(EADDRINUSE))
        }
    }
    unsafe fn uninitialize_inner(&mut self) -> T {
        mem::replace(&mut self.inner, MaybeUninit::uninit()).assume_init()
    }
}
impl<G: pool::Guard, T: 'static + fmt::Debug> fmt::Debug for Guarded<G, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        struct GuardDbg<'a, H>(Option<&'a H>);

        impl<'a, H> fmt::Debug for GuardDbg<'a, H> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self.0 {
                    Some(_) => write!(
                        f,
                        "[guarded using guard type {}]",
                        std::any::type_name::<H>()
                    ),
                    None => write!(f, "[no guard]"),
                }
            }
        }

        f.debug_struct("Guarded")
            .field("value", &*self)
            .field("guard", &GuardDbg(self.guard.as_ref()))
            .finish()
    }
}
impl<G: pool::Guard, T: 'static> Drop for Guarded<G, T> {
    fn drop(&mut self) {
        if self.try_unguard().is_ok() {
            // Drop the inner value if the guard was able to be removed.
            drop(unsafe { self.uninitialize_inner() })
        } else {
            // No nothing and leak the value otherwise.
        }
    }
}
impl<G: pool::Guard, T: 'static> ops::Deref for Guarded<G, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.inner.get_ref() }
    }
}
impl<G: pool::Guard, T: 'static> ops::DerefMut for Guarded<G, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.inner.get_mut() }
    }
}
impl<G: pool::Guard, T: 'static> Borrow<T> for Guarded<G, T> {
    fn borrow(&self) -> &T {
        &*self
    }
}
impl<G: pool::Guard, T: 'static> BorrowMut<T> for Guarded<G, T> {
    fn borrow_mut(&mut self) -> &mut T {
        &mut *self
    }
}
impl<G: pool::Guard, T: 'static> AsRef<T> for Guarded<G, T> {
    fn as_ref(&self) -> &T {
        &*self
    }
}
impl<G: pool::Guard, T: 'static> AsMut<T> for Guarded<G, T> {
    fn as_mut(&mut self) -> &mut T {
        &mut *self
    }
}
impl<G: pool::Guard, T: 'static> From<T> for Guarded<G, T> {
    fn from(inner: T) -> Self {
        Self::new(inner)
    }
}

/// A trait for types that can be "guardable", meaning that they won't do anything on Drop unless
/// they can remove their guard.
///
/// # Safety
///
/// This trait is unsafe to implement, due to the following invariants that must be upheld:
///
/// * When invoking [`try_guard`], any pointers to self must not be invalidated, which wouldn't be the
///   case for e.g. a Vec that inserted a new item when guarding. Additionally, the function must not
///   in any way access the inner data that is being guarded, since the futures will have references
///   before even sending the guard, to that data.
/// * When dropping, the data _must not_ be reclaimed, until the guard that this type has received,
///   is successfully released.
/// * The inner data must implement `std::pin::Unpin` or follow equivalent rules; if this guardable is
///   leaked, the data must no longer be accessible (this rules out data on the stack).
///
/// [`try_guard`]: #method.try_guard
pub unsafe trait Guardable<G> {
    /// Attempt to insert a guard into the guardable, if there wasn't already a guard inserted. If
    /// that were the case, error with the guard that wasn't able to be inserted.
    fn try_guard(&mut self, guard: G) -> Result<(), G>;
}
unsafe impl<'pool, I, E, G, H> Guardable<G> for BufferSlice<'pool, I, E, G, H>
where
    I: pool::Integer,
    H: pool::Handle,
    E: Copy,
    G: pool::Guard,
{
    fn try_guard(&mut self, guard: G) -> Result<(), G> {
        match self.guard(guard) {
            Ok(()) => Ok(()),
            Err(pool::WithGuardError { this }) => Err(this),
        }
    }
}

unsafe impl<G, T> Guardable<G> for Guarded<G, T>
where
    G: pool::Guard,
{
    fn try_guard(&mut self, guard: G) -> Result<(), G> {
        if self.has_guard() {
            return Err(guard);
        }
        Self::guard(self, guard);
        Ok(())
    }
}
// The following implementations exist because a static reference which pointee also is static,
// cannot be dropped at all.
unsafe impl<G, T> Guardable<G> for &'static T
where
    T: 'static,
{
    fn try_guard(&mut self, _guard: G) -> Result<(), G> {
        Ok(())
    }
}
unsafe impl<G, T> Guardable<G> for &'static mut T
where
    T: 'static,
{
    fn try_guard(&mut self, _guard: G) -> Result<(), G> {
        Ok(())
    }
}
unsafe impl<G> Guardable<G> for () {
    fn try_guard(&mut self, _guard: G) -> Result<(), G> {
        Ok(())
    }
}
unsafe impl<G, T> Guardable<G> for [T; 0] {
    fn try_guard(&mut self, _guard: G) -> Result<(), G> {
        Ok(())
    }
}
