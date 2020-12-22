use std::convert::{TryFrom, TryInto};
use std::ptr::NonNull;
use std::sync::Arc;
use std::{mem, slice};

use syscall::data::Map as Mmap;
use syscall::error::{Error, Result};
use syscall::error::{EFAULT, ENOMEM, EOVERFLOW};
use syscall::flag::MapFlags;
use syscall::io_uring::v1::operation::{CloseFlags, DupFlags};
use syscall::io_uring::v1::{PoolFdEntry, Priority};

use parking_lot::Mutex;

pub use guard_trait::{Guarded, GuardedMut};

pub use redox_buffer_pool as pool;

use crate::future::{
    CommandFuture, CommandFutureRepr, State as CommandFutureState,
    StateInner as CommandFutureStateInner,
};
use crate::reactor::{Handle, SubmissionContext, SubmissionSync};

#[cfg(any(doc, target_os = "redox"))]
use crate::reactor::{ProducerRingId, SecondaryRingId};

/// A buffer pool, with the default options for use by userspace-to-userspace rings.
pub type BufferPool<I = u32, H = BufferPoolHandle, E = ()> = pool::BufferPool<I, H, E>;
/// A slice of the [`BufferPool'] type.
pub type BufferSlice<
    'pool,
    I = u32,
    E = BufferPoolHandle,
    G = pool::marker::Guard,
    H = BufferPoolHandle,
    C = BufferPool<I, H, E>,
> = pool::BufferSlice<'pool, I, H, E, G, C>;

/// The handle type managing the raw allocations in use by the buffer pool. This particular one
/// uses io_uring mmaps.
#[derive(Debug)]
pub struct BufferPoolHandle {
    fd: usize,
    reactor: Option<Handle>,
}
impl<I, E> pool::Handle<I, E> for BufferPoolHandle
where
    I: pool::Integer + TryInto<usize>,
    E: Copy,
{
    type Error = Error;

    fn close_all(self, _mmap_entries: pool::MmapEntries<I, E>) -> Result<(), Self::Error> {
        let _ = syscall::close(self.fd)?;
        Ok(())
    }
    fn close(&mut self, mmap_entries: pool::MmapEntries<I, E>) -> Result<(), Self::Error> {
        for entry in mmap_entries {
            unsafe {
                syscall::funmap(
                    entry.pointer.as_ptr() as usize,
                    entry.size.try_into().or(Err(Error::new(EFAULT)))?,
                )?
            };
        }
        Ok(())
    }
}
impl BufferPoolHandle {
    /// Destroy every mmap allocation that has been used by the buffer pool. This is safe because
    /// the handle can only be moved out when all guarded slices have been released.
    // TODO: Make sure this really is the case.
    #[cfg(target_os = "redox")]
    pub async fn destroy_all(self) -> Result<()> {
        match self.reactor {
            Some(ref h) => unsafe {
                // Closing will automagically unmap all mmaps.
                h.close(
                    // TODO
                    h.reactor().primary_instances().next().unwrap(),
                    SubmissionContext::new().with_sync(SubmissionSync::Drain),
                    self.fd,
                    CloseFlags::empty(),
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

impl Handle {
    #[cfg(target_os = "redox")]
    async fn create_buffer_pool_inner<I, E>(
        &self,
        inner_idx: usize,
        inner_reactor_id: usize,
        producer: bool,
    ) -> Result<pool::BufferPool<I, BufferPoolHandle, E>>
    where
        I: pool::Integer + TryInto<usize>,
        E: Copy,
    {
        let reactor = self
            .reactor
            .upgrade()
            .expect("can't create_buffer_pool: reactor is dead");

        assert_eq!(reactor.id().inner, inner_reactor_id);

        let ringfd = {
            let secondary_instances = reactor.secondary_instances.read();
            let instance = secondary_instances
                .instances
                .get(inner_idx)
                .expect("invalid secondary ring id");

            if producer {
                instance
                    .as_producer_instance()
                    .expect(
                        "ring id represents consumer instance, but expected a producer instance",
                    )
                    .producer_instance
                    .ringfd()
            } else {
                instance
                    .as_consumer_instance()
                    .expect(
                        "ring id represents producer instance, but expected a consumer instance",
                    )
                    .consumer_instance
                    .ringfd()
            }
        };

        log::debug!("Running dup");
        let (fd, _) = self
            .dup(
                // TODO
                reactor.primary_instances().next().unwrap(),
                SubmissionContext::default(),
                ringfd,
                DupFlags::PARAM,
                Some(&b"pool"[..]),
            )
            .await?;
        log::debug!("Ran dup");

        Ok(pool::BufferPool::new(Some(BufferPoolHandle {
            reactor: Some(Handle::clone(self)),
            fd,
        })))
    }
    /// Create a new buffer pool meant for use by producers. This will ask the kernel for the
    /// offsets the consumer has already preallocated.
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub async fn create_producer_buffer_pool<I: pool::Integer + TryFrom<u64> + TryInto<usize>>(
        &self,
        // TODO: Support passing the primary reactor to do the operation with.
        secondary_instance: ProducerRingId,
        // TODO: SubmissionContext
        _creation_command_priority: Priority,
    ) -> Result<pool::BufferPool<I, BufferPoolHandle, ()>> {
        let pool = self
            .create_buffer_pool_inner(
                secondary_instance.inner,
                secondary_instance.reactor.inner,
                true,
            )
            .await?;
        import(pool.handle().unwrap(), &pool).await?;
        Ok(pool)
    }
    /// Create a buffer pool meant for consumers. The buffer pool will be semi-managed by the
    /// kernel; the kernel will keep track of all mmap ranges that have been allocated, and allow
    /// the consumer to check for new offsets, so that the pool can be expanded correctly.
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub async fn create_buffer_pool<E: Copy, I: pool::Integer + TryInto<usize> + TryInto<u64>>(
        &self,
        secondary_instance: SecondaryRingId,
        _creation_command_priority: Priority,
        initial_len: I,
        initial_extra: E,
    ) -> Result<pool::BufferPool<I, BufferPoolHandle, E>> {
        let pool = self
            .create_buffer_pool_inner(
                secondary_instance.inner,
                secondary_instance.reactor.inner,
                false,
            )
            .await?;

        let expansion = pool.begin_expand(initial_len).or(Err(Error::new(ENOMEM)))?;
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
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
pub async fn expand(handle: &BufferPoolHandle, offset: u64, len: usize) -> Result<*mut u8> {
    let map_flags = MapFlags::MAP_SHARED | MapFlags::PROT_READ | MapFlags::PROT_WRITE;

    match handle.reactor {
        Some(ref h) => unsafe {
            h.mmap(
                // TODO
                h.reactor().primary_instances().next().unwrap(),
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
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
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

// TODO: This should also exist on Linux.
#[cfg(target_os = "redox")]
unsafe fn expand_blocking<I: TryInto<usize>>(
    fd: usize,
    offset: I,
    len: usize,
    map_flags: MapFlags,
) -> Result<*mut u8> {
    #[cfg(target_os = "redox")]
    {
        syscall::fmap(
            fd,
            &Mmap {
                address: 0, // unused
                offset: offset.try_into().or(Err(Error::new(EOVERFLOW)))?,
                size: len,
                flags: map_flags,
            },
        )
        .map(|addr| addr as *mut u8)
    }
}
