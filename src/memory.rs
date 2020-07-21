use std::convert::{TryFrom, TryInto};
use std::sync::{Arc, Weak};

use syscall::data::Map as Mmap;
use syscall::error::{Error, Result};
use syscall::error::EOVERFLOW;
use syscall::flag::MapFlags;
use syscall::io_uring::operation::DupFlags;

use parking_lot::Mutex;

use redox_buffer_pool as pool;

use crate::future::{CommandFuture, CommandFutureRepr, State as CommandFutureState};
use crate::reactor::{Handle, SecondaryRingId};

pub type BufferPool<H = BufferPoolHandle> = pool::BufferPool<H>;
pub type BufferSlice<'a, G = CommandFutureGuard, H = BufferPoolHandle> = pool::BufferSlice<'a, H, G>;

pub struct BufferPoolHandle {
    fd: usize,
    reactor: Option<Handle>,
}
impl pool::Handle for BufferPoolHandle {
    fn close(&mut self) -> Result<()> {
        let _ = syscall::close(self.fd)?;
        Ok(())
    }
}
impl BufferPoolHandle {
    pub async fn destroy_all(self) -> Result<()> {
        match self.reactor {
            Some(ref h) => unsafe {
                // Closing will automagically unmap all mmaps.
                h.close(h.reactor().primary_instance(), self.fd, false)
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
    pub fn guard<'a>(&self, slice: &mut pool::BufferSlice<'a, BufferPoolHandle, CommandFutureGuard>) {
        let weak = match self.inner.repr {
            CommandFutureRepr::Direct { ref state, .. } => Arc::downgrade(state),
            CommandFutureRepr::Tagged { tag, .. } => {
                if let Some(reactor) = self.inner.reactor.upgrade() {
                    if let Some(state) = reactor.tag_map.read().get(&tag) {
                        Arc::downgrade(state)
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
        };
        let guard = CommandFutureGuard {
            inner: weak,
        };
        slice.guard(guard).expect("cannot guard using future: another guard already present");
    }
}
impl Handle {
    pub async fn create_buffer_pool(
        &self,
        secondary_instance: SecondaryRingId,
        _creation_command_priority: u16,
        initial_len: u32,
    ) -> Result<pool::BufferPool<BufferPoolHandle>> {
        let reactor = self
            .reactor
            .upgrade()
            .expect("can't create_buffer_pool: reactor is dead");

        assert_eq!(reactor.id(), secondary_instance.reactor);

        let ringfd = reactor
            .secondary_instances
            .read()
            .instances
            .get(secondary_instance.inner.get() - 1)
            .expect("invalid secondary ring id")
            .as_consumer_instance()
            .expect("ring id represents producer instance")
            .consumer_instance
            .read()
            .ringfd();

        log::debug!("Running dup");
        let fd = unsafe {
            self.dup(
                reactor.primary_instance(),
                ringfd,
                DupFlags::PARAM,
                Some(b"pool"),
            )
        }
        .await?;
        log::debug!("Ran dup");

        let pool = pool::BufferPool::new(Some(BufferPoolHandle {
            reactor: Some(Handle::clone(self)),
            fd,
        }));

        let expansion = pool.begin_expand(initial_len)?;
        let pointer = expand(&pool.handle().unwrap(), expansion.offset(), expansion.len()).await?;
        unsafe { expansion.initialize(pointer)?; }
        Ok(pool)
    }
}
async fn expand(handle: &BufferPoolHandle, offset: u32, len: u32) -> Result<*mut u8> {
    let map_flags = MapFlags::MAP_SHARED | MapFlags::PROT_READ | MapFlags::PROT_WRITE;
    let len = usize::try_from(len).or(Err(Error::new(EOVERFLOW)))?;

    match handle.reactor {
        Some(ref h) => unsafe {
            h.mmap(
                h.reactor().primary_instance(),
                handle.fd,
                map_flags,
                len,
                u64::from(offset),
            )
            .await
            .map(|addr| addr as *mut u8)
        },
        None => unsafe {
            expand_blocking(handle.fd, offset, len, map_flags)
        },
    }
}
unsafe fn expand_blocking(fd: usize, offset: u32, len: usize, map_flags: MapFlags) -> Result<*mut u8> {
    syscall::fmap(
        fd,
        &Mmap {
            offset: offset.try_into().expect("why are you using a 16-bit CPU?"),
            size: len,
            flags: map_flags,
        },
    ).map(|addr| addr as *mut u8)
}

#[derive(Debug)]
pub struct CommandFutureGuard {
    inner: Weak<Mutex<CommandFutureState>>,
}
impl pool::Guard for CommandFutureGuard {
    fn try_release(&self) -> bool {
        if let Some(arc) = self.inner.upgrade() {
            // Only allow reclaiming buffer slices when their guarded future has actually
            // completed.
            matches!(&*arc.lock(), CommandFutureState::Cancelled | CommandFutureState::Completed(_))
        } else {
            // Future is not used anymore, we can safely remove the guard now.
            true
        }
    }
}

