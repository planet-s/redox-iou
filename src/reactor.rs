use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Weak};
use std::task;

use syscall::data::IoVec;
use syscall::error::{Error, Result};
use syscall::error::{E2BIG, EBADF, ECANCELED, EINVAL, EOPNOTSUPP, EOVERFLOW};
use syscall::flag::{EventFlags, MapFlags};
use syscall::io_uring::operation::Dup2Flags;
use syscall::io_uring::{CqEntry64, IoUringEnterFlags, IoUringSqeFlags, RingPopError, SqEntry64};

use crossbeam_queue::ArrayQueue;
use either::*;
use once_cell::sync::OnceCell;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};

use crate::future::{
    AtomicTag, CommandFuture, CommandFutureInner, CommandFutureRepr, FdUpdates, State, Tag,
};
use crate::instance::ConsumerInstance;

/// A reactor driven by one primary `io_uring` and zero or more secondary `io_uring`s. May or may
/// not be integrated into `Executor`
#[derive(Debug)]
pub struct Reactor {
    // the primary instance - when using secondary instances, this should be a kernel-attached
    // instance, that can monitor secondary instances (typically userspace-to-userspace rings).
    // when only a single instance is used, then this instance is free to also be a
    // userspace-to-userspace ring.
    pub(crate) main_instance: InstanceWrapper,

    // distinguishes "trusted instances" from "non-trusted" instances. the major difference between
    // these two, is that a non-trusted instance will use a map to associate integer tags with the
    // future states. meanwhile, a trusted instance will put the a Weak::into_raw pointer in the
    // user_data field, and then call Weak::from_raw to wake up the executor (which hopefully is
    // this one). this is because we most likely don't want a user process modifying out own
    // pointers!
    trusted_main_instance: bool,

    // the secondary instances, which are typically userspace-to-userspace, for schemes I/O or IPC.
    // these are not blocked on using the `SYS_ENTER_IORING` syscall; instead, they use FilesUpdate
    // on the main instance (which __must__ be attached to the kernel for secondary instances to
    // exist whatsoever), and then pops the entries of that ring separately, precisely like with
    // the primary ring.
    secondary_instances: RwLock<Vec<InstanceWrapper>>,

    // TODO: ConcurrentBTreeMap - I (4lDO2) am currently writing this.

    // a map between integer tags and internal future state. this map is only used for untrusted
    // secondary instances, and for the main instance if `trusted_main_instance` is false.
    pub(crate) tag_map: RwLock<BTreeMap<Tag, Arc<Mutex<State>>>>,

    // the next tag to use, retrieved with fetch_add(1, Ordering::Relaxed). if the value has
    // overflown, `tag_has_overflown` will be set, and further tags must be checked so that no tag
    // is accidentally replaced. this limit will probably _never_ be encountered on a 64-bit
    // system, but on a 32-bit system it might happen.
    //
    // TODO: 64-bit tags?
    next_tag: AtomicTag,

    // an atomic queue that is used for Arc reclamation of `State`s.
    reusable_tags: ArrayQueue<(Tag, Arc<Mutex<State>>)>,

    // this is lazily initialized to make things work at initialization, but one should always
    // assume that the reactor holds a weak reference to itself, to make it easier to obtain a
    // handle.
    weak_ref: OnceCell<Weak<Reactor>>,
}

#[derive(Debug)]
pub(crate) struct InstanceWrapper {
    // a convenient safe wrapper over the raw underlying interface.
    pub(crate) consumer_instance: RwLock<ConsumerInstance>,

    // stored when the ring encounters a shutdown error either when submitting an SQ, or receiving
    // a CQ.
    dropped: AtomicBool,
}

/// A builder that configures the reactor.
pub struct ReactorBuilder {
    trusted_instance: bool,
    secondary_instances: Vec<InstanceWrapper>,
    primary_instance: Option<ConsumerInstance>,
}

impl ReactorBuilder {
    /// Create an executor builder with the default options.
    pub const fn new() -> Self {
        Self {
            trusted_instance: false,
            primary_instance: None,
            secondary_instances: Vec::new(),
        }
    }
    ///
    /// Assume that the producer of the `io_uring` can be trusted, and that the `user_data` field
    /// of completion entries _always_ equals the corresponding user data of the submission for
    /// that command. This option is disabled by default, so long as the producer is not the
    /// kernel.
    ///
    /// # Safety
    /// This is unsafe because when enabled, it will optimize the executor to use the `user_data`
    /// field as a pointer to the status. A rogue producer would be able to change the user data
    /// pointer, to an arbitrary address, and cause program corruption. While the addresses can be
    /// checked at runtime, this is too expensive to check if performance is a concern (and
    /// probably even more expensive than simply storing the user_data as a tag, which is the
    /// default). When the kernel is a producer though, this will not make anything more unsafe
    /// (since the kernel has full access to the address space anyways).
    ///
    pub unsafe fn assume_trusted_instance(self) -> Self {
        Self {
            trusted_instance: true,
            ..self
        }
    }
    ///
    /// Set the primary instance that will be used by the executor.
    ///
    /// # Panics
    /// This function will panic if the primary instance has already been specified, or if this
    /// instance is one of the secondary instances.
    ///
    pub fn with_primary_instance(self, primary_instance: ConsumerInstance) -> Self {
        // TODO: ConsumerInstance Debug impl
        if self.primary_instance.is_some() {
            panic!("Cannot specify the primary instance twice!");
        }
        // TODO: Check for conflict with secondary instances
        Self {
            primary_instance: Some(primary_instance),
            ..self
        }
    }

    ///
    /// Add a secondary instance, typically a userspace-to-userspace ring.
    ///
    pub fn add_secondary_instance(mut self, secondary_instance: ConsumerInstance) -> Self {
        self.secondary_instances.push(InstanceWrapper {
            consumer_instance: RwLock::new(secondary_instance),
            dropped: AtomicBool::new(false),
        });
        self
    }

    ///
    /// Finalize the reactor, using the options that have been specified here.
    ///
    /// # Panics
    /// This function will panic if the primary instance has not been set.
    ///
    pub fn build(self) -> Arc<Reactor> {
        let primary_instance = self.primary_instance.expect("expected");
        Reactor::new(
            primary_instance,
            self.trusted_instance,
            self.secondary_instances,
        )
    }
}
impl Reactor {
    fn new(
        main_instance: ConsumerInstance,
        trusted_main_instance: bool,
        secondary_instances: Vec<InstanceWrapper>,
    ) -> Arc<Self> {
        let main_instance = InstanceWrapper {
            consumer_instance: RwLock::new(main_instance),
            dropped: AtomicBool::new(false),
        };

        let reactor_arc = Arc::new(Reactor {
            main_instance,
            trusted_main_instance,
            secondary_instances: RwLock::new(secondary_instances),

            tag_map: RwLock::new(BTreeMap::new()),
            next_tag: AtomicTag::new(1),
            reusable_tags: ArrayQueue::new(512),
            weak_ref: OnceCell::new(),
        });
        let res = reactor_arc.weak_ref.set(Arc::downgrade(&reactor_arc));
        if res.is_err() {
            unreachable!();
        }
        reactor_arc
    }
    /// Obtain a handle to this reactor, capable of creating futures that use it.
    pub fn handle(&self) -> Handle {
        Handle {
            reactor: Weak::clone(self.weak_ref.get().unwrap()),
        }
    }
    /// Add an additional secondary instance to the reactor, waking up the executor to include it
    /// if necessary.
    pub fn add_secondary_instance(&self, instance: ConsumerInstance) {
        self.secondary_instances.write().push(InstanceWrapper {
            consumer_instance: RwLock::new(instance),
            dropped: AtomicBool::new(false),
        });
    }
    pub(crate) fn drive(&self, waker: &task::Waker) {
        let a = {
            let read_guard = self.main_instance.consumer_instance.read();
            let flags = if unsafe {
                read_guard
                    .sender()
                    .as_64()
                    .expect("expected 64-bit SQEs")
                    .ring_header()
            }
            .available_entry_count_spsc()
                > 0
            {
                IoUringEnterFlags::empty()
            } else {
                IoUringEnterFlags::WAKEUP_ON_SQ_AVAIL
            };
            read_guard
                .wait(0, flags)
                .expect("redox_iou: failed to enter io_uring")
        };

        let mut write_guard = self.main_instance.consumer_instance.write();

        let ring_header = unsafe { write_guard.receiver().as_64().unwrap().ring_header() };
        let available_completions = ring_header.available_entry_count_spsc();

        if a > available_completions {
            log::warn!("The kernel/other process gave us a higher number of available completions than present on the ring.");
        }

        for i in 0..available_completions {
            let result = write_guard
                .receiver_mut()
                .as_64_mut()
                .expect("expected 64-bit CQEs")
                .try_recv();

            match result {
                Ok(cqe) => { let _ = Self::handle_cqe(self.trusted_main_instance, self.tag_map.read(), waker, cqe); }
                Err(RingPopError::Empty { .. }) => panic!("the kernel gave us a higher number of available completions than actually available (at {}/{})", i, available_completions),
                Err(RingPopError::Shutdown) => self.main_instance.dropped.store(true, std::sync::atomic::Ordering::Release),
            }
        }
    }
    fn handle_cqe(
        trusted_instance: bool,
        tags: RwLockReadGuard<'_, BTreeMap<Tag, Arc<Mutex<State>>>>,
        driving_waker: &task::Waker,
        cqe: CqEntry64,
    ) -> Option<()> {
        let cancelled = cqe.status == (-(ECANCELED as i64)) as u64;

        let state_arc;

        let state_lock = if trusted_instance {
            let pointer = usize::try_from(cqe.user_data).ok()? as *mut Mutex<State>;
            let state_weak = unsafe { Weak::from_raw(pointer) };
            assert_eq!(
                Weak::strong_count(&state_weak),
                1,
                "expected strong count to be one when receiving a direct future"
            );
            assert_eq!(
                Weak::weak_count(&state_weak),
                1,
                "expected weak count to be one when receiving a direct future"
            );
            state_arc = state_weak.upgrade()?;
            &state_arc
        } else {
            tags.get(&cqe.user_data)?
        };

        let mut state = state_lock.lock();
        match &mut *state {
            // invalid state after having received a completion
            State::Initial | State::Submitting(_, _) | State::Completed(_) | State::Cancelled => {
                return None
            }

            State::Completing(waker) => {
                // Wake other executors which have futures using this reactor.
                if !waker.will_wake(driving_waker) {
                    waker.wake_by_ref();
                }

                *state = if cancelled {
                    State::Cancelled
                } else {
                    State::Completed(cqe)
                };
            }
            State::ReceivingMulti(ref mut pending_cqes, waker) => {
                if !waker.will_wake(driving_waker) {
                    waker.wake_by_ref();
                }
                pending_cqes.push_back(cqe);
            }
        }
        Some(())
    }
}

/// A handle to the reactor, used for creating futures.
#[derive(Clone, Debug)]
pub struct Handle {
    pub(crate) reactor: Weak<Reactor>,
}

impl Handle {
    /// Retrieve the reactor that this handle is using.
    ///
    /// # Panics
    ///
    /// This method will panic if the reactor Arc has been dropped.
    pub fn reactor(&self) -> Arc<Reactor> {
        self.reactor
            .upgrade()
            .expect("couldn't retrieve reactor from Handle: reactor is dead")
    }

    ///
    /// Get a future which represents submitting a command, and then waiting for it to complete. If
    /// this executor was built with `assume_trusted_instance`, the user data field of the sqe will
    /// be overridden, so that it can store the pointer to the state.
    ///
    /// # Safety
    ///
    /// Unsafe because there is no guarantee that the buffers used by `sqe` will be used properly.
    /// If a future is dropped, and its memory is used again (possibly on the stack where it is
    /// even worse), there will be a data race between the kernel and the process.
    ///
    /// Additionally, the buffers used may point to invalid locations on the stack or heap, which
    /// is UB.
    ///
    pub unsafe fn send(&self, sqe: SqEntry64) -> CommandFuture {
        self.send_inner(sqe, false)
            .left()
            .expect("send_inner() must return CommandFuture if is_stream is set to false")
    }
    unsafe fn send_inner(
        &self,
        sqe: SqEntry64,
        is_stream: bool,
    ) -> Either<CommandFuture, FdUpdates> {
        let reactor = self
            .reactor
            .upgrade()
            .expect("failed to initiate new command: reactor is dead");

        let (tag_num_opt, state_opt) = match reactor.reusable_tags.pop() {
            // try getting a reusable tag to minimize unnecessary allocations
            Ok((n, state)) => {
                assert!(
                    matches!(&*state.lock(), &State::Initial),
                    "reusable tag was not in the reclaimed state"
                );
                assert_eq!(
                    Arc::strong_count(&state),
                    1,
                    "weird leakage of strong refs to CommandFuture state"
                );
                assert_eq!(
                    Arc::weak_count(&state),
                    0,
                    "weird leakage of weak refs to CommandFuture state"
                );

                if reactor.trusted_main_instance {
                    (None, Some(state))
                } else {
                    reactor
                        .tag_map
                        .write()
                        .insert(n, state)
                        .expect("reusable tag was already within the active tag map");

                    (Some(n), None)
                }
            }
            // if no reusable tag was present, create a new tag
            Err(crossbeam_queue::PopError) => {
                let state_arc = Arc::new(Mutex::new(State::Initial));

                let n = reactor
                    .next_tag
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                if reactor.trusted_main_instance {
                    (None, Some(state_arc))
                } else {
                    reactor.tag_map.write().insert(n, state_arc);
                    (Some(n), None)
                }
            }
        };

        let inner = CommandFutureInner {
            reactor: Weak::clone(&self.reactor),
            repr: if reactor.trusted_main_instance {
                CommandFutureRepr::Direct {
                    state: state_opt.unwrap(),
                    initial_sqe: sqe,
                }
            } else {
                CommandFutureRepr::Tagged {
                    tag: tag_num_opt.unwrap(),
                    initial_sqe: Some(sqe),
                }
            },
        };

        if is_stream {
            Right(inner.into())
        } else {
            Left(inner.into())
        }
    }

    /// Create an asynchronous stream that represents the events coming from one of more file
    /// descriptors that are triggered when e.g. the file has changed, or is capable of reading new
    /// data, etc.
    pub fn subscribe_to_fd_updates(
        &self,
        fd: usize,
        event_flags: EventFlags,
        oneshot: bool,
    ) -> FdUpdates {
        let sqe = SqEntry64::new(IoUringSqeFlags::SUBSCRIBE, 0, 0).file_update(
            fd.try_into().unwrap(),
            event_flags,
            oneshot,
        );
        unsafe {
            self.send_inner(sqe, true)
                .right()
                .expect("send_inner must return Right if is_stream is set to true")
        }
    }

    fn completion_as_rw_io_result(cqe: CqEntry64) -> Result<usize> {
        // reinterpret the status as signed, to take an errors into account.
        let signed = cqe.status as i64;

        match isize::try_from(signed) {
            Ok(s) => Error::demux(s as usize),
            Err(_) => {
                log::warn!("Failed to cast 64 bit {{,p}}{{read,write}}{{,v}} status ({:?}), into pointer sized status.", Error::demux64(signed as u64));
                if let Ok(actual_bytes_read) = Error::demux64(signed as u64) {
                    let trunc =
                        std::cmp::min(isize::max_value() as u64, actual_bytes_read) as usize;
                    log::warn!("Truncating the number of bytes/written read as it could not fit usize, from {} to {}", signed, trunc);
                    return Ok(trunc as usize);
                }
                Err(Error::new(EOVERFLOW))
            }
        }
    }
    async unsafe fn rw_io<F>(&self, fd: usize, f: F) -> Result<usize>
    where
        F: FnOnce(SqEntry64, u64) -> SqEntry64,
    {
        let fd: u64 = fd.try_into().or(Err(Error::new(EOVERFLOW)))?;

        let base_sqe = SqEntry64::new(IoUringSqeFlags::empty(), 0, (-1i64) as u64);
        let sqe = f(base_sqe, fd);

        let cqe = self.send(sqe).await?;
        Self::completion_as_rw_io_result(cqe)
    }

    /// Open a path represented by a UTF-8 byte slice, returning a new file descriptor for the file
    /// specified by that path.
    ///
    /// # Safety
    ///
    /// Refer to [`open`] for invariants that must be upheld.
    ///
    /// [`open`]: #variant.open
    pub async unsafe fn open_raw<B: AsRef<[u8]> + ?Sized>(
        &self,
        path: &B,
        flags: u64,
    ) -> Result<usize> {
        let sqe =
            SqEntry64::new(IoUringSqeFlags::empty(), 0, (-1i64) as u64).open(path.as_ref(), flags);
        let cqe = self.send(sqe).await?;
        Self::completion_as_rw_io_result(cqe)
    }
    pub async fn open_raw_static<B: AsRef<[u8]> + ?Sized + 'static>(
        &self,
        path: &'static B,
        flags: u64,
    ) -> Result<usize> {
        unsafe { self.open_raw(path, flags) }.await
    }
    pub async fn open_raw_move_buf(&self, path: Vec<u8>, flags: u64) -> Result<(usize, Vec<u8>)> {
        let fd = unsafe { self.open_raw(&*path, flags) }.await?;
        Ok((fd, path))
    }
    /// Open a path, returning a new file descriptor for the file specified by that path.
    ///
    /// # Safety
    ///
    /// For this to be safe, the path buffer that is used by the path, _must_ outlive the execution
    /// of this future, and the buffer must not be reclaimed until completion or cancellation.
    pub async unsafe fn open<S: AsRef<str> + ?Sized>(&self, path: &S, flags: u64) -> Result<usize> {
        self.open_raw(path.as_ref().as_bytes(), flags).await
    }
    pub async fn open_static<S: AsRef<str> + ?Sized + 'static>(
        &self,
        path: &'static S,
        flags: u64,
    ) -> Result<usize> {
        unsafe { self.open_raw(path.as_ref().as_bytes(), flags) }.await
    }
    pub async fn open_move_buf(&self, path: String, flags: u64) -> Result<(usize, String)> {
        let fd = unsafe { self.open_raw(path.as_str().as_bytes(), flags) }.await?;
        Ok((fd, path))
    }

    /// Close a file descriptor, optionally flushing it if necessary. This will only complete when
    /// the file descriptor has been removed from the file table, the underlying scheme has
    /// been handled the close, and optionally, pending data has been flushed to secondary storage.
    ///
    /// # Safety
    ///
    /// The file descriptor in use must not in any way reference memory that can be reclaimed by
    /// this process while the other process, kernel, or hardware, keeps using it. Generally, this
    /// is safe for things like files, pipes and scheme sockets, but not for mmapped files,
    /// O_DIRECT files, or file descriptors that use io_uring.
    ///
    /// Additionally, even though the completion entry to this syscall may be delayed, the file
    /// descriptor _must_ not be used after this command has been submitted, since the kernel is
    /// free to assign the same file descriptor for new handles, even though this may not happen
    /// immediately after submission.
    pub async unsafe fn close(&self, fd: usize, flush: bool) -> Result<()> {
        let sqe = SqEntry64::new(IoUringSqeFlags::empty(), 0, (-1i64) as u64)
            .close(fd.try_into().or(Err(Error::new(EOVERFLOW)))?, flush);
        let cqe = self.send(sqe).await?;

        Self::completion_as_rw_io_result(cqe)?;

        Ok(())
    }

    /// Close a range of file descriptors, optionally flushing them if necessary. This functions
    /// exactly like multiple invocations of the [`close`] call, with the difference of only taking
    /// up one SQE and thus being more efficient when closing many adjacent file descriptors.
    ///
    /// # Safety
    ///
    /// Refer to the invariants documented in the [`close`] call.
    ///
    /// [`close`]: #variant.close
    pub async unsafe fn close_range(
        &self,
        range: std::ops::Range<usize>,
        flush: bool,
    ) -> Result<()> {
        let start: u64 = range.start.try_into().or(Err(Error::new(EOVERFLOW)))?;
        let end: u64 = range.end.try_into().or(Err(Error::new(EOVERFLOW)))?;
        let count = end.checked_sub(start).ok_or(Error::new(EINVAL))?;

        let sqe = SqEntry64::new(IoUringSqeFlags::empty(), 0, (-1i64) as u64)
            .close_many(start, count, flush);
        let cqe = self.send(sqe).await?;

        Self::completion_as_rw_io_result(cqe)?;

        Ok(())
    }

    /// Read bytes.
    ///
    /// # Safety
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn read(&self, fd: usize, buf: &mut [u8]) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.read(fd, buf)).await
    }
    /// Read bytes, vectored.
    ///
    /// # Safety
    /// The caller must ensure that the buffers outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn readv(&self, fd: usize, bufs: &[IoVec]) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.readv(fd, bufs)).await
    }

    /// Read bytes from a specific offset. Does not change the file offset.
    ///
    /// # Safety
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn pread(&self, fd: usize, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.pread(fd, buf, offset)).await
    }

    /// Read bytes from a specific offset, vectored. Does not change the file offset.
    ///
    /// # Safety
    /// The caller must ensure that the buffers outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn preadv(&self, fd: usize, bufs: &[IoVec], offset: u64) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.preadv(fd, bufs, offset)).await
    }

    /// Write bytes.
    ///
    /// # Safety
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn write(&self, fd: usize, buf: &[u8]) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.write(fd, buf)).await
    }

    /// Write bytes, vectored.
    ///
    /// # Safety
    /// The caller must ensure that the buffers outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn writev(&self, fd: usize, bufs: &[IoVec]) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.writev(fd, bufs)).await
    }

    /// Write bytes to a specific offset. Does not change the file offset.
    ///
    /// # Safety
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn pwrite(&self, fd: usize, buf: &[u8], offset: u64) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.pwrite(fd, buf, offset)).await
    }
    /// Write bytes to a specific offset, vectored. Does not change the file offset.
    ///
    /// # Safety
    /// The caller must ensure that the buffers outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn pwritev(&self, fd: usize, bufs: &[IoVec], offset: u64) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.pwritev(fd, bufs, offset))
            .await
    }

    /// "Duplicate" a file descriptor, returning a new one based on the old one.
    ///
    /// # Panics
    /// This function will panic if the parameter is set, but the flags don't contain
    /// [`Dup2Flags::PARAM`].
    ///
    /// # Safety
    /// If the parameter is used, that mut point to a slice that is valid for the receiver.
    pub async unsafe fn dup2(
        &self,
        fd: usize,
        flags: Dup2Flags,
        param: Option<&[u8]>,
    ) -> Result<usize> {
        let fd64 = u64::try_from(fd).or(Err(Error::new(EBADF)))?;

        let cqe = self
            .send(SqEntry64::new(IoUringSqeFlags::empty(), 0, 0).dup2(fd64, flags, param))
            .await?;

        let res_fd = Error::demux64(cqe.status)?;
        let res_fd = usize::try_from(res_fd).or(Err(Error::new(EOVERFLOW)))?;

        Ok(res_fd)
    }

    /// Create a memory map from an offset+len pair inside a file descriptor, with an optional hint
    /// to where the mmap will be created. If [`MAP_FIXED`] or [`MAP_FIXED_NOREPLACE`] (which
    /// implies [`MAP_FIXED`] is set), the address hint is not taken as a hint, but rather as the
    /// actual offset to map to.
    ///
    /// # Safety
    ///
    /// This function is unsafe since it's dealing with the address space of a process, and may
    /// overwrite an existing grant, if [`MAP_FIXED`] is set and [`MAP_FIXED_NOREPLACE`] is not.
    pub async unsafe fn mmap2(
        &self,
        fd: usize,
        flags: MapFlags,
        addr_hint: Option<usize>,
        len: usize,
        offset: u64,
    ) -> Result<*const ()> {
        let fd64 = u64::try_from(fd).or(Err(Error::new(EBADF)))?;
        let len64 = u64::try_from(len).or(Err(Error::new(E2BIG)))?;

        if flags.contains(MapFlags::MAP_FIXED) && addr_hint.is_none() {
            panic!("An mmap2 with MAP_FIXED requires the addr hint to be specified, but here it was None.");
        }
        let addr_hint = addr_hint.unwrap_or(0);
        let addr_hint64 = u64::try_from(addr_hint).or(Err(Error::new(EOPNOTSUPP)))?;

        let cqe = self
            .send(SqEntry64::new(IoUringSqeFlags::empty(), 0, 0).mmap(
                fd64,
                flags,
                addr_hint64,
                len64,
                offset,
            ))
            .await?;

        let pointer = Error::demux64(cqe.status)?;
        Ok(pointer as *const ())
    }

    /// Create a memory map from an offset+len pair inside a file descriptor, in a similar way
    /// compared to [`mmap2`]. The only distinction between [`mmap2`] and this call, is that there
    /// is cannot be any hint information to where the memory map will exist, and instead the
    /// kernel will arbitrarily choose the any address it finds useful.
    ///
    /// # Safety
    ///
    /// While there is no obvious invariant that comes with this call, unlike [`mmap2`] when
    /// discarding existing mappings as part for [`MAP_FIXED`], the only reason this call is marked
    /// as unsafe, is simply because it deals with memory, and may have side effects. If the mmap
    /// is shared with another process, that could also lead to data races, however returning a
    /// pointer forwards this invariant to the caller.
    ///
    /// [`mmap2`]: #variant.mmap2
    pub async unsafe fn mmap(
        &self,
        fd: usize,
        flags: MapFlags,
        len: usize,
        offset: u64,
    ) -> Result<*const ()> {
        assert!(!flags.contains(MapFlags::MAP_FIXED));
        self.mmap2(fd, flags, None, len, offset).await
    }
}
