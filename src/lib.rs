use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Weak};
use std::{task, thread};

pub use syscall::io_uring::*;
use syscall::{Error, Event, IoVec, Result, ECANCELED, EFAULT, EINVAL, EOVERFLOW, ESHUTDOWN};

use crossbeam_queue::ArrayQueue;
use either::Either;
pub use futures::io::AsyncBufRead;
use futures::Stream;
use once_cell::sync::OnceCell;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};

/// A minimal executor, that does not use any thread pool.
pub struct Executor {
    // a waker type that is more efficient to use if this executor drives the reactor itself (which
    // isn't the case when integrating with e.g. async-std or tokio and using their executors).
    driving_waker: task::Waker,

    // a regular waker that will wait for the reactor or whatever completes the future, to wake the
    // executor up.
    standard_waker: task::Waker,
    standard_waker_thread: Arc<RwLock<thread::Thread>>,

    // the reactor that this executor is driving, or alternatively None if the driver is running in
    // another thread. the latter is less performant.
    reactor: Option<Arc<Reactor>>,
}

struct Reactor {
    instance: InstanceWrapper,
    trusted_instance: bool,

    // TODO: ConcurrentBTreeMap
    tag_map: RwLock<BTreeMap<usize, Arc<Mutex<State>>>>,

    next_tag: AtomicUsize,
    reusable_tags: ArrayQueue<(usize, Arc<Mutex<State>>)>,

    // this is Option to make things work at initialization, but one should always assume that the
    // reactor holds a weak reference to itself, to make it easier to obtain a handle.
    weak_ref: OnceCell<Weak<Reactor>>,
}

struct InstanceWrapper {
    consumer_instance: RwLock<v1::ConsumerInstance>,
    dropped: AtomicBool,
    // store more of the arcs here instead
}

/// A builder that creates an `Executor`.
pub struct ExecutorBuilder {
    trusted_instance: bool,
}

impl ExecutorBuilder {
    /// Create an executor builder with the default options.
    pub fn new() -> Self {
        Self {
            trusted_instance: false,
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
    /// field as a pointer to the status. A rouge producer would be able to change the user data
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

    pub fn build(self, instance: v1::ConsumerInstance) -> Executor {
        Executor::new(instance, self.trusted_instance)
    }
}

impl Executor {
    fn new(instance: v1::ConsumerInstance, trusted_instance: bool) -> Self {
        let instance = InstanceWrapper {
            consumer_instance: RwLock::new(instance),
            dropped: AtomicBool::new(false),
        };

        let reactor_arc = Arc::new(Reactor {
            instance,
            trusted_instance,

            tag_map: RwLock::new(BTreeMap::new()),
            next_tag: AtomicUsize::new(1),
            reusable_tags: ArrayQueue::new(512),
            weak_ref: OnceCell::new(),
        });
        let res = reactor_arc.weak_ref.set(Arc::downgrade(&reactor_arc));
        if res.is_err() {
            unreachable!();
        }

        let standard_waker_thread = Arc::new(RwLock::new(thread::current()));

        Self {
            driving_waker: Self::driving_waker(&reactor_arc),
            standard_waker: Self::standard_waker(&standard_waker_thread),
            standard_waker_thread,
            reactor: Some(reactor_arc),
        }
    }
    fn driving_waker(reactor: &Arc<Reactor>) -> task::Waker {
        let reactor = Arc::downgrade(reactor);

        async_task::waker_fn(move || {
            let reactor = reactor
                .upgrade()
                .expect("failed to wake up executor: integrated reactor dead");

            let instance_guard = reactor.instance.consumer_instance.read();

            match instance_guard.sender() {
                &ConsumerGenericSender::Bits32(ref sender32) => sender32.notify(),
                &ConsumerGenericSender::Bits64(ref sender64) => sender64.notify(),
            }
        })
    }
    fn standard_waker(standard_waker_thread: &Arc<RwLock<thread::Thread>>) -> task::Waker {
        let standard_waker_thread = Arc::downgrade(standard_waker_thread);

        async_task::waker_fn(move || {
            let thread_lock = standard_waker_thread
                .upgrade()
                .expect("failed to wake up executor: executor dead");
            let thread = thread_lock.read();
            thread.unpark();
        })
    }

    /// Run a future until completion.
    pub fn run<O, F>(&self, future: F) -> O
    where
        F: Future<Output = O>,
    {
        let mut future = future;

        let waker = if self.reactor.is_some() {
            self.driving_waker.clone()
        } else {
            *self.standard_waker_thread.write() = thread::current();
            self.standard_waker.clone()
        };

        let mut cx = task::Context::from_waker(&waker);

        loop {
            let pinned_future = unsafe { Pin::new_unchecked(&mut future) };
            match pinned_future.poll(&mut cx) {
                task::Poll::Ready(o) => return o,

                task::Poll::Pending => {
                    if let Some(reactor) = self.reactor.as_ref() {
                        reactor.drive(&waker);
                    } else {
                        thread::park();
                    }
                }
            }
        }
    }

    pub fn reactor_handle(&self) -> Option<Handle> {
        self.reactor.as_ref().map(|reactor_arc| Handle {
            reactor: Arc::downgrade(&reactor_arc),
        })
    }
}
impl Reactor {
    fn drive(&self, waker: &task::Waker) {
        let a = {
            let read_guard = self.instance.consumer_instance.read();
            let flags = if unsafe {
                read_guard
                    .sender()
                    .as_64()
                    .expect("expected 64-bit SQEs")
                    .ring_header()
            }
            .available_entry_count()
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

        let mut write_guard = self.instance.consumer_instance.write();

        let available_completions =
            unsafe { write_guard.receiver().as_64().unwrap().ring_header() }
                .available_entry_count();

        if a > available_completions {
            log::warn!("The kernel/other process gave us a higher number of available completions than present on the ring.");
        }

        for _ in 0..available_completions {
            let result = write_guard
                .receiver_mut()
                .as_64_mut()
                .expect("expected 64-bit CQEs")
                .try_recv();

            match result {
                Ok(cqe) => { let _ = Self::handle_cqe(self.trusted_instance, self.tag_map.read(), waker, cqe); }
                Err(RingPopError::Empty { .. }) => panic!("the kernel gave us a higher number of available completions than actually available"),
                Err(RingPopError::Shutdown) => self.instance.dropped.store(true, std::sync::atomic::Ordering::Release),
            }
        }
    }
    fn handle_cqe(
        trusted_instance: bool,
        tags: RwLockReadGuard<'_, BTreeMap<usize, Arc<Mutex<State>>>>,
        driving_waker: &task::Waker,
        cqe: CqEntry64,
    ) -> Option<()> {
        let cancelled = cqe.status == (-(ECANCELED as i64)) as u64;

        let state_arc;

        let state_lock = if trusted_instance {
            let pointer = usize::try_from(cqe.user_data).ok()? as *mut Mutex<State>;
            state_arc = unsafe { Arc::from_raw(pointer) };
            assert_eq!(
                Arc::strong_count(&state_arc),
                1,
                "expected strong count to be one when receiving a direct future"
            );
            assert_eq!(
                Arc::weak_count(&state_arc),
                1,
                "expected weak count to be one when receiving a direct future"
            );
            &state_arc
        } else {
            tags.get(&cqe.user_data.try_into().ok()?)?
        };

        let mut state = state_lock.lock();
        match &*state {
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
        }
        Some(())
    }
}
pub struct Handle {
    reactor: Weak<Reactor>,
}

impl Handle {
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

                if reactor.trusted_instance {
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

                if reactor.trusted_instance {
                    (None, Some(state_arc))
                } else {
                    reactor.tag_map.write().insert(n, state_arc);
                    (Some(n), None)
                }
            }
        };

        CommandFuture {
            reactor: Weak::clone(&self.reactor),
            repr: if reactor.trusted_instance {
                CommandFutureRepr::Direct {
                    state: Either::Left(state_opt.unwrap()),
                    initial_sqe: sqe,
                }
            } else {
                CommandFutureRepr::Tagged {
                    tag: tag_num_opt.unwrap(),
                    initial_sqe: Some(sqe),
                }
            },
        }
    }
    pub unsafe fn subscribe_to_fd_updates(&self, _fd: usize) -> FdUpdates {
        todo!()
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
                    log::warn!("Truncating the number of bytes read as it could not fit usize, from {} to {}", signed, trunc);
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

    pub async unsafe fn close(&self, fd: usize, flush: bool) -> Result<()> {
        let sqe = SqEntry64::new(IoUringSqeFlags::empty(), 0, (-1i64) as u64)
            .close(fd.try_into().or(Err(Error::new(EOVERFLOW)))?, flush);
        let cqe = self.send(sqe).await?;

        Self::completion_as_rw_io_result(cqe)?;

        Ok(())
    }
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
}

pub struct CommandFuture {
    repr: CommandFutureRepr,
    reactor: Weak<Reactor>,
}

// the internal state of the pending command.
#[derive(Debug)]
enum State {
    // the future has been initiated, but nothing has happened to it yet
    Initial,

    // the future is currently trying to submit the future, to a ring that was full
    Submitting(SqEntry64, task::Waker),

    // the future has submitted the SQE, but is waiting for the command to complete with the
    // corresponding CQE.
    Completing(task::Waker),

    // the future has received the CQE, and the command is now complete. this state can now be
    // reused for another future.
    Completed(CqEntry64),

    // the command was cancelled, while in the Completing state.
    Cancelled,
}

enum CommandFutureRepr {
    Direct {
        state: Either<Arc<Mutex<State>>, Weak<Mutex<State>>>,
        initial_sqe: SqEntry64,
    },

    Tagged {
        tag: usize,
        initial_sqe: Option<SqEntry64>,
    },
}

impl Future for CommandFuture {
    type Output = Result<CqEntry64>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        // TODO: Cleaner code?
        let this = self.get_mut();

        let reactor = this
            .reactor
            .upgrade()
            .expect("failed to poll CommandFuture: reactor is dead");

        fn try_submit(
            instance: &mut v1::ConsumerInstance,
            state: &mut State,
            cx: &mut task::Context<'_>,
            sqe: SqEntry64,
            state_arc: Option<Arc<Mutex<State>>>,
        ) -> (task::Poll<Result<CqEntry64>>, bool) {
            let sqe = if let Some(state_arc) = state_arc {
                match (Arc::into_raw(state_arc) as usize).try_into() {
                    Ok(ptr64) => sqe.with_user_data(ptr64),
                    Err(_) => return (task::Poll::Ready(Err(Error::new(EFAULT))), false),
                }
            } else {
                sqe
            };

            match instance
                .sender_mut()
                .as_64_mut()
                .expect("expected instance with 64-bit SQEs")
                .try_send(sqe)
            {
                Ok(()) => {
                    *state = State::Completing(cx.waker().clone());
                    return (task::Poll::Pending, true);
                }
                Err(RingPushError::Full(sqe)) => {
                    *state = State::Submitting(sqe, cx.waker().clone());
                    return (task::Poll::Pending, false);
                }
                Err(RingPushError::Shutdown(_)) => {
                    return (task::Poll::Ready(Err(Error::new(ESHUTDOWN))), false);
                }
            }
        }

        let tags_guard;
        let mut initial_sqe;

        let (state_lock, mut init_sqe, is_direct) = match this.repr {
            CommandFutureRepr::Tagged {
                tag,
                ref mut initial_sqe,
            } => {
                tags_guard = reactor.tag_map.read();
                let state_lock = tags_guard
                    .get(&tag)
                    .expect("CommandFuture::poll error: tag used by future has been removed");

                (Either::Left(state_lock), initial_sqe, false)
            }
            CommandFutureRepr::Direct {
                ref state,
                initial_sqe: sqe,
            } => {
                initial_sqe = Some(sqe);
                (state.as_ref(), &mut initial_sqe, true)
            }
        };
        let state_arc = state_lock.either(
            |arc| Arc::clone(arc),
            |weak| {
                weak.upgrade()
                    .expect("CommandFuture::poll error: state has been dropped")
            },
        );

        let mut state_guard = state_arc.lock();

        let mut in_state_sqe;

        if let State::Submitting(sqe, _) = *state_guard {
            in_state_sqe = Some(sqe);
            init_sqe = &mut in_state_sqe;
        }
        match &*state_guard {
            &State::Initial | &State::Submitting(_, _) => {
                let (result, downgrade) = try_submit(
                    &mut *reactor.instance.consumer_instance.write(),
                    &mut *state_guard,
                    cx,
                    init_sqe.expect("expected an initial SQE when submitting command"),
                    if is_direct {
                        Some(Arc::clone(&state_arc))
                    } else {
                        None
                    },
                );

                if downgrade {
                    if let CommandFutureRepr::Direct { state, .. } = &mut this.repr {
                        if let Either::Left(arc) = state {
                            *state = Either::Right(Arc::downgrade(&arc));
                        } else {
                            unreachable!("try_submit downgraded twice")
                        }
                    } else {
                        unreachable!("try_submit returned downgrade when the repr was not Direct");
                    }
                }

                return result;
            }
            &State::Completing(_) => return task::Poll::Pending,

            &State::Completed(cqe) => {
                *state_guard = State::Initial;
                return task::Poll::Ready(Ok(cqe));
            }
            &State::Cancelled => {
                *state_guard = State::Initial;
                return task::Poll::Ready(Err(Error::new(ECANCELED)));
            }
        }
    }
}

pub struct FdUpdates;

impl Stream for FdUpdates {
    type Item = Result<Event>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        todo!()
    }
}

/*
pub struct Buffer<'a> {
    bytes: *mut u8,
    size: usize,
    finished: AtomicBool<bool>,
    _marker: PhantomData<&'a mut [u8]>,
}
pub type OwnedBuffer = Buffer<'static>;

impl<'a> Buffer<'a> {
    pub unsafe fn from_slice(slice: &'a mut [u8]) -> Self {
        Self {
            bytes: slice.as_mut_ptr(),
            size: slice.len(),
            finished: AtomicBool::new(true),
            _marker: PhantomData,
        }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if !self.finished {
            #[cfg(debug_assertions)]
            eprintln!("Leaking currently kernel-owned buffer at {:p}, size {}", self.bytes, self.size);

            // if the kernel owns the buffer and someone was stupid enough to drop it, the only
            // thing that can be done is to leak it
            return;
        }

        Box::from_raw(unsafe { slice::from_raw_parts(self.bytes, self.size) as *const [u8] })
    }
}

pub struct File {
    fd: usize,
    buffer: Buffer,
}

impl AsyncBufRead for File {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
    }
    fn consume(self: Pin<&mut Self>, amt: usize) {
    }
}*/
