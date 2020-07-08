use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::{slice, task};

use syscall::{
    EINVAL, EOVERFLOW, ESHUTDOWN, ECANCELED,
    Error, Event, IoVec, Result,
};
pub use syscall::io_uring::*;

use crossbeam_queue::ArrayQueue;
pub use futures::io::AsyncBufRead;
use futures::Stream;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};


/// A minimal executor, that does not use any thread pool.
pub struct Executor {
    trusted_instance: bool,
    instance: Arc<InstanceWrapper>,
    standard_waker: task::Waker,

    // TODO: ConcurrentBTreeMap
    tag_map: Arc<RwLock<BTreeMap<usize, Mutex<State>>>>,
    next_tag: Arc<AtomicUsize>,
    reusable_tags: Arc<ArrayQueue<(usize, State)>>,
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
            .. self
        }
    }

    pub fn build(self, instance: v1::ConsumerInstance) -> Executor {
        Executor::new(instance, self.trusted_instance)
    }
}

impl Executor {
    fn new(instance: v1::ConsumerInstance, trusted_instance: bool) -> Self {
        let instance_arc = Arc::new(InstanceWrapper {
            consumer_instance: RwLock::new(instance),
            dropped: AtomicBool::new(false),
        });
        Self {
            standard_waker: Self::waker(&instance_arc),
            instance: instance_arc,
            trusted_instance,

            tag_map: Arc::new(RwLock::new(BTreeMap::new())),
            next_tag: Arc::new(AtomicUsize::new(1)),
            reusable_tags: Arc::new(ArrayQueue::new(512)),
        }
    }
    fn waker(instance: &Arc<InstanceWrapper>) -> task::Waker {
        let instance = Arc::downgrade(instance);
        async_task::waker_fn(move || {
            let instance = match instance.upgrade() {
                Some(i) => i,
                None => return,
            };
            let instance_guard = instance.consumer_instance.read();

            match instance_guard.sender() {
                &ConsumerGenericSender::Bits32(ref sender32) => sender32.notify(),
                &ConsumerGenericSender::Bits64(ref sender64) => sender64.notify(),
            }
        })
    }
    /// Run a future until completion.
    pub fn run<O, F>(&self, future: F) -> O
    where
        F: Future<Output = O>,
    {
        let mut future = future;

        let waker = &self.standard_waker;
        let mut cx = task::Context::from_waker(&waker);

        loop {
            let pinned_future = unsafe { Pin::new_unchecked(&mut future) };
            match pinned_future.poll(&mut cx) {
                task::Poll::Ready(o) => return o,
                task::Poll::Pending => {
                    let a = {
                        let read_guard = self.instance.consumer_instance.read();
                        let flags = if unsafe { read_guard.sender().as_64().expect("expected 64-bit SQEs").ring_header() }.available_entry_count() > 0 {
                            IoUringEnterFlags::empty()
                        } else {
                            IoUringEnterFlags::WAKEUP_ON_SQ_AVAIL
                        };
                        read_guard.wait(0, flags).expect("redox_iou: failed to enter io_uring")
                    };

                    let mut write_guard = self.instance.consumer_instance.write();

                    let available_completions = unsafe { write_guard.receiver().as_64().unwrap().ring_header() }.available_entry_count();

                    if a > available_completions {
                        log::warn!("The kernel/other process gave us a higher number of available completions than present on the ring.");
                    }

                    for _ in 0..available_completions {
                        let result = write_guard.receiver_mut().as_64_mut().expect("expected 64-bit CQEs").try_recv();

                        match result {
                            Ok(cqe) => { let _ = Self::handle_cqe(self.trusted_instance, self.tag_map.read(), &self.standard_waker, cqe); }
                            Err(RingPopError::Empty { .. }) => panic!("the kernel gave us a higher number of available completions than actually available"),
                            Err(RingPopError::Shutdown) => self.instance.dropped.store(true, std::sync::atomic::Ordering::Release),
                        }
                    }
                }
            }
        }
    }

    fn handle_cqe(trusted_instance: bool, tags: RwLockReadGuard<'_, BTreeMap<usize, Mutex<State>>>, standard_waker: &task::Waker, cqe: CqEntry64) -> Option<()> {
        let cancelled = cqe.status == (-(ECANCELED as i64)) as u64;

        let state_lock = if trusted_instance {
            let pointer = usize::try_from(cqe.user_data).ok()? as *mut Mutex<State>;
            let state_box = unsafe { Box::from_raw(pointer) };
            todo!()
        } else {
            tags.get(&cqe.user_data.try_into().ok()?)?
        };

        let mut state = state_lock.lock();
        match &*state {
            // invalid state after having received a completion
            State::Initial | State::Submitting(_, _) | State::Completed(_) | State::Cancelled => return None,
            State::Completing(waker) => {
                if !waker.will_wake(standard_waker) {
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
    pub fn handle(&self) -> Handle {
        Handle {
            instance: Arc::downgrade(&self.instance),
            next_tag: Arc::downgrade(&self.next_tag),
            reusable_tags: Arc::downgrade(&self.reusable_tags),
            tag_map: Arc::downgrade(&self.tag_map),
            trusted_instance: self.trusted_instance,
        }
    }
}
pub struct Handle {
    instance: Weak<InstanceWrapper>,
    trusted_instance: bool,

    // TODO: reusable_tags is completely useless
    reusable_tags: Weak<ArrayQueue<(usize, State)>>,
    tag_map: Weak<RwLock<BTreeMap<usize, Mutex<State>>>>,
    next_tag: Weak<AtomicUsize>,
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
        CommandFuture {
            instance: self.instance.upgrade().expect("cannot send command: executor dead"),
            repr: if self.trusted_instance {
                CommandFutureRepr::Direct(todo!())
            } else {
                let tag_num = match self.reusable_tags.upgrade().expect("cannot reclaim tag: executor dead").pop() {
                    // try getting a reusable tag to minimize unnecessary allocations
                    Ok((n, tag)) => {
                        assert!(matches!(tag, State::Initial), "reusable tag was not in the reclaimed state");
                        self.tag_map.upgrade().expect("cannot insert tag: executor dead").write().insert(n, Mutex::new(tag)).expect("reusable tag was already within the active tag map");
                        n
                    }
                    // if no reusable tag was present, create a new tag
                    Err(crossbeam_queue::PopError) => {
                        let n = self.next_tag.upgrade().expect("cannot get new tag number: executor dead").fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.tag_map.upgrade().expect("cannot insert new tag: executor dead").write().insert(n, Mutex::new(State::Initial));
                        n
                    }
                };
                CommandFutureRepr::Tagged {
                    tag: tag_num,
                    tags: self.tag_map.upgrade().expect("cannot get tag: executor dead"),
                    initial_sqe: Some(sqe),
                }
            },
        }
    }
    pub unsafe fn subscribe_to_fd_updates(&self, fd: usize) -> FdUpdates {
        todo!()
    }

    fn completion_as_rw_io_result(cqe: CqEntry64) -> Result<usize> {
        // reinterpret the status as signed, to take an errors into account.
        let signed = cqe.status as i64;

        match isize::try_from(signed) {
            Ok(s) => return Error::demux(s as usize),
            Err(_) => {
                log::warn!("Failed to cast 64 bit {{,p}}{{read,write}}{{,v}} status ({:?}), into pointer sized status.", Error::demux64(signed as u64));
                if let Ok(actual_bytes_read) = Error::demux64(signed as u64) {
                    let trunc = std::cmp::min(isize::max_value() as u64, actual_bytes_read) as usize;
                    log::warn!("Truncating the number of bytes read as it could not fit usize, from {} to {}", signed, trunc);
                    return Ok(trunc as usize);
                }
                return Err(Error::new(EOVERFLOW));
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

        let cqe = self.send(
            sqe
        ).await?;
        Self::completion_as_rw_io_result(cqe)
    }

    pub async unsafe fn open_raw<B: AsRef<[u8]> + ?Sized>(&self, path: &B, flags: u64) -> Result<usize> {
        let sqe = SqEntry64::new(IoUringSqeFlags::empty(), 0, (-1i64) as u64)
            .open(path.as_ref(), flags);
        let cqe = self.send(sqe).await?;
        Self::completion_as_rw_io_result(cqe)
    }
    pub async fn open_raw_static<B: AsRef<[u8]> + ?Sized + 'static>(&self, path: &'static B, flags: u64) -> Result<usize> {
        unsafe { self.open_raw(path, flags) }.await
    }
    pub async fn open_raw_move_buf(&self, path: Vec<u8>, flags: u64) -> Result<(usize, Vec<u8>)> {
        let fd = unsafe { self.open_raw(&*path, flags) }.await?;
        Ok((fd, path))
    }
    pub async unsafe fn open<S: AsRef<str> + ?Sized>(&self, path: &S, flags: u64) -> Result<usize> {
        self.open_raw(path.as_ref().as_bytes(), flags).await
    }
    pub async fn open_static<S: AsRef<str> + ?Sized + 'static>(&self, path: &'static S, flags: u64) -> Result<usize> {
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
    pub async unsafe fn close_range(&self, range: std::ops::Range<usize>, flush: bool) -> Result<()> {
        let start: u64 = range.start.try_into().or(Err(Error::new(EOVERFLOW)))?;
        let end: u64 = range.end.try_into().or(Err(Error::new(EOVERFLOW)))?;
        let count = end.checked_sub(start).ok_or(Error::new(EINVAL))?;

        let sqe = SqEntry64::new(IoUringSqeFlags::empty(), 0, (-1i64) as u64)
            .close_many(start, count, flush);
        let cqe = self.send(sqe).await?;

        Self::completion_as_rw_io_result(cqe)?;

        Ok(())
    }

    pub async unsafe fn read(&self, fd: usize, buf: &mut [u8]) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.read(fd, buf)).await
    }
    pub async unsafe fn readv(&self, fd: usize, bufs: &[IoVec]) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.readv(fd, bufs)).await
    }
    pub async unsafe fn pread(&self, fd: usize, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.pread(fd, buf, offset)).await
    }
    pub async unsafe fn preadv(&self, fd: usize, bufs: &[IoVec], offset: u64) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.preadv(fd, bufs, offset)).await
    }

    pub async unsafe fn write(&self, fd: usize, buf: &[u8]) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.write(fd, buf)).await
    }
    pub async unsafe fn writev(&self, fd: usize, bufs: &[IoVec]) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.writev(fd, bufs)).await
    }
    pub async unsafe fn pwrite(&self, fd: usize, buf: &[u8], offset: u64) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.pwrite(fd, buf, offset)).await
    }
    pub async unsafe fn pwritev(&self, fd: usize, bufs: &[IoVec], offset: u64) -> Result<usize> {
        self.rw_io(fd, |sqe, fd| sqe.pwritev(fd, bufs, offset)).await
    }
}

pub struct CommandFuture {
    repr: CommandFutureRepr,
    instance: Arc<InstanceWrapper>,
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

enum CommandFutureState {
    Initial(SqEntry64),
    Submitting(SqEntry64),
    Completing,
    Finished,
}

enum CommandFutureRepr {
    Direct(ManuallyDrop<Box<Mutex<State>>>),

    Tagged {
        tag: usize,
        tags: Arc<RwLock<BTreeMap<usize, Mutex<State>>>>,
        initial_sqe: Option<SqEntry64>,
    },
}

impl Future for CommandFuture {
    type Output = Result<CqEntry64>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let this = self.get_mut();

        fn try_submit(instance: &mut v1::ConsumerInstance, state: &mut State, cx: &mut task::Context<'_>, sqe: SqEntry64) -> task::Poll<Result<CqEntry64>> {
            match instance.sender_mut().as_64_mut().expect("expected instance with 64-bit SQEs").try_send(sqe) {
                Ok(()) => {
                    *state = State::Completing(cx.waker().clone());
                    return task::Poll::Pending;
                }
                Err(RingPushError::Full(sqe)) => {
                    *state = State::Submitting(sqe, cx.waker().clone());
                    return task::Poll::Pending;
                }
                Err(RingPushError::Shutdown(_)) => return task::Poll::Ready(Err(Error::new(ESHUTDOWN))),
            }
        }

        match this.repr {
            CommandFutureRepr::Tagged { tag, ref tags, ref mut initial_sqe } => {
                let tags = tags.read();
                let tag_lock = tags.get(&tag).expect("CommandFuture error: tag used by future has been removed");
                let mut tag = tag_lock.lock();

                match &*tag {
                    &State::Initial => return try_submit(&mut *this.instance.consumer_instance.write(), &mut *tag, cx, initial_sqe.take().expect("expected an initial SQE when in the initial state")),
                    &State::Submitting(sqe, _) => return try_submit(&mut *this.instance.consumer_instance.write(), &mut *tag, cx, sqe),
                    &State::Completing(_) => return task::Poll::Pending,

                    &State::Completed(cqe) => return task::Poll::Ready(Ok(cqe)),
                    &State::Cancelled => return task::Poll::Ready(Err(Error::new(ECANCELED))),
                }
            }
            CommandFutureRepr::Direct(ref state) => todo!(),
        }
    }
}

pub struct FdUpdates;

impl Stream for FdUpdates {
    type Item = Result<Event>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Option<Self::Item>> {
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
