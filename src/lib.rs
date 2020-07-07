use std::convert::{TryFrom, TryInto};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, atomic::AtomicBool};
use std::{slice, task};

use syscall::{
    EOVERFLOW, ESHUTDOWN,
    Error, Event, IoVec, Result,
};

use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};

pub use syscall::io_uring::*;
pub use futures::io::AsyncBufRead;
use futures::Stream;

/// A minimal executor, that does not use any thread pool.
pub struct Executor {
    instance: Arc<RwLock<v1::ConsumerInstance>>,
    standard_waker: task::Waker,

    // TODO: ConcurrentBTreeMap
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
    /// checked at runtime, this is too expensive to check if performance is a concern. When the
    /// kernel is a producer though, this will not make anything more unsafe (since the kernel has
    /// full access to the address space anyways).
    ///
    pub unsafe fn assume_trusted_instance() -> Self {
        Self {
            trusted_instance: true,
        }
    }

    pub fn build(instance: v1::ConsumerInstance) -> Executor {
        Executor::new(instance)
    }
}

impl Executor {
    fn new(instance: v1::ConsumerInstance) -> Self {
        let instance_arc = Arc::new(RwLock::new(instance));
        Self {
            standard_waker: Self::waker(&instance_arc),
            instance: instance_arc,
        }
    }
    fn waker(instance: &Arc<RwLock<v1::ConsumerInstance>>) -> task::Waker {
        let instance = Arc::downgrade(instance);
        async_task::waker_fn(move || {
            let instance = match instance.upgrade() {
                Some(i) => i,
                None => return,
            };
            let instance_guard = instance.read();

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
        pin_utils::pin_mut!(future);

        let waker = self.waker();
        let cx = task::Context::from_waker(&waker);

        loop {
            match future.poll(cx) {
                task::Poll::Ready(o) => return o,
                task::Poll::Pending => {
                    let read_guard = self.instance.read();
                    read_guard.wait(0, IoUringEnterFlags::empty()).expect("redox_iou: failed to enter io_uring");
                }
            }
        }
    }
    ///
    /// Get a future which represents submitting a command, and then waiting for it to complete. If
    /// this executor was built with `assume_trusted_instance`, the user data field of the sqe will
    /// be overridden, so that it can store the pointer to the state.
    ///
    /// # Safety
    /// Unsafe because there is no guarantee that the buffers used by `sqe` will be used properly.
    /// If a future is dropped, and its memory is used again (possibly on the stack where it is
    /// even worse), there will be a data race between the kernel and the process.
    ///
    /// Additionally, the buffers used may point to invalid locations on the stack or heap, which
    /// is UB.
    ///
    pub unsafe fn send(&self, sqe: SqEntry64) -> CommandFuture {
        CommandFuture {
            state: Some(CommandFutureState {
                kind: CommandFutureStateKind::PendingSubmission(sqe),
                instance: Arc::clone(&self.instance),
                standard_waker: self.standard_waker.clone(),
            }),
        }
    }
    pub unsafe fn subscribe_to_fd_updates(&self, fd: usize) -> impl Stream<Item = (Event, u64)> {
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

        let cqe = unsafe { self.send(
            sqe
        ).await? };
        Self::completion_as_rw_io_result(cqe)
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

enum CommandFutureRepr {
    Direct,
    Tagged,
}

pub struct CommandFuture {
    state: Option<CommandFutureState>,
}

struct CommandFutureState {
    kind: CommandFutureStateKind,
    instance: Arc<RwLock<v1::ConsumerInstance>>,
    standard_waker: task::Waker,
    other_wakers: Option<Arc<Mutex<Vec<task::Waker>>>>,
}

enum CommandFutureStateKind {
    // The future cannot send the command due to the SQ being full. Thus, it will have to wait for
    // the producer to pop at least one item.
    PendingSubmission(SqEntry64),

    // The future is waiting for the command to complete.
    PendingCompletion,
}

pub enum CommandFutureError<S> {
    ShutdownDuringSend(S),
    ShutdownDuringRecv,
}
impl<S> From<CommandFutureError<S>> for Error {
    fn from(error: CommandFutureError<S>) -> Self {
        match error {
            CommandFutureError::ShutdownDuringRecv => Self::new(ESHUTDOWN),
            CommandFutureError::ShutdownDuringSend(_) => Self::new(ESHUTDOWN),
        }
    }
}

impl Future for CommandFuture {
    type Output = Result<CqEntry64, CommandFutureError<SqEntry64>>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let this = self.get_mut();
        let state = this.state.as_mut().expect("cannot poll CommandFuture after it has finished");

        fn change_state_and_block(state: &mut CommandFutureState, new_kind: CommandFutureStateKind) {
            state.kind = new_kind;

        }
        fn mark_pending(state: &CommandFutureState, cx: &mut task::Context<'_>) {
            if ! state.standard_waker.will_wake(cx.waker()) {
                state.other_wakers.as_ref().expect("pushing other waker when external wakers weren't supported").lock().push(cx.waker().clone());
            }
        }

        match this.state.kind {
            CommandFutureStateKind::PendingSubmission(entry) => {
                let mut write_guard = state.instance.write();
                match write_guard.sender_mut() {
                    &mut ConsumerGenericSender::Bits32(ref mut sender32) => todo!(),
                    &mut ConsumerGenericSender::Bits64(ref mut sender64) => match sender64.try_send(entry) {
                        Ok(()) => {
                            state.kind = CommandFutureStateKind::PendingCompletion;
                            self.poll(cx)
                        }
                        Err(RingPushError::Full(entry)) => {
                            mark_pending(&*state, cx);
                            return task::Poll::Pending;
                        }
                        Err(RingPushError::Shutdown(entry)) => return task::Poll::Ready(Err(CommandFutureError::ShutdownDuringSend(entry))),
                    }
                }
            }
            CommandFutureStateKind::PendingCompletion => {
                let mut write_guard = state.instance.write();

                match write_guard.receiver_mut() {
                    &mut ConsumerGenericReceiver::Bits32(ref mut receiver32) => todo!(),
                    &mut ConsumerGenericReceiver::Bits64(ref mut receiver64) => match receiver64.try_recv() {
                        Ok(cqe) => task::Poll::Ready(Ok(cqe)),
                        Err(RingPopError::Empty { .. }) => mark_pending(&*state),
                        Err(RingPopError::Shutdown) => return task::Poll::Ready(Err(CommandFutureError::ShutdownDuringRecv)),
                    }
                }
            }
        }
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
