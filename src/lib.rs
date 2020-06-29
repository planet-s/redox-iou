use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, atomic::AtomicBool};
use std::{slice, task};

use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};

pub use syscall::io_uring::*;
pub use futures::io::AsyncBufRead;

/// A minimal executor, that does not use any thread pool.
pub struct Executor {
    instance: Arc<RwLock<v1::ConsumerInstance>>,
    standard_waker: task::Waker,
    other_wakers: Arc<Mutex<Vec<task::Waker>>>,
}

impl Executor {
    pub fn from_consumer_instance(instance: v1::ConsumerInstance) -> Self {
        let instance_arc = Arc::new(RwLock::new(instance));
        Self {
            standard_waker: Self::waker(&instance_arc),
            instance: instance_arc,
            other_wakers: Mutex::new(Vec::new()),
        }
    }
    fn waker(instance: &Arc<RwLock<v1::ConsumerInstance>>) -> task::Waker {
        let instance = Arc::downgrade(instance);
        let waker = async_task::waker_fn(move || {
            let instance = match instance.upgrade() {
                Some(i) => i,
                None => return,
            };
            let instance_guard = instance.read();

            match instance_guard.sender() {
                &ConsumerGenericSender::Bits32(ref sender32) => sender32.notify(),
                &ConsumerGenericSender::Bits64(ref sender64) => sender64.notify(),
            }
        });
    }
    /// Run a future until completion.
    pub fn run<O, F>(&self, future: F) -> O
    where
        F: Future<Ouptut = O>,
    {
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
    /// Get a future which represents submitting a command then waiting for it to complete.
    ///
    /// # Safety
    /// Unsafe because there is no guarantee that the buffers used by `sqe` will be used properly.
    /// If a future is dropped, and its memory is used again (possibly on the stack where it is
    /// even worse), there will be a data race between the kernel and the process.
    pub unsafe fn send(&self, sqe: SqEntry64) -> CommandFuture {
        CommandFuture {
            state: Some(CommandFutureState {
                kind: CommandFutureStateKind::PendingSubmission(sqe),
                instance: Arc::clone(&self.instance),
                standard_waker: self.standard_waker.clone(),
                other_wakers: Arc::clone(&self.other_wakers),
            }),
        }
    }
}

pub struct CommandFuture {
    state: Option<CommandFutureState>,
}

struct CommandFutureState {
    kind: CommandFutureStateKind,
    instance: Arc<RwLock<v1::ConsumerInstance>>,
    standard_waker: task::Waker,
    other_wakers: Arc<Mutex<Vec<task::Waker>>>,
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

impl Future for CommandFuture {
    type Output = Result<CqEntry64, CommandFutureError<SqEntry64>>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let this = self.get_mut();
        let state = this.state.as_mut().expect("cannot poll CommandFuture after it has finished");

        fn change_state_and_block(state: &mut CommandFutureState, new_kind: CommandFutureStateKind) -> task::Poll<Self::Output> {
            this.state.kind = new_kind;

        }
        fn mark_pending(state: &CommandFutureState) {
            if ! state.standard_waker.will_wake(cx.waker()) {
                state.other_wakers.lock().push(cx.waker().clone());
            }
            task::Poll::Pending
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
                        Err(RingPushError::Full(entry)) => mark_pending(&*state),
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
