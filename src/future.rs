use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};
use std::{fmt, task};

use syscall::error::{Error, Result};
use syscall::error::{ECANCELED, EFAULT};
use syscall::flag::EventFlags;

use either::*;
use parking_lot::Mutex;

#[cfg(target_os = "redox")]
use {
    crate::redox::instance::ConsumerInstance,
    syscall::io_uring::{IoUringCqeFlags, RingPushError},
    syscall::error::{EIO, ESHUTDOWN},
};
#[cfg(any(doc, target_os = "redox"))]
use {
    std::collections::VecDeque,
    futures_core::Stream,
    syscall::io_uring::SqEntry64,
};

#[cfg(target_os = "linux")]
use crate::linux::ConsumerInstance;

use crate::reactor::{Reactor, RingId, SysCqe, SysFd, SysSqeRef};

pub(crate) type Tag = u64;
pub(crate) type AtomicTag = AtomicU64;

#[derive(Debug)]
pub(crate) struct CommandFutureInner {
    pub(crate) ring: RingId,
    pub(crate) repr: CommandFutureRepr,
    pub(crate) reactor: Weak<Reactor>,
}
/// A future representing a submission that is being handled by the producer. This future
/// implements [`Unpin`], and is safe to drop or forget at any time (since the guard, if present,
/// will never release the guardee until completion).
pub struct CommandFuture<F> {
    pub(crate) inner: CommandFutureInner,
    pub(crate) prepare_fn: Option<F>,
}

#[derive(Debug)]
pub(crate) struct State {
    pub(crate) inner: StateInner,
    pub(crate) epoch: usize,
}

// The internal state of the pending command.
#[derive(Debug)]
pub(crate) enum StateInner {
    // The future has been initiated, but nothing has happened to it yet
    Initial,

    // The future is currently trying to submit the future, to a ring that was full

    // TODO: Should the SQE be stored here or not?
    Submitting(task::Waker),

    // The future has submitted the SQE, but is waiting for the command to complete with the
    // corresponding CQE.
    Completing(task::Waker),

    // The future is a stream and is receiving multiple CQEs.
    // TODO: Remove the vecdeque from here.
    #[cfg(target_os = "redox")]
    ReceivingMulti(VecDeque<SysCqe>, task::Waker),

    // The future has received the CQE, and the command is now complete. This state can now be
    // reused for another future.
    Completed(SysCqe),

    // The command was cancelled, while in the Completing state.
    Cancelled,
}

#[derive(Debug)]
pub(crate) enum CommandFutureRepr {
    Direct(Arc<Mutex<State>>),
    Tagged(Tag),
}
impl<F> fmt::Debug for CommandFuture<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandFuture")
            .field("inner", &self.inner)
            .field("prepare_fn", &std::any::type_name::<F>())
            .finish()
    }
}

fn try_submit(
    instance: &ConsumerInstance,
    state: &mut State,
    prepare: impl FnOnce(&mut SysSqeRef),
    user_data: Either<Weak<Mutex<State>>, Tag>,
    is_stream: bool,
    cx: &mut task::Context<'_>,
) -> task::Poll<syscall::Error> {
    #[cfg(target_os = "redox")]
    let mut sqe_orig = SqEntry64::default();
    #[cfg(target_os = "redox")]
    let mut sqe = &mut sqe_orig;

    #[cfg(target_os = "linux")]
    let mut guard = instance.lock();
    #[cfg(target_os = "linux")]
    let mut sqe = match guard.prepare_sqe() {
        Some(sqe_slot) => sqe_slot,
        None => {
            state.inner = StateInner::Submitting(cx.waker().clone());
            log::debug!("io_uring submission queue full");
            return task::Poll::Pending;
        }
    };

    prepare(&mut sqe);

    match user_data {
        Left(state_weak) => match (Weak::into_raw(state_weak) as usize).try_into() {
            Ok(ptr64) => {
                #[cfg(target_os = "redox")]
                {
                    sqe.user_data = ptr64;
                }

                #[cfg(target_os = "linux")]
                unsafe {
                    sqe.set_user_data(ptr64)
                }
            }
            Err(_) => return task::Poll::Ready(Error::new(EFAULT)),
        },
        Right(tag) => {
            #[cfg(target_os = "redox")]
            {
                sqe.user_data = tag;
            }

            #[cfg(target_os = "linux")]
            unsafe {
                sqe.set_user_data(tag)
            }
        }
    };

    #[cfg(target_os = "redox")]
    let send_result = {
        log::debug!("Sending SQE {:?}", sqe_orig);

        instance
            .sender()
            .write()
            .as_64_mut()
            .expect("expected instance with 64-bit SQEs")
            .try_send(sqe_orig)
    };
    #[cfg(target_os = "redox")]
    match send_result {
        Ok(()) => {
            if is_stream {
                log::debug!("Successfully sent stream command, awaiting CQEs.");
                state.inner = StateInner::ReceivingMulti(VecDeque::new(), cx.waker().clone());
            } else {
                log::debug!("Successfully sent command, awaiting CQE.");
                state.inner = StateInner::Completing(cx.waker().clone());
            }
            task::Poll::Pending
        }
        Err(RingPushError::Full(_)) => {
            state.inner = StateInner::Submitting(cx.waker().clone());
            log::debug!("io_uring submission queue full");
            task::Poll::Pending
        }
        Err(RingPushError::Shutdown(_)) => task::Poll::Ready(Error::new(ESHUTDOWN)),
        Err(RingPushError::Broken(_)) => {
            log::warn!(
                "Failed to pop entry from instance {:?} due to a broken ring",
                instance
            );
            return task::Poll::Ready(Error::new(EIO));
        }
    }
    #[cfg(target_os = "linux")]
    {
        if is_stream {
            todo!();
        //state.inner = StateInner::ReceivingMulti(VecDeque::new(), cx.waker().clone());
        } else {
            log::debug!("Successfully sent command, awaiting CQE.");
            state.inner = StateInner::Completing(cx.waker().clone());
        }
        task::Poll::Pending
    }
}

impl CommandFutureInner {
    fn poll(
        &mut self,
        is_stream: bool,
        prepare_fn: Option<impl FnOnce(&mut SysSqeRef)>,
        cx: &mut task::Context,
    ) -> task::Poll<Option<Result<SysCqe>>> {
        log::debug!("Polling future");
        let reactor = self
            .reactor
            .upgrade()
            .expect("failed to poll CommandFuture: reactor is dead");

        let tags_guard;

        let (state_lock, is_direct, tag) = match self.repr {
            CommandFutureRepr::Tagged(tag) => {
                tags_guard = reactor.tag_map.read();
                let state_lock = tags_guard
                    .get(&tag)
                    .expect("CommandFuture::poll error: tag used by future has been removed");

                (state_lock, false, Some(tag))
            }
            CommandFutureRepr::Direct(ref state) => (state, true, None),
        };

        let mut state_guard = state_lock.lock();

        let instance_either = reactor.consumer_instance(self.ring);
        let instance = &*instance_either;

        match &mut state_guard.inner {
            &mut StateInner::Initial | &mut StateInner::Submitting(_) => try_submit(
                instance,
                &mut *state_guard,
                prepare_fn.expect("expected preparation function when the state was in initial"),
                if is_direct {
                    Left(Arc::downgrade(&state_lock))
                } else {
                    Right(tag.expect("expected tagged future to have a tag available"))
                },
                is_stream,
                cx,
            )
            .map(Err)
            .map(Some),
            &mut StateInner::Completing(_) => task::Poll::Pending,

            #[cfg(target_os = "redox")]
            &mut StateInner::ReceivingMulti(ref mut received, ref mut waker) => {
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }

                if let Some(item) = received.pop_front() {
                    let flags = IoUringCqeFlags::from_bits_truncate((item.flags & 0xFF) as u8);

                    // TODO: Use EVENT flag.

                    if flags.contains(IoUringCqeFlags::LAST_UPDATE) {
                        return task::Poll::Ready(None);
                    }

                    task::Poll::Ready(Some(Ok(item)))
                } else {
                    task::Poll::Pending
                }
            }

            completed @ &mut StateInner::Completed(_) => {
                let cqe = match std::mem::replace(completed, StateInner::Initial) {
                    StateInner::Completed(cqe) => cqe,
                    _ => unreachable!(),
                };
                state_guard.epoch += 1;
                task::Poll::Ready(Some(Ok(cqe)))
            }
            &mut StateInner::Cancelled => {
                state_guard.inner = StateInner::Initial;
                state_guard.epoch += 1;
                task::Poll::Ready(Some(Err(Error::new(ECANCELED))))
            }
        }
    }
}

impl<F> Drop for CommandFuture<F> {
    fn drop(&mut self) {
        // TODO: Implement cancellation.
        let this = &*self;
        let arc;
        let state = match this.inner.repr {
            CommandFutureRepr::Direct(ref state) => &*state,
            CommandFutureRepr::Tagged(tag) => {
                let state_opt = this
                    .inner
                    .reactor
                    .upgrade()
                    .and_then(|reactor| Some(Arc::clone(reactor.tag_map.read().get(&tag)?)));

                match state_opt {
                    Some(state) => {
                        arc = state;
                        &*arc
                    }
                    None => {
                        log::debug!("Dropping future: {:?}, state: (reactor dead)", this);
                        return;
                    }
                }
            }
        };
        log::debug!("Future when dropped: {:?}, state: {:?}", this, state);
    }
}

impl<F> Future for CommandFuture<F>
where
    F: for<'ring, 'tmp> FnOnce(&'tmp mut SysSqeRef<'ring>) + Unpin,
{
    type Output = Result<SysCqe>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let this = self.get_mut();

        match this.inner.poll(false, this.prepare_fn.take(), cx) {
            task::Poll::Ready(value) => task::Poll::Ready(
                value.expect("expected return value of non-stream to always be Some"),
            ),
            task::Poll::Pending => task::Poll::Pending,
        }
    }
}

/// A stream that yields CQEs representing events, using system event queues under the hood.
#[derive(Debug)]
pub struct FdEvents {
    pub(crate) inner: CommandFutureInner,
    pub(crate) initial: Option<FdEventsInitial>,
}

#[derive(Debug)]
pub(crate) struct FdEventsInitial {
    pub(crate) fd: SysFd,
    pub(crate) event_flags: EventFlags,
    pub(crate) oneshot: bool,
    // TODO: Submission context
}

#[cfg(target_os = "redox")]
// TODO: linux
impl Stream for FdEvents {
    type Item = Result<SysCqe>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let prepare_fn = this.initial.take().map(|initial| {
            move |sqe: &mut SysSqeRef| {
                #[cfg(target_os = "redox")]
                {
                    // TODO: Validate fd cast.
                    sqe.sys_register_events(
                        initial.fd as u64,
                        initial.event_flags,
                        initial.oneshot,
                    );
                }
            }
        });
        let is_stream = true;
        this.inner.poll(is_stream, prepare_fn, cx)
    }
}

#[derive(Debug)]
#[cfg(any(doc, target_os = "redox"))]
pub(crate) enum ProducerSqesState {
    Receiving {
        deque: VecDeque<SqEntry64>,
        capacity: usize,
        waker: Option<task::Waker>,
    },
    Finished,
}

/// A stream that yields the SQEs sent to a producer instance.
#[derive(Debug)]
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
pub struct ProducerSqes {
    pub(crate) state: Arc<Mutex<ProducerSqesState>>,
}
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
impl Stream for ProducerSqes {
    type Item = Result<SqEntry64>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut state_guard = this.state.lock();
        log::debug!(
            "Polling ProducerSqes, currently in state {:?}",
            &*state_guard
        );
        match *state_guard {
            ProducerSqesState::Receiving {
                ref mut deque,
                ref mut waker,
                ..
            } => match deque.pop_front() {
                Some(s) => task::Poll::Ready(Some(Ok(s))),
                None => {
                    *waker = Some(cx.waker().clone());
                    task::Poll::Pending
                }
            },
            ProducerSqesState::Finished => task::Poll::Ready(None),
        }
    }
}
