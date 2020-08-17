use std::collections::VecDeque;
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};
use std::task;

use syscall::error::{Error, Result};
use syscall::error::{ECANCELED, EFAULT, ESHUTDOWN};
use syscall::io_uring::{CqEntry64, IoUringCqeFlags, RingPushError, SqEntry64};

use either::*;
use futures_core::Stream;
use parking_lot::Mutex;

use crate::instance::ConsumerInstance;
use crate::reactor::{Reactor, RingId};

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
#[derive(Debug)]
pub struct CommandFuture {
    pub(crate) inner: CommandFutureInner,
}

#[derive(Debug)]
pub(crate) struct State {
    pub(crate) inner: StateInner,
    pub(crate) epoch: usize,
}

// the internal state of the pending command.
#[derive(Debug)]
pub(crate) enum StateInner {
    // the future has been initiated, but nothing has happened to it yet
    Initial,

    // the future is currently trying to submit the future, to a ring that was full
    Submitting(SqEntry64, task::Waker),

    // the future has submitted the SQE, but is waiting for the command to complete with the
    // corresponding CQE.
    Completing(task::Waker),

    // the future is a stream and is receiving multiple CQEs.
    ReceivingMulti(VecDeque<CqEntry64>, task::Waker),

    // the future has received the CQE, and the command is now complete. this state can now be
    // reused for another future.
    Completed(CqEntry64),

    // the command was cancelled, while in the Completing state.
    Cancelled,
}

#[derive(Debug)]
pub(crate) enum CommandFutureRepr {
    Direct {
        state: Arc<Mutex<State>>,
        initial_sqe: SqEntry64,
    },

    Tagged {
        tag: Tag,
        initial_sqe: Option<SqEntry64>,
    },
}
impl From<CommandFutureInner> for CommandFuture {
    fn from(inner: CommandFutureInner) -> Self {
        Self { inner }
    }
}
impl From<CommandFutureInner> for FdUpdates {
    fn from(inner: CommandFutureInner) -> Self {
        Self { inner }
    }
}

fn try_submit(
    instance: &ConsumerInstance,
    state: &mut State,
    cx: &mut task::Context<'_>,
    sqe: SqEntry64,
    user_data: Either<Weak<Mutex<State>>, Tag>,
    is_stream: bool,
) -> task::Poll<Option<Result<CqEntry64>>> {
    let sqe = match user_data {
        Left(state_weak) => match (Weak::into_raw(state_weak) as usize).try_into() {
            Ok(ptr64) => sqe.with_user_data(ptr64),
            Err(_) => return task::Poll::Ready(Some(Err(Error::new(EFAULT)))),
        },
        Right(tag) => sqe.with_user_data(tag),
    };
    log::debug!("Sending SQE {:?}", sqe);

    match instance
        .sender()
        .write()
        .as_64_mut()
        .expect("expected instance with 64-bit SQEs")
        .try_send(sqe)
    {
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
        Err(RingPushError::Full(sqe)) => {
            state.inner = StateInner::Submitting(sqe, cx.waker().clone());
            log::debug!("Submission ring is full");
            task::Poll::Pending
        }
        Err(RingPushError::Shutdown(_)) => task::Poll::Ready(Some(Err(Error::new(ESHUTDOWN)))),
    }
}

impl CommandFutureInner {
    fn poll(
        &mut self,
        is_stream: bool,
        cx: &mut task::Context,
    ) -> task::Poll<Option<Result<CqEntry64>>> {
        log::debug!("Polling future");
        let reactor = self
            .reactor
            .upgrade()
            .expect("failed to poll CommandFuture: reactor is dead");

        let tags_guard;
        let mut initial_sqe;

        let (state_lock, mut init_sqe, is_direct, tag) = match self.repr {
            CommandFutureRepr::Tagged {
                tag,
                ref mut initial_sqe,
            } => {
                tags_guard = reactor.tag_map.read();
                let state_lock = tags_guard
                    .get(&tag)
                    .expect("CommandFuture::poll error: tag used by future has been removed");

                (state_lock, initial_sqe, false, Some(tag))
            }
            CommandFutureRepr::Direct {
                ref state,
                initial_sqe: sqe,
            } => {
                initial_sqe = Some(sqe);
                (state, &mut initial_sqe, true, None)
            }
        };

        let mut state_guard = state_lock.lock();

        let mut in_state_sqe;

        if let StateInner::Submitting(sqe, _) = state_guard.inner {
            in_state_sqe = Some(sqe);
            init_sqe = &mut in_state_sqe;
        }
        let instance_either = reactor.instance(self.ring);
        let instance =
            &*instance_either.expect("cannot poll CommandFuture: instance is a producer instance");

        match &mut state_guard.inner {
            &mut StateInner::Initial | &mut StateInner::Submitting(_, _) => try_submit(
                instance,
                &mut *state_guard,
                cx,
                init_sqe.expect("expected an initial SQE when submitting command"),
                if is_direct {
                    Left(Arc::downgrade(&state_lock))
                } else {
                    Right(tag.expect("expected tagged future to have a tag available"))
                },
                is_stream,
            ),
            &mut StateInner::Completing(_) => task::Poll::Pending,
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

            &mut StateInner::Completed(cqe) => {
                state_guard.inner = StateInner::Initial;
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

impl Drop for CommandFuture {
    fn drop(&mut self) {
        // TODO: Implement cancellation.
        let this = &*self;
        let arc;
        let state = match this.inner.repr {
            CommandFutureRepr::Direct { ref state, .. } => &*state,
            CommandFutureRepr::Tagged { tag, .. } => {
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

impl Future for CommandFuture {
    type Output = Result<CqEntry64>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let this = self.get_mut();

        match this.inner.poll(false, cx) {
            task::Poll::Ready(value) => task::Poll::Ready(
                value.expect("expected return value of non-stream to always be Some"),
            ),
            task::Poll::Pending => task::Poll::Pending,
        }
    }
}

/// A stream that yields CQEs representing events, using system event queues under the hood.
#[derive(Debug)]
pub struct FdUpdates {
    inner: CommandFutureInner,
}

impl Stream for FdUpdates {
    type Item = Result<CqEntry64>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        self.get_mut().inner.poll(true, cx)
    }
}

#[derive(Debug)]
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
pub struct ProducerSqes {
    pub(crate) state: Arc<Mutex<ProducerSqesState>>,
}
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
