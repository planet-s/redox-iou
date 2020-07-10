use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};
use std::task;

use syscall::data::Event;
use syscall::error::{Error, Result};
use syscall::error::{ECANCELED, EFAULT, ESHUTDOWN};
use syscall::io_uring::{v1, CqEntry64, RingPushError, SqEntry64};

use futures::Stream;
use parking_lot::Mutex;

use crate::reactor::Reactor;

pub(crate) type Tag = u64;
pub(crate) type AtomicTag = AtomicU64;

pub struct CommandFuture {
    pub(crate) repr: CommandFutureRepr,
    pub(crate) reactor: Weak<Reactor>,
}

// the internal state of the pending command.
#[derive(Debug)]
pub(crate) enum State {
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
            state_weak: Option<Weak<Mutex<State>>>,
        ) -> task::Poll<Result<CqEntry64>> {
            let sqe = if let Some(state_weak) = state_weak {
                match (Weak::into_raw(state_weak) as usize).try_into() {
                    Ok(ptr64) => sqe.with_user_data(ptr64),
                    Err(_) => return task::Poll::Ready(Err(Error::new(EFAULT))),
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
                    return task::Poll::Pending;
                }
                Err(RingPushError::Full(sqe)) => {
                    *state = State::Submitting(sqe, cx.waker().clone());
                    return task::Poll::Pending;
                }
                Err(RingPushError::Shutdown(_)) => {
                    return task::Poll::Ready(Err(Error::new(ESHUTDOWN)));
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

                (state_lock, initial_sqe, false)
            }
            CommandFutureRepr::Direct {
                ref state,
                initial_sqe: sqe,
            } => {
                initial_sqe = Some(sqe);
                (state, &mut initial_sqe, true)
            }
        };

        let mut state_guard = state_lock.lock();

        let mut in_state_sqe;

        if let State::Submitting(sqe, _) = *state_guard {
            in_state_sqe = Some(sqe);
            init_sqe = &mut in_state_sqe;
        }
        match &*state_guard {
            &State::Initial | &State::Submitting(_, _) => try_submit(
                &mut *reactor.main_instance.consumer_instance.write(),
                &mut *state_guard,
                cx,
                init_sqe.expect("expected an initial SQE when submitting command"),
                if is_direct {
                    Some(Arc::downgrade(&state_lock))
                } else {
                    None
                },
            ),
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
