//! # The `redox-iou` executor.
//!
//! This executor simply organizes futures in a runqueue. Futures that return
//! [`std::task::Poll::Ready`] when polled, will be kept in the "ready queue", where they are
//! popped, and polled. If the future instead were to return [`std::task::Poll::Pending`], it will
//! be assigned a unique internal tag, and stored in the "pending map". The waker will use this tag
//! to remove the future from the pending map, and then reinsert it into the "ready queue", and so
//! on.
//!
//! The executor can also by default, have the reactor integrated into it. This will most likely be
//! more performant if not more lightweight, since the reactor can do its work when all futures
//! return pending; the alternative, is to let the reactor run in its separate thread, and wake up
//! this executor using regular thread parking. That option is also useful when one wants to use
//! this executor on its own, potentially without the reactor from here at all.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::{fmt, task, thread};

use crossbeam_queue::SegQueue;
use parking_lot::{Mutex, RwLock};

use crate::reactor::{Handle as ReactorHandle, Reactor};

/// A minimal executor, that does not require any thread pool.
#[derive(Debug)]
pub struct Executor {
    // a regular waker that will wait for the reactor or whatever completes the future, to wake the
    // executor up.
    standard_waker: task::Waker,

    // the current thread (yes, only one at the moment), that is currently waiting for the standard
    // waker to wake it up.
    standard_waker_thread: Arc<RwLock<thread::Thread>>,

    // the reactor that this executor is driving, or alternatively None if the driver is running in
    // another thread. the latter is less performant.
    reactor: Option<ReactorWrapper>,

    // the runqueue, storing a queue of ready futures, as well as a map from pending tags, to the
    // pending futures.
    runqueue: Arc<Runqueue>,
}

type TaggedFutureMap = BTreeMap<usize, Pin<Box<dyn Future<Output = ()> + Send + 'static>>>;

pub(crate) struct Runqueue {
    pub(crate) ready_futures: SegQueue<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    pub(crate) pending_futures: Mutex<TaggedFutureMap>,
    pub(crate) next_pending_tag: AtomicUsize,
}

impl fmt::Debug for Runqueue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        struct PendingFutures<'a>(&'a TaggedFutureMap);

        impl<'a> fmt::Debug for PendingFutures<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "({} pending futures)", self.0.len())
            }
        }

        let pending_futures = self.pending_futures.lock();

        f.debug_struct("Runqueue")
            .field("ready_futures", &self.ready_futures)
            .field("pending_futures", &PendingFutures(&*pending_futures))
            .field(
                "next_pending_tag",
                &self.next_pending_tag.load(Ordering::Relaxed),
            )
            .finish()
    }
}

#[derive(Debug)]
struct ReactorWrapper {
    // a reference counted reactor, which handles can be obtained from
    reactor: Arc<Reactor>,

    // a waker type that is more efficient to use if this executor drives the reactor itself (which
    // isn't the case when integrating with e.g. async-std or tokio and using their executors).
    driving_waker: task::Waker,
}

impl Executor {
    ///
    /// Create an executor that does not use include the reactor. This can be slower than having
    /// the reactor built-in, since another thread will have to drive the `io_uring` instead.
    ///
    pub fn without_reactor() -> Self {
        let standard_waker_thread = Arc::new(RwLock::new(thread::current()));

        Self {
            reactor: None,
            standard_waker: Self::standard_waker(&standard_waker_thread, None),
            standard_waker_thread,
            runqueue: Arc::new(Runqueue {
                pending_futures: Mutex::new(BTreeMap::new()),
                ready_futures: SegQueue::new(),
                next_pending_tag: AtomicUsize::new(0),
            }),
        }
    }

    /// Create an executor that includes an integrated reactor.
    ///
    /// This is generally more efficient than offloading the reactor to another thread, provided
    /// that the futures can drive the ring themselves when polled, since the kernel can
    /// immediately wake up the futures once new completion entries are attainable. The executor
    /// can also be woken up by other threads directly, by incrementing the pop epoch of the main
    /// ring, which will trick the kernel into thinking that new entries are available.
    pub fn with_reactor(reactor_arc: Arc<Reactor>) -> Self {
        let mut this = Self::without_reactor();

        this.reactor = Some(ReactorWrapper {
            driving_waker: Reactor::driving_waker(&reactor_arc, None, 0 /* TODO */),
            reactor: reactor_arc,
        });
        this
    }

    fn standard_waker(
        standard_waker_thread: &Arc<RwLock<thread::Thread>>,
        runqueue: Option<(Weak<Runqueue>, usize)>,
    ) -> task::Waker {
        let standard_waker_thread = Arc::downgrade(standard_waker_thread);

        // TODO: Use an pointer to a list of futures, where the lower bits that are ignored due to
        // the alignment, can represent the index or tag of the future. This may cause spurious
        // wakeup if the wakers are handled incorrectly, but should be much faster than having a
        // separate allocation (is is what waker_fn does internally), per waker.
        async_task::waker_fn(move || {
            if let Some((runqueue, tag)) = runqueue
                .as_ref()
                .and_then(|(rq, tag)| Some((rq.upgrade()?, tag)))
            {
                let removed = runqueue.pending_futures.lock().remove(&tag);

                match removed {
                    Some(pending) => runqueue.ready_futures.push(pending),
                    None => return,
                }
            }

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

        let waker = if let Some(ref reactor) = self.reactor {
            reactor.driving_waker.clone()
        } else {
            *self.standard_waker_thread.write() = thread::current();
            self.standard_waker.clone()
        };

        let mut cx = task::Context::from_waker(&waker);

        // TODO: Support pinning the future to a single thread for `!Send` futures, by allowing the
        // future to pin its SQEs to the io_uring in the same thread executing that future, to
        // avoid having to wake up other rings with signals or in other ways. `Send` futures would
        // instead freely be movable to whatever thread is not currently waiting for an io_uring.
        // So, change `idx` to something that is not necessarily zero.
        let idx = 0;

        loop {
            let pinned_future = unsafe { Pin::new_unchecked(&mut future) };
            match pinned_future.poll(&mut cx) {
                task::Poll::Ready(o) => return o,

                task::Poll::Pending => {
                    if let Some(reactor_wrapper) = self.reactor.as_ref() {
                        self.poll_spawned_futures();
                        reactor_wrapper.reactor.drive_primary(idx, &waker, true);
                    } else {
                        self.poll_spawned_futures();
                        thread::park();
                    }
                }
            }
        }
    }
    /// Poll the spawned futures directly, in the foreground.
    pub fn poll_spawned_futures(&self) {
        while let Ok(ready_future) = self.runqueue.ready_futures.pop() {
            let mut ready_future = ready_future;
            let pinned = ready_future.as_mut();

            let tag = self
                .runqueue
                .next_pending_tag
                .fetch_add(1, Ordering::Relaxed);

            let secondary_waker = if let Some(reactor) = self.reactor.as_ref() {
                Reactor::driving_waker(
                    &reactor.reactor,
                    Some((Arc::downgrade(&self.runqueue), tag)),
                    0, // TODO
                )
            } else {
                Self::standard_waker(
                    &self.standard_waker_thread,
                    Some((Arc::downgrade(&self.runqueue), tag)),
                )
            };

            let mut cx = task::Context::from_waker(&secondary_waker);

            match Future::poll(pinned, &mut cx) {
                task::Poll::Ready(()) => (),
                task::Poll::Pending => {
                    self.runqueue
                        .pending_futures
                        .lock()
                        .insert(tag, ready_future);
                }
            }
        }
    }

    /// Get a handle to the integrated reactor, if present. This is a convenience wrapper over
    /// `self.integrated_reactor().map(|r| r.handle())`.
    pub fn reactor_handle(&self) -> Option<ReactorHandle> {
        self.integrated_reactor().map(|reactor| reactor.handle())
    }

    /// Get the integrated reactor used by this executor, if present.
    pub fn integrated_reactor(&self) -> Option<&Arc<Reactor>> {
        self.reactor.as_ref().map(|wrapper| &wrapper.reactor)
    }
    /// Get a handle that can be used for spawning tasks.
    pub fn spawn_handle(&self) -> SpawnHandle {
        SpawnHandle {
            runqueue: Arc::downgrade(&self.runqueue),
            waker: if let Some(reactor) = self.reactor.as_ref() {
                reactor.driving_waker.clone()
            } else {
                self.standard_waker.clone()
            },
        }
    }
}
/// A handle that is used for spawning futures (tasks) onto the executor.
#[derive(Debug)]
pub struct SpawnHandle {
    runqueue: Weak<Runqueue>,
    waker: task::Waker,
}
impl SpawnHandle {
    /// Spawn a future onto the executor. These will be run in the background, after the main
    /// future returns pending.
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let runqueue = self
            .runqueue
            .upgrade()
            .expect("cannot spawn future: runqueue (and therefore executor) is dead");
        runqueue.ready_futures.push(Box::pin(future));
        self.waker.wake_by_ref();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawning() {
        let counter = Arc::new(AtomicUsize::new(0));

        let executor = Executor::without_reactor();
        let spawn_handle = executor.spawn_handle();

        let thread = {
            let counter = Arc::clone(&counter);

            thread::spawn(move || {
                for _ in 0..1000 {
                    let counter = Arc::clone(&counter);

                    spawn_handle.spawn(async move {
                        let _ = counter.fetch_add(1, Ordering::Relaxed);
                    });
                }
            })
        };
        executor.run(async {
            let mut prev = 0;

            while prev < 1000 {
                let val = counter.load(Ordering::Acquire);
                if val == prev {
                    futures::pending!();
                }
                prev = val;
            }
        });
        thread.join().unwrap();
        assert_eq!(counter.load(Ordering::Acquire), 1000);
    }
}
