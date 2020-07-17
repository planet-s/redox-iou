use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{task, thread};

use crossbeam_queue::SegQueue;
use parking_lot::{Mutex, RwLock};

use crate::instance::ConsumerGenericSender;
use crate::reactor::{Handle as ReactorHandle, Reactor};

/// A minimal executor, that does not use any thread pool.
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

    runqueue: Arc<Runqueue>,
}
struct Runqueue {
    ready_futures: SegQueue<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    pending_futures: Mutex<BTreeMap<usize, Pin<Box<dyn Future<Output = ()> + Send + 'static>>>>,
    next_pending_tag: AtomicUsize,
}

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

    ///
    /// Create an executor that includes an integrated reactor. This is generally more efficient
    /// than offloading the reactor to another thread, provided that the futures can drive the
    /// ring themselves when polled, since the kernel can immediately wake up the futures once new
    /// completion entries are attainable. The executor can also be woken up by other threads
    /// directly, by incrementing the pop epoch of the main ring, which will trick the kernel into
    /// thinking that new entries are available.
    ///
    pub fn with_reactor(reactor_arc: Arc<Reactor>) -> Self {
        let mut this = Self::without_reactor();

        this.reactor = Some(ReactorWrapper {
            driving_waker: Self::driving_waker(&reactor_arc, None),
            reactor: reactor_arc,
        });
        this
    }

    fn driving_waker(reactor: &Arc<Reactor>, runqueue: Option<(Weak<Runqueue>, usize)>) -> task::Waker {
        let reactor = Arc::downgrade(reactor);

        async_task::waker_fn(move || {
            if let Some((runqueue, tag)) = runqueue.as_ref().and_then(|(rq, tag)| Some((rq.upgrade()?, tag))) {
                let removed = runqueue.pending_futures.lock().remove(&tag);

                match removed {
                    Some(pending) => runqueue.ready_futures.push(pending),
                    None => return,
                }
            }

            let reactor = reactor
                .upgrade()
                .expect("failed to wake up executor: integrated reactor dead");

            let instance_guard = reactor.main_instance.consumer_instance.read();

            match instance_guard.sender() {
                ConsumerGenericSender::Bits32(ref sender32) => sender32.notify(),
                ConsumerGenericSender::Bits64(ref sender64) => sender64.notify(),
            }
        })
    }
    fn standard_waker(standard_waker_thread: &Arc<RwLock<thread::Thread>>, runqueue: Option<(Weak<Runqueue>, usize)>) -> task::Waker {
        let standard_waker_thread = Arc::downgrade(standard_waker_thread);

        async_task::waker_fn(move || {
            if let Some((runqueue, tag)) = runqueue.as_ref().and_then(|(rq, tag)| Some((rq.upgrade()?, tag))) {
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

        loop {
            let pinned_future = unsafe { Pin::new_unchecked(&mut future) };
            match pinned_future.poll(&mut cx) {
                task::Poll::Ready(o) => return o,

                task::Poll::Pending => {
                    if let Some(reactor_wrapper) = self.reactor.as_ref() {
                        reactor_wrapper.reactor.drive(&waker);
                        self.poll_spawned_futures()
                    } else {
                        self.poll_spawned_futures();
                        thread::park();
                    }
                }
            }
        }
    }
    pub fn poll_spawned_futures(&self) {
        while let Ok(ready_future) = self.runqueue.ready_futures.pop() {
            let mut ready_future = ready_future;
            let pinned = ready_future.as_mut();

            let tag = self.runqueue.next_pending_tag.fetch_add(1, Ordering::Relaxed);

            let secondary_waker = if let Some(reactor) = self.reactor.as_ref() {
                Self::driving_waker(&reactor.reactor, Some((Arc::downgrade(&self.runqueue), tag)))
            } else {
                Self::standard_waker(&self.standard_waker_thread, Some((Arc::downgrade(&self.runqueue), tag)))
            };

            let mut cx = task::Context::from_waker(&secondary_waker);

            match Future::poll(pinned, &mut cx) {
                task::Poll::Ready(()) => (),
                task::Poll::Pending => {
                    self.runqueue.pending_futures.lock().insert(tag, ready_future);
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
pub struct SpawnHandle {
    runqueue: Weak<Runqueue>,
    waker: task::Waker,
}
impl SpawnHandle {
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let runqueue = self.runqueue
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
