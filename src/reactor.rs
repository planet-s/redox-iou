use std::collections::{BTreeMap, VecDeque};
use std::convert::{TryFrom, TryInto};
use std::num::NonZeroUsize;
use std::sync::atomic::{self, AtomicBool, AtomicUsize};
use std::sync::{Arc, Weak};
use std::{mem, task};

use syscall::data::IoVec;
use syscall::error::{Error, Result};
use syscall::error::{E2BIG, EBADF, ECANCELED, EINVAL, EOPNOTSUPP, EOVERFLOW};
use syscall::flag::{EventFlags, MapFlags};
use syscall::io_uring::operation::{DupFlags, FilesUpdateFlags};
use syscall::io_uring::v1::{
    CqEntry64, IoUringCqeFlags, IoUringSqeFlags, Priority, RingPopError, RingPushError, SqEntry64,
    StandardOpcode,
};
use syscall::io_uring::IoUringEnterFlags;

use crossbeam_queue::ArrayQueue;
use either::*;
use once_cell::sync::OnceCell;
use parking_lot::{
    MappedRwLockReadGuard, Mutex, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard,
};

use crate::future::{
    AtomicTag, CommandFuture, CommandFutureInner, CommandFutureRepr, FdUpdates, ProducerSqes,
    ProducerSqesState, State, Tag,
};
use crate::instance::{ConsumerInstance, ProducerInstance};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReactorId {
    inner: usize,
}

static LAST_REACTOR_ID: AtomicUsize = AtomicUsize::new(0);

/// A reactor driven by one primary `io_uring` and zero or more secondary `io_uring`s. May or may
/// not be integrated into `Executor`
#[derive(Debug)]
pub struct Reactor {
    pub(crate) id: ReactorId,

    // the primary instance - when using secondary instances, this should be a kernel-attached
    // instance, that can monitor secondary instances (typically userspace-to-userspace rings).
    // when only a single instance is used, then this instance is free to also be a
    // userspace-to-userspace ring.
    pub(crate) main_instance: ConsumerInstanceWrapper,

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
    pub(crate) secondary_instances: RwLock<SecondaryInstancesWrapper>,

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
pub(crate) struct ConsumerInstanceWrapper {
    // a convenient safe wrapper over the raw underlying interface.
    pub(crate) consumer_instance: RwLock<ConsumerInstance>,

    // stored when the ring encounters a shutdown error either when submitting an SQ, or receiving
    // a CQ.
    dropped: AtomicBool,
}
#[derive(Debug)]
pub(crate) struct ProducerInstanceWrapper {
    pub(crate) producer_instance: RwLock<ProducerInstance>,
    stream_state: Option<Arc<Mutex<ProducerSqesState>>>,
    dropped: AtomicBool,
}
#[derive(Debug)]
pub(crate) enum SecondaryInstanceWrapper {
    // Since this is a secondary instance, a userspace-to-userspace consumer.
    ConsumerInstance(ConsumerInstanceWrapper),

    // Either a kernel-to-userspace producer, or a userspace-to-userspace producer.
    ProducerInstance(ProducerInstanceWrapper),
}

impl SecondaryInstanceWrapper {
    pub(crate) fn as_consumer_instance(&self) -> Option<&ConsumerInstanceWrapper> {
        match self {
            Self::ConsumerInstance(ref instance) => Some(instance),
            Self::ProducerInstance(_) => None,
        }
    }
    pub(crate) fn as_producer_instance(&self) -> Option<&ProducerInstanceWrapper> {
        match self {
            Self::ConsumerInstance(_) => None,
            Self::ProducerInstance(ref instance) => Some(instance),
        }
    }
}

#[derive(Debug)]
pub(crate) struct SecondaryInstancesWrapper {
    pub(crate) instances: Vec<SecondaryInstanceWrapper>,
    // maps file descriptor to index within the instances
    fds_backref: BTreeMap<usize, usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingId {
    pub(crate) reactor: ReactorId,
    // 0 means main instance, a number above zero is the index of the secondary instance in the
    // vec, plus 1.
    pub(crate) inner: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SecondaryRingId {
    pub(crate) reactor: ReactorId,
    // the index into the secondary array, plus 1
    pub(crate) inner: NonZeroUsize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrimaryRingId {
    pub(crate) reactor: ReactorId,
}
impl PrimaryRingId {
    pub fn reactor(&self) -> ReactorId {
        self.reactor
    }
}
impl SecondaryRingId {
    pub fn reactor(&self) -> ReactorId {
        self.reactor
    }
}
impl RingId {
    pub fn reactor(&self) -> ReactorId {
        self.reactor
    }
    pub fn is_primary(&self) -> bool {
        self.inner == 0
    }
    pub fn is_secondary(&self) -> bool {
        self.inner > 0
    }
}

impl PartialEq<RingId> for PrimaryRingId {
    fn eq(&self, other: &RingId) -> bool {
        self.reactor == other.reactor && other.inner == 0
    }
}
impl PartialEq<RingId> for SecondaryRingId {
    fn eq(&self, other: &RingId) -> bool {
        self.reactor == other.reactor && self.inner.get() == other.inner
    }
}
impl PartialEq<PrimaryRingId> for RingId {
    fn eq(&self, other: &PrimaryRingId) -> bool {
        other == self
    }
}
impl PartialEq<SecondaryRingId> for RingId {
    fn eq(&self, other: &SecondaryRingId) -> bool {
        other == self
    }
}
impl From<PrimaryRingId> for RingId {
    fn from(primary: PrimaryRingId) -> Self {
        Self {
            reactor: primary.reactor,
            inner: 0,
        }
    }
}
impl From<SecondaryRingId> for RingId {
    fn from(secondary: SecondaryRingId) -> Self {
        Self {
            reactor: secondary.reactor,
            inner: secondary.inner.get(),
        }
    }
}

/// A builder that configures the reactor.
pub struct ReactorBuilder {
    trusted_instance: bool,
    primary_instance: Option<ConsumerInstance>,
}

impl ReactorBuilder {
    /// Create an executor builder with the default options.
    pub const fn new() -> Self {
        Self {
            trusted_instance: false,
            primary_instance: None,
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
    /// Finalize the reactor, using the options that have been specified here.
    ///
    /// # Panics
    /// This function will panic if the primary instance has not been set.
    ///
    pub fn build(self) -> Arc<Reactor> {
        let primary_instance = self.primary_instance.expect("expected");
        Reactor::new(primary_instance, self.trusted_instance)
    }
}
impl Reactor {
    fn new(main_instance: ConsumerInstance, trusted_main_instance: bool) -> Arc<Self> {
        let main_instance = ConsumerInstanceWrapper {
            consumer_instance: RwLock::new(main_instance),
            dropped: AtomicBool::new(false),
        };

        let reactor_arc = Arc::new(Reactor {
            id: ReactorId {
                inner: LAST_REACTOR_ID.fetch_add(1, atomic::Ordering::Relaxed),
            },
            main_instance,
            trusted_main_instance,
            secondary_instances: RwLock::new(SecondaryInstancesWrapper {
                instances: Vec::new(),
                fds_backref: BTreeMap::new(),
            }),

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
    pub fn primary_instance(&self) -> PrimaryRingId {
        PrimaryRingId { reactor: self.id }
    }
    /// Obtain a handle to this reactor, capable of creating futures that use it.
    pub fn handle(&self) -> Handle {
        Handle {
            reactor: Weak::clone(self.weak_ref.get().unwrap()),
        }
    }
    /// Add an additional secondary instance to the reactor, waking up the executor to include it
    /// if necessary. If the main SQ is full, this will fail with ENOSPC (TODO: fix this).
    pub fn add_secondary_instance(
        &self,
        instance: ConsumerInstance,
        priority: Priority,
    ) -> Result<SecondaryRingId> {
        let ringfd = instance.ringfd();
        self.add_secondary_instance_generic(
            SecondaryInstanceWrapper::ConsumerInstance(ConsumerInstanceWrapper {
                consumer_instance: RwLock::new(instance),
                dropped: AtomicBool::new(false),
            }),
            ringfd,
            priority,
        )
    }
    fn add_secondary_instance_generic(
        &self,
        instance: SecondaryInstanceWrapper,
        ringfd: usize,
        priority: Priority,
    ) -> Result<SecondaryRingId> {
        let mut guard = self.secondary_instances.write();

        // Tell the kernel to send us speciel event CQEs which indicate that other io_urings have
        // received additional entries, which is what actually allows secondary instances to make
        // progress.
        {
            let fd64 = ringfd.try_into().or(Err(Error::new(EBADF)))?;

            self.main_instance
                .consumer_instance
                .write()
                .sender_mut()
                .as_64_mut()
                .expect("expected SqEntry64")
                .try_send(SqEntry64 {
                    opcode: StandardOpcode::FilesUpdate as u8,
                    priority,
                    flags: IoUringSqeFlags::SUBSCRIBE.bits(),
                    // not used since the driver will know that it's an io_uring being updated
                    user_data: 0,

                    syscall_flags: (FilesUpdateFlags::READ | FilesUpdateFlags::IO_URING).bits(),
                    addr: 0, // unused
                    fd: fd64,
                    offset: 0, // unused
                    len: 0,    // unused

                    additional1: 0,
                    additional2: 0,
                })?;
        }

        let instances_len = guard.instances.len();

        guard.fds_backref.insert(ringfd, instances_len);
        guard.instances.push(instance);

        Ok(SecondaryRingId {
            reactor: self.id,
            inner: NonZeroUsize::new(guard.instances.len()).unwrap(),
        })
    }
    pub fn add_producer_instance(
        &self,
        instance: ProducerInstance,
        priority: Priority,
    ) -> Result<SecondaryRingId> {
        let ringfd = instance.ringfd();
        self.add_secondary_instance_generic(
            SecondaryInstanceWrapper::ProducerInstance(ProducerInstanceWrapper {
                producer_instance: RwLock::new(instance),
                dropped: AtomicBool::new(false),
                stream_state: None,
            }),
            ringfd,
            priority,
        )
    }
    pub fn id(&self) -> ReactorId {
        self.id
    }
    pub(crate) fn drive_primary(&self, waker: &task::Waker, wait: bool) {
        self.drive(&self.main_instance, waker, wait, true)
    }
    fn drive(&self, instance: &ConsumerInstanceWrapper, waker: &task::Waker, wait: bool, primary: bool) {
        let a = if wait {
            let read_guard = instance.consumer_instance.read();
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
            log::debug!("Entering io_uring");
            Some(
                read_guard
                    .wait(0, flags)
                    .expect("redox_iou: failed to enter io_uring"),
            )
        } else {
            None
        };
        log::debug!("Entered io_uring with a {:?}", a);

        let mut intent_guard = instance.consumer_instance.upgradable_read();

        let ring_header = unsafe { intent_guard.receiver().as_64().unwrap().ring_header() };
        let available_completions = ring_header.available_entry_count_spsc();

        log::debug!("Available completions: {}", available_completions);

        if a.unwrap_or(0) > available_completions {
            log::warn!("The kernel/other process gave us a higher number of available completions than present on the ring.");
        }

        for i in 0..available_completions {
            let mut write_guard = RwLockUpgradableReadGuard::upgrade(intent_guard);
            let result = write_guard
                .receiver_mut()
                .as_64_mut()
                .expect("expected 64-bit CQEs")
                .try_recv();
            intent_guard = RwLockWriteGuard::downgrade_to_upgradable(write_guard);

            match result {
                Ok(cqe) => {
                    log::debug!("Received CQE: {:?}", cqe);
                    if IoUringCqeFlags::from_bits_truncate((cqe.flags & 0xFF) as u8).contains(IoUringCqeFlags::EVENT) && EventFlags::from_bits_truncate((cqe.flags >> 8) as usize).contains(EventFlags::EVENT_IO_URING) {
                        // if this was an event, that was tagged io_uring, we can assume that the
                        // event came from the kernel having polled some secondary io_urings. We'll
                        // then drive those instances and wakeup futures.

                        let fd64 = match Error::demux64(cqe.status) {
                            Ok(fd64) => fd64,
                            Err(error) => {
                                log::warn!("Error on receiving an event about secondary io_uring progress: {}. Ignoring event.", error);
                                continue;
                            }
                        };

                        let fd = match usize::try_from(fd64) {
                            Ok(fd) => fd,
                            Err(_) => {
                                log::warn!("The kernel gave us a CQE with a status that was too large to fit a system-wide file descriptor ({} > {}). Ignoring event.", cqe.status, usize::max_value());
                                continue;
                            }
                        };

                        let secondary_instances_guard = self.secondary_instances.read();

                        let secondary_instance_index = match secondary_instances_guard.fds_backref.get(&fd) {
                            Some(idx) => *idx,
                            None => {
                                log::warn!("The fd ({}) meant to describe the instance to drive, was not recognized. Ignoring event.", fd);
                                continue;
                            }
                        };
                        match secondary_instances_guard.instances
                            .get(secondary_instance_index)
                            .expect("fd backref BTreeMap corrupt, contains a file descriptor that was removed")
                        {
                            SecondaryInstanceWrapper::ConsumerInstance(ref instance) => self.drive(instance, waker, false, false),
                            SecondaryInstanceWrapper::ProducerInstance(ref instance) => self.drive_producer_instance(&instance),
                        }

                    } else {
                        let _ = Self::handle_cqe(self.trusted_main_instance && primary, self.tag_map.read(), waker, cqe);
                    }

                }
                Err(RingPopError::Empty { .. }) => panic!("the kernel gave us a higher number of available completions than actually available (at {}/{})", i, available_completions),
                Err(RingPopError::Shutdown) => { instance.dropped.store(true, std::sync::atomic::Ordering::Release); break },
            }
        }
    }
    fn drive_producer_instance(&self, instance: &ProducerInstanceWrapper) {
        log::debug!("Event was an external producer io_uring, thus polling the ring itself");
        loop {
            assert!(self.trusted_main_instance);

            let state_lock = match instance.stream_state {
                Some(ref s) => s,
                None => return,
            };
            let mut state_guard = state_lock.lock();
            log::debug!("Driving state: {:?}", &*state_guard);

            match *state_guard {
                // TODO: Since ProducerSqes only supports one stream per producer instance, as it
                // does nothing but receiving SQEs from that ring with proper notification, this
                // could happen inside the future instead. Actually, the future needs not contain
                // any state arc at all, and could consist of nothing but a waker.
                ProducerSqesState::Receiving {
                    ref mut deque,
                    capacity,
                    waker: ref mut future_waker,
                } => {
                    if deque.len() < capacity {
                        let mut guard = instance.producer_instance.write();
                        let sqe = match guard.receiver_mut().as_64_mut().unwrap().try_recv() {
                            Ok(sqe) => sqe,
                            Err(RingPopError::Empty { .. }) => break,
                            Err(RingPopError::Shutdown) => {
                                log::debug!("Secondary producer ring dropped");
                                instance
                                    .dropped
                                    .store(true, std::sync::atomic::Ordering::Release);
                                *state_guard = ProducerSqesState::Finished;
                                break;
                            }
                        };
                        log::info!("Secondary producer SQE: {:?}", sqe);
                        deque.push_back(sqe);
                        if let Some(future_waker) = future_waker.take() {
                            future_waker.wake();
                        }
                        log::debug!("New driving state: {:?}", &*state_guard);
                    } else {
                        // The future doesn't want more SQEs, so we'll avoid the push and let the
                        // consumer of this producer, encounter a full ring instead, forming a very
                        // basic sort of congestion control.
                        log::debug!("Ignoring state");
                    }
                }
                ProducerSqesState::Finished => break,
                ProducerSqesState::Cancelled => break,
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
    pub(crate) fn instance(
        &self,
        ring: impl Into<RingId>,
    ) -> Option<Either<&RwLock<ConsumerInstance>, MappedRwLockReadGuard<RwLock<ConsumerInstance>>>>
    {
        let ring = ring.into();

        if ring.reactor() != self.id() {
            panic!(
                "Using a reactor id from another reactor to get an instance: {:?} is not from {:?}",
                ring,
                self.id()
            );
        }

        if ring == self.primary_instance() {
            Some(Left(&self.main_instance.consumer_instance))
        } else {
            RwLockReadGuard::try_map(self.secondary_instances.read(), |instances| {
                instances.instances[ring.inner - 1]
                    .as_consumer_instance()
                    .map(|i| &i.consumer_instance)
            })
            .ok()
            .map(Right)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SubmissionSync {
    NoSync,
    Drain,
    Chain,
}
impl Default for SubmissionSync {
    fn default() -> Self {
        Self::NoSync
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct SubmissionContext {
    priority: Priority,
    sync: SubmissionSync,
}
impl SubmissionContext {
    pub fn new() -> Self {
        Self::default()
    }
    pub const fn with_priority(self, priority: Priority) -> Self {
        Self {
            priority,
            .. self
        }
    }
    pub const fn priority(&self) -> Priority {
        self.priority
    }
    pub fn set_priority(&mut self, priority: Priority) {
        self.priority = priority;
    }
    pub const fn with_sync(self, sync: SubmissionSync) -> Self {
        Self {
            sync,
            .. self
        }
    }
    pub const fn sync(&self) -> SubmissionSync {
        self.sync
    }
    pub fn set_sync(&mut self, sync: SubmissionSync) {
        self.sync = sync;
    }
}

pub struct UnsafeSubmissionContext {
    #[cfg(feature = "buffer_pool")]
    guard: Option<crate::memory::CommandFutureGuard>,

    context: SubmissionContext,
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
    pub unsafe fn send(&self, ring: impl Into<RingId>, sqe: SqEntry64) -> CommandFuture {
        self.send_inner(ring, sqe, false)
            .left()
            .expect("send_inner() must return CommandFuture if is_stream is set to false")
    }
    unsafe fn send_inner(
        &self,
        ring: impl Into<RingId>,
        sqe: SqEntry64,
        is_stream: bool,
    ) -> Either<CommandFuture, FdUpdates> {
        let ring = ring.into();

        let reactor = self
            .reactor
            .upgrade()
            .expect("failed to initiate new command: reactor is dead");

        assert_eq!(ring.reactor, reactor.id());

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

                if reactor.trusted_main_instance && ring.is_primary() {
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

                if reactor.trusted_main_instance && ring.is_primary() {
                    (None, Some(state_arc))
                } else {
                    reactor.tag_map.write().insert(n, state_arc);
                    (Some(n), None)
                }
            }
        };

        let inner = CommandFutureInner {
            ring,
            reactor: Weak::clone(&self.reactor),
            repr: if reactor.trusted_main_instance && ring.is_primary() {
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
    pub fn send_producer_cqe(
        &self,
        instance: SecondaryRingId,
        cqe: CqEntry64,
    ) -> Result<(), RingPushError<CqEntry64>> {
        let reactor = self
            .reactor
            .upgrade()
            .expect("failed to send producer CQE: reactor is dead");

        assert_eq!(reactor.id(), instance.reactor);

        let guard = reactor.secondary_instances.read();

        let producer_instance = match guard
            .instances
            .get(instance.inner.get() - 1)
            .expect("invalid SecondaryRingId: non-existent instance")
        {
            SecondaryInstanceWrapper::ProducerInstance(ref instance) => instance,
            SecondaryInstanceWrapper::ConsumerInstance(_) => {
                panic!("cannot send producer CQE using a consumer instance")
            }
        };
        let mut producer_instance_guard = producer_instance.producer_instance.write();
        match producer_instance_guard
            .sender_mut()
            .as_64_mut()
            .unwrap()
            .try_send(cqe)
        {
            Ok(()) => Ok(()),
            Err(RingPushError::Full(_)) => Err(RingPushError::Full(cqe)),
            Err(RingPushError::Shutdown(_)) => {
                producer_instance
                    .dropped
                    .store(true, atomic::Ordering::Release);
                Err(RingPushError::Shutdown(cqe))
            }
        }
    }
    pub fn producer_sqes(&self, ring_id: SecondaryRingId, capacity: usize) -> ProducerSqes {
        let reactor = self
            .reactor
            .upgrade()
            .expect("failed to send producer CQE: reactor is dead");

        assert_eq!(reactor.id(), ring_id.reactor);
        assert!(reactor.trusted_main_instance);

        let secondary_instances = reactor.secondary_instances.upgradable_read();

        let state_opt = match secondary_instances
            .instances
            .get(ring_id.inner.get() - 1)
            .unwrap()
        {
            SecondaryInstanceWrapper::ConsumerInstance(_) => {
                panic!("calling producer_sqes on a consumer instance")
            }
            SecondaryInstanceWrapper::ProducerInstance(ref instance) => {
                instance.stream_state.clone()
            }
        };
        // TODO: Override capacity if the stream state already is present.

        let state = match state_opt {
            Some(st) => st,
            None => {
                let mut secondary_instances =
                    RwLockUpgradableReadGuard::upgrade(secondary_instances);
                match secondary_instances
                    .instances
                    .get_mut(ring_id.inner.get() - 1)
                    .unwrap()
                {
                    SecondaryInstanceWrapper::ConsumerInstance(_) => unreachable!(),
                    SecondaryInstanceWrapper::ProducerInstance(ref mut instance) => {
                        let new_state = Arc::new(Mutex::new(ProducerSqesState::Receiving {
                            capacity,
                            deque: VecDeque::with_capacity(capacity),
                            waker: None,
                        }));

                        instance.stream_state = Some(Arc::clone(&new_state));
                        new_state
                    }
                }
            }
        };

        ProducerSqes { state }
    }

    /// Create an asynchronous stream that represents the events coming from one of more file
    /// descriptors that are triggered when e.g. the file has changed, or is capable of reading new
    /// data, etc.
    pub fn subscribe_to_fd_updates(
        &self,
        ring: impl Into<RingId>,
        fd: usize,
        event_flags: EventFlags,
        oneshot: bool,
    ) -> FdUpdates {
        assert!(!event_flags.contains(EventFlags::EVENT_IO_URING), "only the redox_iou reactor is allowed to use this flag unless io_uring API is used directly");
        let sqe = SqEntry64::new(
            IoUringSqeFlags::SUBSCRIBE,
            Priority::default(),
            (-1i64) as u64,
        )
        .file_update(fd.try_into().unwrap(), event_flags, oneshot);
        unsafe {
            self.send_inner(ring, sqe, true)
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
    async unsafe fn rw_io<F>(&self, ring: impl Into<RingId>, fd: usize, f: F) -> Result<usize>
    where
        F: FnOnce(SqEntry64, u64) -> SqEntry64,
    {
        let fd: u64 = fd.try_into().or(Err(Error::new(EOVERFLOW)))?;

        let base_sqe = SqEntry64::new(
            IoUringSqeFlags::empty(),
            Priority::default(),
            (-1i64) as u64,
        );
        let sqe = f(base_sqe, fd);

        let cqe = self.send(ring, sqe).await?;
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
        ring: impl Into<RingId>,
        path: &B,
        flags: u64,
    ) -> Result<usize> {
        let sqe = SqEntry64::new(
            IoUringSqeFlags::empty(),
            Priority::default(),
            (-1i64) as u64,
        )
        .open(path.as_ref(), flags);
        let cqe = self.send(ring, sqe).await?;
        Self::completion_as_rw_io_result(cqe)
    }
    pub async fn open_raw_static<B: AsRef<[u8]> + ?Sized + 'static>(
        &self,
        ring: impl Into<RingId>,
        path: &'static B,
        flags: u64,
    ) -> Result<usize> {
        unsafe { self.open_raw(ring, path, flags) }.await
    }
    pub async fn open_raw_move_buf(
        &self,
        ring: impl Into<RingId>,
        path: Vec<u8>,
        flags: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let fd = unsafe { self.open_raw(ring, &*path, flags) }.await?;
        Ok((fd, path))
    }
    /// Open a path, returning a new file descriptor for the file specified by that path.
    ///
    /// # Safety
    ///
    /// For this to be safe, the path buffer that is used by the path, _must_ outlive the execution
    /// of this future, and the buffer must not be reclaimed until completion or cancellation.
    pub async unsafe fn open<S: AsRef<str> + ?Sized>(
        &self,
        ring: impl Into<RingId>,
        path: &S,
        flags: u64,
    ) -> Result<usize> {
        self.open_raw(ring, path.as_ref().as_bytes(), flags).await
    }
    pub async fn open_static<S: AsRef<str> + ?Sized + 'static>(
        &self,
        ring: impl Into<RingId>,
        path: &'static S,
        flags: u64,
    ) -> Result<usize> {
        unsafe { self.open_raw(ring, path.as_ref().as_bytes(), flags) }.await
    }
    pub async fn open_move_buf(
        &self,
        ring: impl Into<RingId>,
        path: String,
        flags: u64,
    ) -> Result<(usize, String)> {
        let fd = unsafe { self.open_raw(ring, path.as_str().as_bytes(), flags) }.await?;
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
    pub async unsafe fn close(
        &self,
        ring: impl Into<RingId>,
        fd: usize,
        flush: bool,
    ) -> Result<()> {
        let sqe = SqEntry64::new(
            IoUringSqeFlags::empty(),
            Priority::default(),
            (-1i64) as u64,
        )
        .close(fd.try_into().or(Err(Error::new(EOVERFLOW)))?, flush);
        let cqe = self.send(ring, sqe).await?;

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
        ring: impl Into<RingId>,
        range: std::ops::Range<usize>,
        flush: bool,
    ) -> Result<()> {
        let start: u64 = range.start.try_into().or(Err(Error::new(EOVERFLOW)))?;
        let end: u64 = range.end.try_into().or(Err(Error::new(EOVERFLOW)))?;
        let count = end.checked_sub(start).ok_or(Error::new(EINVAL))?;

        let sqe = SqEntry64::new(
            IoUringSqeFlags::empty(),
            Priority::default(),
            (-1i64) as u64,
        )
        .close_many(start, count, flush);
        let cqe = self.send(ring, sqe).await?;

        Self::completion_as_rw_io_result(cqe)?;

        Ok(())
    }

    /// Read bytes.
    ///
    /// # Safety
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn read(
        &self,
        ring: impl Into<RingId>,
        fd: usize,
        buf: &mut [u8],
    ) -> Result<usize> {
        self.rw_io(ring, fd, |sqe, fd| sqe.read(fd, buf)).await
    }
    /// Read bytes, vectored.
    ///
    /// # Safety
    /// The caller must ensure that the buffers outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn readv(
        &self,
        ring: impl Into<RingId>,
        fd: usize,
        bufs: &[IoVec],
    ) -> Result<usize> {
        self.rw_io(ring, fd, |sqe, fd| sqe.readv(fd, bufs)).await
    }

    /// Read bytes from a specific offset. Does not change the file offset.
    ///
    /// # Safety
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn pread(
        &self,
        ring: impl Into<RingId>,
        fd: usize,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize> {
        self.rw_io(ring, fd, |sqe, fd| sqe.pread(fd, buf, offset))
            .await
    }

    /// Read bytes from a specific offset, vectored. Does not change the file offset.
    ///
    /// # Safety
    /// The caller must ensure that the buffers outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn preadv(
        &self,
        ring: impl Into<RingId>,
        fd: usize,
        bufs: &[IoVec],
        offset: u64,
    ) -> Result<usize> {
        self.rw_io(ring, fd, |sqe, fd| sqe.preadv(fd, bufs, offset))
            .await
    }

    /// Write bytes.
    ///
    /// # Safety
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn write(
        &self,
        ring: impl Into<RingId>,
        fd: usize,
        buf: &[u8],
    ) -> Result<usize> {
        self.rw_io(ring, fd, |sqe, fd| sqe.write(fd, buf)).await
    }

    /// Write bytes, vectored.
    ///
    /// # Safety
    /// The caller must ensure that the buffers outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn writev(
        &self,
        ring: impl Into<RingId>,
        fd: usize,
        bufs: &[IoVec],
    ) -> Result<usize> {
        self.rw_io(ring, fd, |sqe, fd| sqe.writev(fd, bufs)).await
    }

    /// Write bytes to a specific offset. Does not change the file offset.
    ///
    /// # Safety
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn pwrite(
        &self,
        ring: impl Into<RingId>,
        fd: usize,
        buf: &[u8],
        offset: u64,
    ) -> Result<usize> {
        self.rw_io(ring, fd, |sqe, fd| sqe.pwrite(fd, buf, offset))
            .await
    }
    /// Write bytes to a specific offset, vectored. Does not change the file offset.
    ///
    /// # Safety
    /// The caller must ensure that the buffers outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn pwritev(
        &self,
        ring: impl Into<RingId>,
        fd: usize,
        bufs: &[IoVec],
        offset: u64,
    ) -> Result<usize> {
        self.rw_io(ring, fd, |sqe, fd| sqe.pwritev(fd, bufs, offset))
            .await
    }

    /// "Duplicate" a file descriptor, returning a new one based on the old one.
    ///
    /// # Panics
    /// This function will panic if the parameter is set, but the flags don't contain
    /// [`DupFlags::PARAM`].
    ///
    /// # Safety
    ///
    /// If the parameter is used, that must point to a slice that is valid for the receiver.
    /// Additionally, that slice must also outlive the lifetime of this future, and if the future
    /// is dropped or forgotten, the slice must not be used afterwards, since that would lead to a
    /// data race.
    pub async unsafe fn dup(
        &self,
        ring: impl Into<RingId>,
        fd: usize,
        flags: DupFlags,
        param: Option<&[u8]>,
    ) -> Result<usize> {
        let fd64 = u64::try_from(fd).or(Err(Error::new(EBADF)))?;

        let cqe = self
            .send(
                ring,
                SqEntry64::new(
                    IoUringSqeFlags::empty(),
                    Priority::default(),
                    (-1i64) as u64,
                )
                .dup(fd64, flags, param),
            )
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
        ring: impl Into<RingId>,
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
            .send(
                ring,
                SqEntry64::new(IoUringSqeFlags::empty(), Priority::default(), 0).mmap(
                    fd64,
                    flags,
                    addr_hint64,
                    len64,
                    offset,
                ),
            )
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
        ring: impl Into<RingId>,
        fd: usize,
        flags: MapFlags,
        len: usize,
        offset: u64,
    ) -> Result<*const ()> {
        assert!(!flags.contains(MapFlags::MAP_FIXED));
        self.mmap2(ring, fd, flags, None, len, offset).await
    }
}
