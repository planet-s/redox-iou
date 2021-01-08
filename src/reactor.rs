/// # The `redox-iou` reactor.
///
/// This reactor is based on one or more `io_uring`s; either, a userspace-to-userspace ring is
/// used, or a userspace-to-kernel ring is used, together with zero or more additional secondary
/// userspace-to-userspace or kernel-to-userspace rings.
///
/// The reactor will poll the rings by trying to pop entries from it. If there are no available
/// entries to pop, it will invoke `SYS_ENTER_IORING` on the main ring file descriptor, causing the
/// current thread to halt, until the kernel wakes it up when new entries have been pushed, or when
/// a previously full ring has had entries popped from it. Other threads can also wake up the
/// reactor, and hence the executor in case the reactor is integrated, by incrementing the epoch
/// count of the main ring, followed by a `SYS_ENTER_IORING` syscall.
use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::mem::ManuallyDrop;
use std::sync::atomic::{self, AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::{ops, task};

#[cfg(target_os = "linux")]
use {
    crate::linux::{ReadFlags, WriteFlags},
    ioprio::Priority,
};

use syscall::data::IoVec;
use syscall::error::{Error, Result, EOVERFLOW};
#[cfg(target_os = "redox")]
use {
    crate::{future::ProducerSqesState, redox::instance::ConsumerInstance},
    parking_lot::{RwLockUpgradableReadGuard, RwLockWriteGuard},
    syscall::{
        error::{E2BIG, EBADF, ECANCELED, EFAULT, EINVAL, EIO, EOPNOTSUPP},
        io_uring::{
            v1::{
                operation::{OpenFlags, ReadFlags, RegisterEventsFlags, WriteFlags},
                BrokenRing, IoUringCqeFlags, Priority, RingPopError, SqEntry64, StandardOpcode,
            },
            IoUringEnterFlags,
        },
    },
};
#[cfg(any(doc, target_os = "redox"))]
use {
    crate::{
        future::{FdEventsInitial, ProducerSqes},
        redox::instance::ProducerInstance,
    },
    std::collections::VecDeque,
    syscall::{
        flag::{EventFlags, MapFlags},
        io_uring::v1::{
            operation::{CloseFlags, DupFlags},
            CqEntry64, IoUringSqeFlags, RingPushError,
        },
    },
};

// TODO: Fix ConsumerInstance conflict.
#[cfg(any(doc, target_os = "linux"))]
use crate::linux::ConsumerInstance;

use syscall::io_uring::{GenericSlice, GenericSliceMut};

use crossbeam_queue::ArrayQueue;
use either::*;
use parking_lot::{MappedRwLockReadGuard, Mutex, RwLock, RwLockReadGuard};

use crate::future::{
    AtomicTag, CommandFuture, CommandFutureInner, CommandFutureRepr, FdEvents, State, StateInner,
    Tag,
};

use crate::executor::Runqueue;

use crate::memory::{Guarded, GuardedMut};

#[cfg(target_os = "linux")]
const OFFSET_TREAT_AS_POSITIONED: u64 = (-1_i64) as u64;

/// A unique ID that every reactor gets upon initialization.
///
/// This type implements various traits that allow the ID to be checked against other IDs, compared
/// (reactors created later will have larger IDs), and hashed.
#[derive(Clone, Copy, Debug, Hash, Ord, Eq, PartialEq, PartialOrd)]
pub struct ReactorId {
    pub(crate) inner: usize,
}

static LAST_REACTOR_ID: AtomicUsize = AtomicUsize::new(0);

/// A reactor driven by primary `io_uring`s and zero or more secondary `io_uring`s. May or may not
/// be integrated into `Executor`
#[derive(Debug)]
pub struct Reactor {
    pub(crate) id: ReactorId,

    // The primary instances - these are the io_urings that are actually entered when waiting for
    // I/O in the reactor. There will typically only exist one such instance per OS thread,
    // allowing for parallelism even in the kernel.
    //
    // For Redox, when using secondary instances, these primary instances should be kernel-attached
    // instances, that can monitor secondary instances (typically userspace-to-userspace rings).
    // When no secondary instances are used, then these instances are free to also be a
    // userspace-to-userspace ring.
    pub(crate) main_instances: Vec<ConsumerInstanceWrapper>,

    // The secondary instances, which are typically userspace-to-userspace, for schemes I/O or IPC.
    // These are not blocked on using the `SYS_ENTER_IORING` syscall; instead, they use
    // RegisterEvents on the main instance (which __must__ be attached to the kernel for secondary
    // instances to exist whatsoever), and then pops the entries of that ring separately, precisely
    // like with the primary ring.
    #[cfg(target_os = "redox")]
    pub(crate) secondary_instances: RwLock<SecondaryInstancesWrapper>,

    // TODO: ConcurrentBTreeMap - I (4lDO2) am currently writing this.

    // A map between integer tags and internal future state. This map is only used for untrusted
    // secondary instances, and for main instances if `trusted_main_instance` is false.
    pub(crate) tag_map: RwLock<BTreeMap<Tag, Arc<Mutex<State>>>>,

    // The next tag to use, retrieved with `fetch_add(1, Ordering::Relaxed)`. If the value has
    // overflown, `tag_has_overflown` will be set, and further tags must be checked so that no tag
    // is accidentally replaced. this limit will probably _never_ be encountered on a 64-bit
    // system, but on a 32-bit system it might happen.
    //
    // TODO: 64-bit tags?
    next_tag: AtomicTag,

    // An atomic queue that is used for Arc reclamation of `State`s, preventing unnecessary load on
    // the global allocator when we can use a pool of allocations instead.
    reusable_tags: ArrayQueue<(Tag, Arc<Mutex<State>>)>,

    // This is a weak backref to the reactor itself, allowing handles to be obtained.
    weak_ref: Option<Weak<Reactor>>,
}

#[derive(Debug)]
pub(crate) struct ConsumerInstanceWrapper {
    // A convenient safe wrapper over the raw underlying interface.
    pub(crate) consumer_instance: ConsumerInstance,

    // Distinguishes "trusted instances" from "non-trusted" instances. The major difference between
    // these two, is that a non-trusted instance will use a map to associate integer tags with the
    // future states. Meanwhile, a trusted instance will put the a Weak::into_raw pointer in the
    // user_data field, and then call Weak::from_raw to wake up the executor (which hopefully is
    // this one). This is because we most likely don't want a user process modifying our own
    // pointers!
    #[cfg(target_os = "redox")]
    pub(crate) trusted: bool,

    // TODO: CMPXCHG16B (or CMPXCHG8B on i386) - we need this to be atomic!
    #[cfg(target_os = "linux")]
    pub(crate) current_threadid: RwLock<Option<libc::pthread_t>>,

    // Stored when the ring encounters a shutdown error either when submitting an SQE, or receiving
    // a CQE.
    pub(crate) dropped: AtomicBool,
}
#[cfg(target_os = "redox")]
#[derive(Debug)]
pub(crate) struct ProducerInstanceWrapper {
    pub(crate) producer_instance: ProducerInstance,
    stream_state: Option<Arc<Mutex<ProducerSqesState>>>,
    dropped: AtomicBool,
}
#[cfg(target_os = "redox")]
#[derive(Debug)]
pub(crate) struct SecondaryInstancesWrapper {
    pub(crate) consumer_instances: Vec<ConsumerInstanceWrapper>,
    pub(crate) producer_instances: Vec<ProducerInstanceWrapper>,
    // maps file descriptor to index within the instances
    fds_backref: BTreeMap<usize, (usize, BackrefTy)>,
}
#[cfg(target_os = "redox")]
#[derive(Clone, Copy, Debug)]
enum BackrefTy {
    Consumer,
    Producer,
}

/// An ID that can uniquely identify the reactor that uses a ring, as well as the ring within that
/// reactor itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingId {
    pub(crate) reactor_id: ReactorId,
    pub(crate) index: usize,
    pub(crate) ty: RingTy,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RingTy {
    Primary,
    #[cfg(target_os = "redox")]
    Secondary,
    #[cfg(target_os = "redox")]
    Producer,
}
/// A ring ID that is guaranteed to be the primary ring of a reactor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrimaryRingId {
    pub(crate) reactor_id: ReactorId,
    pub(crate) index: usize,
}
/// A ring ID that is guaranteed to be a secondary ring of a reactor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
pub struct SecondaryRingId {
    pub(crate) reactor_id: ReactorId,
    pub(crate) index: usize,
}
/// A ring ID that is guaranteed to be a producer ring of a reactor.
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProducerRingId {
    pub(crate) reactor_id: ReactorId,
    pub(crate) index: usize,
}
impl PrimaryRingId {
    /// Get the unique reactor ID using this ring.
    #[inline]
    pub fn reactor_id(&self) -> ReactorId {
        self.reactor_id
    }
}
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
impl SecondaryRingId {
    /// Get the unique reactor ID using this ring.
    #[inline]
    pub fn reactor_id(&self) -> ReactorId {
        self.reactor_id
    }
}
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
impl ProducerRingId {
    /// Get the unique reactor ID using this ring.
    #[inline]
    pub fn reactor_id(&self) -> ReactorId {
        self.reactor_id
    }
}
impl RingId {
    /// Get an ID that can uniquely identify the reactor that uses this ring.
    #[inline]
    pub fn reactor(&self) -> ReactorId {
        self.reactor_id
    }
    /// Check whether the ring is the primary ring.
    #[inline]
    pub fn is_primary(&self) -> bool {
        self.ty == RingTy::Primary
    }
    /// Check whether the ring is a secondary ring (a userspace-to-userspace controlled by a main
    /// userspace-to-kernel ring).
    #[inline]
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub fn is_secondary(&self) -> bool {
        self.ty == RingTy::Secondary
    }
    /// Check whether the ring is a producer ring (which is a kernel-to-userspace or the producer
    /// part of a userspace-to-userspace ring, typically controlled by a main ring).
    #[inline]
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub fn is_producer(&self) -> bool {
        self.ty == RingTy::Producer
    }
    /// Attempt to convert this generic ring ID into a primary ring ID, if it represents one.
    #[inline]
    pub fn try_into_primary(&self) -> Option<PrimaryRingId> {
        if self.is_primary() {
            Some(PrimaryRingId {
                reactor_id: self.reactor_id,
                index: self.index,
            })
        } else {
            None
        }
    }
    /// Attempt to convert this generic ring ID into a secondary ring ID, if it represents one.
    #[inline]
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub fn try_into_secondary(&self) -> Option<SecondaryRingId> {
        if self.is_secondary() {
            Some(SecondaryRingId {
                reactor_id: self.reactor_id,
                index: self.index,
            })
        } else {
            None
        }
    }
    /// Attempt to convert this generic ring ID into a producer ring ID, if it represents one.
    #[inline]
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub fn try_into_producer(&self) -> Option<ProducerRingId> {
        if self.is_producer() {
            Some(ProducerRingId {
                reactor_id: self.reactor_id,
                index: self.index,
            })
        } else {
            None
        }
    }
}

impl PartialEq<RingId> for PrimaryRingId {
    #[inline]
    fn eq(&self, other: &RingId) -> bool {
        self.reactor_id == other.reactor_id
            && self.index == other.index
            && other.ty == RingTy::Primary
    }
}
impl PartialEq<PrimaryRingId> for RingId {
    #[inline]
    fn eq(&self, other: &PrimaryRingId) -> bool {
        other == self
    }
}
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
impl PartialEq<RingId> for SecondaryRingId {
    #[inline]
    fn eq(&self, other: &RingId) -> bool {
        self.reactor_id == other.reactor_id
            && self.index == other.index
            && other.ty == RingTy::Secondary
    }
}
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
impl PartialEq<SecondaryRingId> for RingId {
    #[inline]
    fn eq(&self, other: &SecondaryRingId) -> bool {
        other == self
    }
}
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
impl PartialEq<RingId> for ProducerRingId {
    #[inline]
    fn eq(&self, other: &RingId) -> bool {
        self.reactor_id == other.reactor_id
            && self.index == other.index
            && other.ty == RingTy::Producer
    }
}
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
impl PartialEq<ProducerRingId> for RingId {
    #[inline]
    fn eq(&self, other: &ProducerRingId) -> bool {
        other == self
    }
}
impl From<PrimaryRingId> for RingId {
    #[inline]
    fn from(primary: PrimaryRingId) -> Self {
        Self {
            reactor_id: primary.reactor_id,
            index: primary.index,
            ty: RingTy::Primary,
        }
    }
}
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
impl From<SecondaryRingId> for RingId {
    #[inline]
    fn from(secondary: SecondaryRingId) -> Self {
        Self {
            reactor_id: secondary.reactor_id,
            index: secondary.index,
            ty: RingTy::Secondary,
        }
    }
}
#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
impl From<ProducerRingId> for RingId {
    #[inline]
    fn from(secondary: ProducerRingId) -> Self {
        Self {
            reactor_id: secondary.reactor_id,
            index: secondary.index,
            ty: RingTy::Producer,
        }
    }
}
pub(crate) enum RingIdKind {
    Primary(PrimaryRingId),
    #[cfg(target_os = "redox")]
    Secondary(SecondaryRingId),
    #[cfg(target_os = "redox")]
    Producer(ProducerRingId),
}
impl From<RingId> for RingIdKind {
    #[inline]
    fn from(id: RingId) -> Self {
        match id.ty {
            RingTy::Primary => RingIdKind::Primary(PrimaryRingId {
                reactor_id: id.reactor_id,
                index: id.index,
            }),
            #[cfg(target_os = "redox")]
            RingTy::Secondary => RingIdKind::Secondary(SecondaryRingId {
                reactor_id: id.reactor_id,
                index: id.index,
            }),
            #[cfg(target_os = "redox")]
            RingTy::Producer => RingIdKind::Producer(ProducerRingId {
                reactor_id: id.reactor_id,
                index: id.index,
            }),
        }
    }
}
impl From<RingIdKind> for RingId {
    #[inline]
    fn from(id_kind: RingIdKind) -> Self {
        match id_kind {
            RingIdKind::Primary(p) => p.into(),
            #[cfg(target_os = "redox")]
            RingIdKind::Secondary(s) => s.into(),
            #[cfg(target_os = "redox")]
            RingIdKind::Producer(p) => p.into(),
        }
    }
}

/// A reference with lifetime `'ring` to the system type for Submission Queue Entries.
#[cfg(target_os = "redox")]
pub type SysSqeRef<'ring> = &'ring mut SqEntry64;
#[cfg(target_os = "linux")]
/// A reference type (since SQEs are usually large) to the Submission Queue Entry type for the
/// current platform.
pub type SysSqeRef<'ring> = iou::SQE<'ring>;

/// The system Completion Queue Entry type.
#[cfg(target_os = "redox")]
pub type SysCqe = CqEntry64;
#[cfg(target_os = "linux")]
/// The Completion Queue Entry type for the current platform.
pub type SysCqe = iou::CQE;

/// The system type for I/O priorities.
#[cfg(target_os = "redox")]
pub type SysPriority = Priority;
/// The system type for I/O priorities.
#[cfg(target_os = "linux")]
pub type SysPriority = Priority;

#[cfg(target_os = "linux")]
/// The system file descriptor type.
pub type SysFd = std::os::unix::io::RawFd;
/// The system file descriptor type. On redox, this is `usize`.
#[cfg(target_os = "redox")]
pub type SysFd = usize;

/// The return value in CQEs. On Redox, this is `usize`.
#[cfg(target_os = "redox")]
pub type SysRetval = usize;
/// The return value in CQEs. On Linux, this is `i32`.
#[cfg(target_os = "linux")]
pub type SysRetval = i32;

/// The system type for I/O vectors.
#[cfg(target_os = "redox")]
pub type SysIoVec = IoVec;
/// The system type for I/O vectors.
#[cfg(target_os = "linux")]
pub type SysIoVec = libc::iovec;

/// The system type for pwritev2 flags.
#[cfg(target_os = "redox")]
pub type SysWriteFlags = WriteFlags;
/// The system type for pwritev2 flags.
#[cfg(target_os = "linux")]
pub type SysWriteFlags = WriteFlags;

/// The system type for preadv2 flags.
#[cfg(target_os = "redox")]
pub type SysReadFlags = ReadFlags;
/// The system type for preadv2 flags.
#[cfg(target_os = "linux")]
pub type SysReadFlags = ReadFlags;

/// The system type for close flags.
#[cfg(target_os = "redox")]
pub type SysCloseFlags = CloseFlags;
/// The system type for close flags.
#[cfg(target_os = "linux")]
pub type SysCloseFlags = ();

/// A builder that configures the reactor.
#[derive(Debug)]
pub struct ReactorBuilder {
    primary_instances: Vec<ConsumerInstanceWrapper>,
}

impl ReactorBuilder {
    /// Create an executor builder with the default options.
    #[inline]
    pub const fn new() -> Self {
        Self {
            primary_instances: Vec::new(),
        }
    }
    /// Add a primary instance that will be used by the executor. Note that only one of these may
    /// be blocked on concurrently, but it may be more performant to use multiple instances in a
    /// multithreaded program.
    ///
    /// It will be assumed for the instance here, that the producer of the `io_uring`s can be
    /// trusted, and that the `user_data` field of completion entries _always_ equals the
    /// corresponding user data of the submission for that command. This option is disabled by
    /// default, when the the producer is not the kernel.
    ///
    /// # Safety
    ///
    /// This is unsafe because when enabled, it will optimize the executor to use the `user_data`
    /// field as a pointer to the status. A rogue producer would be able to change the user data
    /// pointer, to an arbitrary address, and cause program corruption. While the addresses can be
    /// checked at runtime, this is too expensive to check if performance is a concern (and
    /// probably even more expensive than simply storing the user_data as a tag, which is the
    /// default). When the kernel is a producer though, this will not make anything more unsafe
    /// (since the kernel has full access to the address space anyway).
    pub unsafe fn with_trusted_primary_instance(self, primary_instance: ConsumerInstance) -> Self {
        self.with_primary_instance_generic(primary_instance, true)
    }
    #[allow(unused_variables)]
    unsafe fn with_primary_instance_generic(
        mut self,
        primary_instance: ConsumerInstance,
        trusted: bool,
    ) -> Self {
        self.primary_instances.push(ConsumerInstanceWrapper {
            consumer_instance: primary_instance,
            #[cfg(target_os = "redox")]
            trusted,
            dropped: AtomicBool::new(false),

            #[cfg(target_os = "linux")]
            current_threadid: RwLock::new(None),
        });
        self
    }
    /// Add a primary instance that is considered untrusted. Hence, rather than storing pointers
    /// directly in the user data fields of the SQEs, the user data field will rather point to a
    /// tag, that is the key of a B-tree map of states.
    pub fn with_untrusted_primary_instance(self, primary_instance: ConsumerInstance) -> Self {
        unsafe { self.with_primary_instance_generic(primary_instance, false) }
    }
    /// Add a primary instance to the reactor. Whether this is to be marked _trusted_ is determined
    /// based on platform (Linux is a monolithic kernel and thus only has the kernel as producer),
    /// and the type of the instance.
    pub fn with_primary_instance(self, primary_instance: ConsumerInstance) -> Self {
        #[cfg(target_os = "redox")]
        let trust = primary_instance.is_attached_to_kernel();

        #[cfg(target_os = "linux")]
        let trust = true;

        unsafe { self.with_primary_instance_generic(primary_instance, trust) }
    }

    /// Finalize the reactor, using the options that have been specified here.
    ///
    /// # Panics
    ///
    /// This function will panic if the primary instance has not been set.
    pub fn build(self) -> Arc<Reactor> {
        Reactor::new(self.primary_instances)
    }
}
impl Reactor {
    fn new(main_instances: Vec<ConsumerInstanceWrapper>) -> Arc<Self> {
        let reactor = Reactor {
            id: ReactorId {
                inner: LAST_REACTOR_ID.fetch_add(1, atomic::Ordering::Relaxed),
            },
            main_instances,

            #[cfg(target_os = "redox")]
            secondary_instances: RwLock::new(SecondaryInstancesWrapper {
                consumer_instances: Vec::new(),
                producer_instances: Vec::new(),
                fds_backref: BTreeMap::new(),
            }),

            tag_map: RwLock::new(BTreeMap::new()),
            next_tag: AtomicTag::new(1),
            reusable_tags: ArrayQueue::new(512),
            weak_ref: None,
        };

        let mut arc = Arc::new(reactor);
        let weak = Arc::downgrade(&arc);
        unsafe {
            Arc::get_mut_unchecked(&mut arc).weak_ref = Some(weak);
        }

        arc

        // TODO: Update Redox compiler version!
        /*Arc::new_cyclic(|weak_ref| Reactor {
            id: ReactorId {
                inner: LAST_REACTOR_ID.fetch_add(1, atomic::Ordering::Relaxed),
            },
            main_instances,

            #[cfg(target_os = "redox")]
            secondary_instances: RwLock::new(SecondaryInstancesWrapper {
                instances: Vec::new(),
                fds_backref: BTreeMap::new(),
            }),

            tag_map: RwLock::new(BTreeMap::new()),
            next_tag: AtomicTag::new(1),
            reusable_tags: ArrayQueue::new(512),
            weak_ref: Weak::clone(weak_ref),
        })*/
    }
    /// Retrieve the ring ID of the primary instance, which must be a userspace-to-kernel ring if
    /// there are more than one rings in the reactor.
    pub fn primary_instances(&self) -> impl Iterator<Item = PrimaryRingId> + '_ {
        let reactor_id = self.id();

        (0..self.main_instances.len()).map(move |index| PrimaryRingId { reactor_id, index })
    }
    /// Obtain a handle to this reactor, capable of creating futures that use it. The handle will
    /// be weakly owned, and panic on regular operations if this reactor is dropped.
    pub fn handle(&self) -> Handle {
        Handle {
            reactor: Weak::clone(self.weak_ref.as_ref().unwrap()),
        }
    }
    /// Add an additional secondary instance to the reactor, waking up the executor to include it
    /// if necessary. If the main SQ is full, this will fail with ENOSPC (TODO: fix this, and block
    /// instead).
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub fn add_secondary_instance(
        &self,
        ctx: SubmissionContext,
        instance: ConsumerInstance,
    ) -> Result<SecondaryRingId> {
        let ringfd = instance.ringfd();
        self.add_secondary_instance_generic(
            Left(ConsumerInstanceWrapper {
                consumer_instance: instance,
                dropped: AtomicBool::new(false),
                trusted: false, // TODO
            }),
            ringfd,
            ctx,
        )
        .map(|last_index| SecondaryRingId {
            index: last_index,
            reactor_id: self.id,
        })
    }
    #[cfg(target_os = "redox")]
    fn add_secondary_instance_generic(
        &self,
        instance: Either<ConsumerInstanceWrapper, ProducerInstanceWrapper>,
        ringfd: usize,
        ctx: SubmissionContext,
    ) -> Result<usize> {
        let mut guard = self.secondary_instances.write();

        // Tell the kernel to send us special event CQEs which indicate that other io_urings have
        // received additional entries, which is what actually allows secondary instances to make
        // progress.
        {
            let fd64 = ringfd.try_into().or(Err(Error::new(EBADF)))?;

            self.main_instances
                .get(0)
                .unwrap()
                .consumer_instance
                .sender()
                .write()
                .as_64_mut()
                .expect("expected SqEntry64")
                .try_send(SqEntry64 {
                    opcode: StandardOpcode::RegisterEvents as u8,
                    priority: ctx.priority(),
                    flags: (IoUringSqeFlags::SUBSCRIBE | ctx.sync().redox_sqe_flags()).bits(),
                    // not used since the driver will know that it's an io_uring being updated
                    user_data: 0,

                    syscall_flags: (RegisterEventsFlags::READ | RegisterEventsFlags::IO_URING)
                        .bits(),
                    addr: 0, // unused
                    fd: fd64,
                    offset: 0, // unused
                    len: 0,    // unused

                    additional1: 0,
                    additional2: 0,
                })?;
        }

        let (last_index, ty) = match instance {
            Left(consumer_instance) => {
                let instances_len = guard.consumer_instances.len();
                guard.consumer_instances.push(consumer_instance);

                (instances_len, BackrefTy::Consumer)
            }
            Right(producer_instance) => {
                let instances_len = guard.producer_instances.len();
                guard.producer_instances.push(producer_instance);

                (instances_len, BackrefTy::Producer)
            }
        };

        guard.fds_backref.insert(ringfd, (last_index, ty));

        Ok(last_index)
    }
    /// Add a producer instance (the producer of a userspace-to-userspace or kernel-to-userspace
    /// instance). This will use the main ring to register interest in file updates on the file
    /// descriptor of this ring.
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub fn add_producer_instance(
        &self,
        // TODO: Support specifying a primary ring to do the operation on.
        ctx: SubmissionContext,
        instance: ProducerInstance,
    ) -> Result<ProducerRingId> {
        let ringfd = instance.ringfd();
        self.add_secondary_instance_generic(
            Right(ProducerInstanceWrapper {
                producer_instance: instance,
                dropped: AtomicBool::new(false),
                stream_state: None,
            }),
            ringfd,
            ctx,
        )
        .map(|last_index| ProducerRingId {
            index: last_index,
            reactor_id: self.id,
        })
    }
    /// Retrieve the unique ID of this reactor.
    #[inline]
    pub fn id(&self) -> ReactorId {
        self.id
    }
    pub(crate) fn driving_waker(
        reactor: &Arc<Reactor>,
        runqueue: Option<(Weak<Runqueue>, usize)>,
        // NOTE: We know that the Vec of instances is immutable, since the reactor will always be
        // wrapped in an Arc. Hence, the order will never change, so indices are fine.
        index: usize,
    ) -> task::Waker {
        let reactor = Arc::downgrade(reactor);

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

            let reactor = reactor
                .upgrade()
                .expect("failed to wake up executor: integrated reactor dead");

            let main_instance = reactor
                .main_instances
                .get(index)
                .expect("index passed to driving_waker shouldn't be invalid");

            if main_instance.dropped.load(Ordering::Acquire) {
                return;
            }

            #[cfg(target_os = "redox")]
            {
                // On Redox, we wake up the primary ring (i.e. the only ring that the reactor waits
                // for), by incrementing the push epoch of the CQ without necessarily any new
                // entry. Then we enter the ring with 0 as min_submit and 0 as min_complete,
                // effectively causing the kernel to poll the ring and its epochs, and see that we
                // notified it.

                // TODO: Is this any better than the signal logic on Linux? Apart from saving a
                // system call when notifying, by only adding 1 to the epochs and letting the
                // kernel unblock the target context after scheduling time the waker context (which
                // is only true for certain higher-priority processes), there are probably no major
                // benefits in addition to that.

                dbg!();
                let consumer_instance = &main_instance.consumer_instance;
                dbg!();

                consumer_instance.receiver().read().notify_self_about_push();
                dbg!();

                // TODO: Only enter for rings that are not polled by the kernel when scheduling.
                consumer_instance
                    .enter_for_notification()
                    .expect("failed to wake up executor: entering the io_uring failed");
                dbg!();
            }
            #[cfg(target_os = "linux")]
            {
                let guard = main_instance.current_threadid.read();
                // On Linux, we wake up the primary ring by triggering a custom signal that is set
                // to SIG_IGN, but with the SA_RESTART flag, causing the `io_uring_enter` syscall
                // to immediately error with EINTR.

                // TODO: When using thread pools, begin with finding a thread that the future can
                // be moved to, and then simply unblock that thread. This system call will only
                // need to be used when either all threads are currently in the kernel waiting for
                // an io_uring event, or when it would otherwise make sense to do so, to distribute
                // futures more evenly.
                if let Some(pthread_id) = *guard {
                    // TODO: Handle error!
                    let _ = unsafe { libc::pthread_kill(pthread_id, libc::SIGUSR1) };
                }
            }
        })
    }
    pub(crate) fn drive_primary(&self, idx: usize, waker: &task::Waker, wait: bool) {
        self.drive(&self.main_instances[idx], waker, wait, true)
    }
    fn drive(
        &self,
        instance: &ConsumerInstanceWrapper,
        waker: &task::Waker,
        wait: bool,
        primary: bool,
    ) {
        #[cfg(target_os = "redox")]
        {
            fn warn_about_inconsistency(instance: &ConsumerInstanceWrapper) -> Error {
                log::error!("Ring (instance: {:?}) was not able to pop, as it had entered an inconsistent state. This is either a bug in the producer, a bug in the io_uring management of this process, or a kernel bug.", instance);
                Error::new(EIO)
            }
            let num_completed = if wait {
                let sq_free_entry_count = instance
                    .consumer_instance
                    .sq_free_entry_count()
                    .map_err(|BrokenRing| warn_about_inconsistency(instance))?;

                let flags = if sq_free_entry_count > 0 {
                    IoUringEnterFlags::empty()
                } else {
                    // TODO: ... and has entries that need to be pushed?
                    IoUringEnterFlags::WAKEUP_ON_SQ_AVAIL
                };
                log::debug!("Entering io_uring");
                Some(instance.consumer_instance.enter(0, 0, flags)?)
            } else {
                None
            };
            log::debug!("Entered io_uring with num_completed {:?}", num_completed);

            let mut receiver_write_guard = instance.consumer_instance.receiver().write();

            loop {
                match receiver_write_guard.as_64_mut().unwrap().try_recv() {
                    Ok(cqe) => {
                        // NOTE: The driving wakers that will possibly be called here, must be able
                        // to acquire a read lock. We therefore downgrade our receiver exclusive
                        // lock, into an intent lock, and then eventually back.
                        let receiver_intent_guard =
                            RwLockWriteGuard::downgrade_to_upgradable(receiver_write_guard);
                        let result = self.drive_handle_cqe(primary, instance.trusted, cqe, waker);
                        receiver_write_guard =
                            RwLockUpgradableReadGuard::upgrade(receiver_intent_guard);
                        result
                    }
                    Err(RingPopError::Empty { .. }) => break,
                    Err(RingPopError::Shutdown) => {
                        instance
                            .dropped
                            .store(true, std::sync::atomic::Ordering::Release);
                        break;
                    }
                    Err(RingPopError::Broken) => {
                        return Err(warn_about_inconsistency(instance));
                    }
                }
            }
        }
        #[cfg(target_os = "linux")]
        {
            let mut guard = instance.consumer_instance.lock();
            let trusted = true;

            let min_complete = if wait { 1 } else { 0 };
            // TODO: Map error correctly, with Linux error codes.
            match guard.submit_sqes_and_wait(min_complete) {
                Ok(_) => (),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => return,
                Err(error) => {
                    log::error!("Failed to pop CQE from CQ: {}", error);
                    todo!("convert error: {}", error);
                }
            }

            for cqe in guard.cqes() {
                self.drive_handle_cqe(primary, trusted, cqe, waker);
            }
        }
    }
    fn drive_handle_cqe(&self, primary: bool, trusted: bool, cqe: SysCqe, waker: &task::Waker) {
        log::debug!(
            "Handle CQE primary: {}, trusted: {}, cqe: {:?}, waker {:?}",
            primary,
            trusted,
            cqe,
            waker
        );
        #[cfg(target_os = "linux")]
        let _primary = primary;

        log::debug!("Received CQE: {:?}", cqe);
        #[cfg(target_os = "redox")]
        if IoUringCqeFlags::from_bits_truncate((cqe.flags & 0xFF) as u8)
            .contains(IoUringCqeFlags::EVENT)
            && EventFlags::from_bits_truncate((cqe.flags >> 8) as usize)
                .contains(EventFlags::EVENT_IO_URING)
        {
            // if this was an event, that was tagged io_uring, we can assume that the
            // event came from the kernel having polled some secondary io_urings. We'll
            // then drive those instances and wakeup futures.

            let fd64 = match Error::demux64(cqe.status) {
                Ok(fd64) => fd64,
                Err(error) => {
                    log::warn!("Error on receiving an event about secondary io_uring progress: {}. Ignoring event.", error);
                    return;
                }
            };

            let fd = match usize::try_from(fd64) {
                Ok(fd) => fd,
                Err(_) => {
                    log::warn!("The kernel gave us a CQE with a status that was too large to fit a system-wide file descriptor ({} > {}). Ignoring event.", cqe.status, usize::max_value());
                    return;
                }
            };

            let secondary_instances_guard = self.secondary_instances.read();

            let (secondary_instance_index, backref_ty) = match secondary_instances_guard
                .fds_backref
                .get(&fd)
            {
                Some(pair) => *pair,
                None => {
                    log::warn!("The fd ({}) meant to describe the instance to drive, was not recognized. Ignoring event.", fd);
                    return;
                }
            };
            let primary_instance = self
                .main_instances
                .get(0)
                .expect("expected primary instance to exist");

            let errmsg = "fd backref BTreeMap corrupt, contains a file descriptor that was removed";
            match backref_ty {
                BackrefTy::Consumer => {
                    let instance = secondary_instances_guard
                        .consumer_instances
                        .get(secondary_instance_index)
                        .expect(errmsg);

                    self.drive(instance, waker, false, false)
                        .expect("failed to drive consumer instance");
                }
                BackrefTy::Producer => {
                    let instance = secondary_instances_guard
                        .producer_instances
                        .get(secondary_instance_index)
                        .expect(errmsg);

                    self.drive_producer_instance(primary_instance, &instance)
                        .expect("failed to drive producer instance");
                }
            }
            return;
        }

        let _ = Self::handle_cqe(trusted, self.tag_map.read(), waker, cqe);
    }
    #[cfg(target_os = "redox")]
    fn drive_producer_instance(
        &self,
        primary_instance: &ConsumerInstanceWrapper,
        instance: &ProducerInstanceWrapper,
    ) -> Result<()> {
        log::debug!("Event was an external producer io_uring, thus polling the ring itself");
        loop {
            assert!(primary_instance.trusted);

            let state_lock = match instance.stream_state {
                Some(ref s) => s,
                None => return Ok(()),
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
                        let sqe = match instance
                            .producer_instance
                            .receiver()
                            .write()
                            .as_64_mut()
                            .unwrap()
                            .try_recv()
                        {
                            Ok(sqe) => sqe,
                            Err(RingPopError::Empty { .. }) => {
                                log::debug!("Ring was empty");
                                break;
                            }
                            Err(RingPopError::Shutdown) => {
                                log::debug!("Secondary producer ring dropped");
                                instance
                                    .dropped
                                    .store(true, std::sync::atomic::Ordering::Release);
                                *state_guard = ProducerSqesState::Finished;
                                break;
                            }
                            Err(RingPopError::Broken) => {
                                log::error!("Producer instance {:?} had a broken io_uring. Failing with EIO, causing the ring to be removed from the reactor.", instance);
                                return Err(Error::new(EIO));
                            }
                        };
                        log::debug!("Secondary producer SQE: {:?}", sqe);
                        deque.push_back(sqe);
                        log::debug!("pushed");
                        if let Some(future_waker) = future_waker.take() {
                            log::debug!("will awake");
                            future_waker.wake();
                            log::debug!("awoke");
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
            }
        }
        Ok(())
    }
    fn handle_cqe(
        trusted_instance: bool,
        tags: RwLockReadGuard<'_, BTreeMap<Tag, Arc<Mutex<State>>>>,
        driving_waker: &task::Waker,
        cqe: SysCqe,
    ) -> Option<()> {
        #[cfg(target_os = "redox")]
        let (cancelled, user_data) = {
            let cancelled = cqe.status == (-(ECANCELED as i64)) as u64;
            let user_data = cqe.user_data;
            (cancelled, user_data)
        };

        #[cfg(target_os = "linux")]
        let (cancelled, user_data) = {
            let cancelled = cqe.raw_result() == -libc::ECANCELED;
            let user_data = cqe.user_data();
            (cancelled, user_data)
        };

        let state_arc;

        let state_lock = if trusted_instance {
            let pointer = usize::try_from(user_data).ok()? as *mut Mutex<State>;
            let state_weak = unsafe { Weak::from_raw(pointer) };
            state_arc = state_weak.upgrade()?;
            &state_arc
        } else {
            tags.get(&user_data)?
        };

        let mut state = state_lock.lock();
        match &mut state.inner {
            // invalid state after having received a completion
            StateInner::Initial
            | StateInner::Submitting(_)
            | StateInner::Completed(_)
            | StateInner::Cancelled => return None,

            StateInner::Completing(waker) => {
                // Wake other executors which have futures using this reactor.
                if !waker.will_wake(driving_waker) {
                    waker.wake_by_ref();
                }

                state.inner = if cancelled {
                    StateInner::Cancelled
                } else {
                    StateInner::Completed(cqe)
                };
            }
            #[cfg(target_os = "redox")]
            StateInner::ReceivingMulti(ref mut pending_cqes, waker) => {
                if !waker.will_wake(driving_waker) {
                    waker.wake_by_ref();
                }
                pending_cqes.push_back(cqe);
            }
        }
        Some(())
    }
    pub(crate) fn consumer_instance(
        &self,
        ring: impl Into<RingId>,
    ) -> Either<&ConsumerInstance, MappedRwLockReadGuard<ConsumerInstance>> {
        let ring = ring.into();

        if ring.reactor() != self.id() {
            panic!(
                "Using a reactor id from another reactor to get an instance: {:?} is not from {:?}",
                ring,
                self.id()
            );
        }
        #[cfg(target_os = "redox")]
        assert_ne!(ring.ty, RingTy::Producer);

        if ring.is_primary() {
            Left(&self.main_instances[ring.index].consumer_instance)
        } else {
            #[cfg(target_os = "redox")]
            {
                Right(RwLockReadGuard::map(
                    self.secondary_instances.read(),
                    |instances| {
                        &instances
                            .consumer_instances
                            .get(ring.index)
                            .expect("invalid secondary consumer ring ID")
                            .consumer_instance
                    },
                ))
            }
            #[cfg(not(target_os = "redox"))]
            unreachable!();
        }
    }
}

/// The type of synchronization needed prior to handling a submission queue entry, if any.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SubmissionSync {
    /// Do not synchronize this SQE; instead, allow SQE reordering between both preceding and
    /// succeeding SQEs.
    NoSync,
    /// Do a full pipeline barrier, requiring _every_ SQE prior to this SQE to complete (i.e. have
    /// its CQE pushed), before this SQE can be handled.
    Drain,
    /// Do a partial pipeline barrier, by requiring the SQE before this SQE to complete prior to
    /// handling this SQE.
    // TODO: As per my understanding, Linux also has soft links and hard links (and maybe the word
    // "link" is better than "chain". I simply chose "chain" because that's the terminology used in
    // XHCI I/O queues). A soft link will terminate the entire chain upon a single cancellation or
    // error, while a hard link will execute every command in the chain sequentially, but not
    // cancel the entire chain immediately.
    Chain,
    // TODO: Add support for speculative (I hope I don't make io_uring Turing-complete and
    // vulnerable to Spectre) execution of subsequent SQEs, so long as they don't affect or are
    // affected by the results of submissions on the other side of the barrier.
}

impl SubmissionSync {
    /// Get the SQE flags that would be used for an SQE that has the same synchronization options
    /// as specified here. Note that the flags here only change the how the SQE is synchronized, so
    /// one might need to OR these flags with some other flags.
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub fn redox_sqe_flags(self) -> IoUringSqeFlags {
        match self {
            Self::NoSync => IoUringSqeFlags::empty(),
            Self::Drain => IoUringSqeFlags::DRAIN,
            Self::Chain => IoUringSqeFlags::CHAIN,
        }
    }
    /// Like with [`redox_sqe_flags`], retrieve the necessary flags to achieve the synchronization
    /// needed.
    ///
    /// [`redox_sqe_flags`]: Self::redox_sqe_flags
    #[cfg(any(doc, target_os = "linux"))]
    #[doc(cfg(target_os = "linux"))]
    pub fn linux_sqe_flags(self) -> iou::sqe::SubmissionFlags {
        match self {
            Self::NoSync => iou::sqe::SubmissionFlags::empty(),
            Self::Drain => iou::sqe::SubmissionFlags::IO_DRAIN,
            Self::Chain => iou::sqe::SubmissionFlags::IO_LINK,
        }
    }
}

impl Default for SubmissionSync {
    fn default() -> Self {
        Self::NoSync
    }
}

/// The context for a submission, containing information such as priority, synchronization, and the
/// guard that is set once the actual future gets constructed.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct SubmissionContext {
    priority: SysPriority,
    sync: SubmissionSync,
}
impl SubmissionContext {
    /// Create a new submission context, using a non-specified priority and with no explicit
    /// synchronization.
    pub fn new() -> Self {
        Self::default()
    }
    /// Set the priority of this submission, taking self by value.
    pub fn with_priority(self, priority: SysPriority) -> Self {
        Self { priority, ..self }
    }
    /// Get the priority of this submission.
    pub const fn priority(&self) -> SysPriority {
        self.priority
    }
    /// Set the priority of this submission, by reference.
    pub fn set_priority(&mut self, priority: SysPriority) {
        self.priority = priority;
    }
    /// Set the synchronization mode of this submission, taking self by value.
    pub fn with_sync(self, sync: SubmissionSync) -> Self {
        Self { sync, ..self }
    }
    /// Retrieve the synchronization of this submission.
    pub const fn sync(&self) -> SubmissionSync {
        self.sync
    }
    /// Set the synchronization mode of this submission, taking self by reference.
    pub fn set_sync(&mut self, sync: SubmissionSync) {
        self.sync = sync;
    }
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
    pub unsafe fn send<F>(&self, ring: impl Into<RingId>, prepare_sqe: F) -> CommandFuture<F>
    where
        F: for<'ring, 'tmp> FnOnce(&'tmp mut SysSqeRef<'ring>),
    {
        self.send_inner(ring, SendArg::<F>::Single(prepare_sqe))
            .left()
            .expect("send_inner() must return CommandFuture if is_stream is set to false")
    }
    /// Shorthand for send(), but where the submission context is applied to the preparation
    /// function, before calling the inner.
    ///
    /// # Safety
    ///
    /// The same safety requirements found in [`send`], all apply here.
    ///
    /// [`send`]: Self::send
    pub unsafe fn send_with_ctx<F>(
        &self,
        ring: impl Into<RingId>,
        #[cfg_attr(target_os = "linux", allow(unused_variables))] ctx: SubmissionContext,
        prepare_sqe: F,
    ) -> CommandFuture<impl FnOnce(&mut SysSqeRef)>
    where
        F: for<'ring, 'tmp> FnOnce(&'tmp mut SysSqeRef<'ring>),
    {
        let prepare_sqe_wrapper = move |sqe: &mut SysSqeRef| {
            #[cfg(target_os = "redox")]
            {
                sqe.base(ctx.sync().redox_sqe_flags(), ctx.priority(), (-1i64) as u64);
            }
            #[cfg(target_os = "linux")]
            {}

            prepare_sqe(sqe)
        };

        self.send(ring, prepare_sqe_wrapper)
    }

    unsafe fn send_inner<F>(
        &self,
        ring: impl Into<RingId>,
        send_arg: SendArg<F>,
    ) -> Either<CommandFuture<F>, FdEvents> {
        let ring = ring.into();

        let reactor = self
            .reactor
            .upgrade()
            .expect("failed to initiate new command: reactor is dead");

        assert_eq!(ring.reactor_id, reactor.id());

        #[cfg(target_os = "redox")]
        let trusted = ring.is_primary() && reactor.main_instances[ring.index].trusted;

        #[cfg(target_os = "linux")]
        let trusted = true;

        let (tag_num_opt, state_opt) = match reactor.reusable_tags.pop() {
            // try getting a reusable tag to minimize unnecessary allocations
            Ok((n, state)) => {
                assert!(
                    matches!(state.lock().inner, StateInner::Initial),
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

                if trusted {
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
                let state_arc = Arc::new(Mutex::new(State {
                    inner: StateInner::Initial,
                    epoch: 0,
                }));

                let n = reactor
                    .next_tag
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                if n.checked_add(1).is_none() {
                    // FIXME: Find some way to handle this, if it ever were to become a problem. We
                    // cannot really reuse tags, unless we really want to traverse the B-tree again
                    // to find unused tags (which would be fairly simple; given that tags are
                    // initialized and deinitialized in the same tempo, there should be a large
                    // range of unused tags at the start).
                    panic!("redox-iou tag overflow");
                }

                if trusted {
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
            repr: if trusted {
                CommandFutureRepr::Direct(state_opt.unwrap())
            } else {
                CommandFutureRepr::Tagged(tag_num_opt.unwrap())
            },
        };

        match send_arg {
            #[cfg(target_os = "redox")]
            SendArg::Stream(initial) => Right(FdEvents {
                inner,
                initial: Some(initial),
            }),
            SendArg::Single(prep_fn) => {
                let mut future = CommandFuture {
                    inner,
                    prepare_fn: None,
                };
                future.prepare_fn = Some(prep_fn);

                Left(future)
            }
        }
    }
    /// Send a Completion Queue Entry to the consumer, waking it up when the reactor enters the
    /// io_uring again.
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub fn send_producer_cqe(
        &self,
        instance: ProducerRingId,
        cqe: CqEntry64,
    ) -> Result<(), RingPushError<CqEntry64>> {
        let reactor = self
            .reactor
            .upgrade()
            .expect("failed to send producer CQE: reactor is dead");

        assert_eq!(reactor.id(), instance.reactor_id());

        let guard = reactor.secondary_instances.read();

        let producer_instance = guard
            .producer_instances
            .get(instance.index)
            .expect("invalid ProducerRingId: non-existent instance");

        let send_result = producer_instance
            .producer_instance
            .sender()
            .write()
            .as_64_mut()
            .unwrap()
            .try_send(cqe);

        match send_result {
            Ok(()) => Ok(()),
            Err(RingPushError::Full(_)) => Err(RingPushError::Full(cqe)),
            Err(RingPushError::Shutdown(_)) | Err(RingPushError::Broken(_)) => {
                producer_instance
                    .dropped
                    .store(true, atomic::Ordering::Release);
                Err(RingPushError::Shutdown(cqe))
            }
        }
    }
    /// Create a futures-compatible stream that yields the SQEs sent by the consumer, to this
    /// producer.
    ///
    /// The capacity field will specify the number of SQEs in the internal queue of the stream. A
    /// low capacity will cause the ring to be polled more often, while a higher capacity will
    /// prevent congestion control to some extent, by popping the submission ring more often,
    /// allowing the consumer to push more entries before it must block.
    ///
    /// TODO: Poll the ring directly from the future instead.
    ///
    /// # Panics
    ///
    /// This method will panic if the reactor has been dropped, if the secondary ring ID is
    /// invalid, or if the capacity is zero.
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub fn producer_sqes(&self, ring_id: ProducerRingId, capacity: usize) -> ProducerSqes {
        let reactor = self
            .reactor
            .upgrade()
            .expect("failed to send producer CQE: reactor is dead");

        assert_eq!(reactor.id(), ring_id.reactor_id());

        let mut secondary_instances = reactor.secondary_instances.write();

        let producer_instance = secondary_instances
            .producer_instances
            .get_mut(ring_id.index)
            .expect("ring ID has an invalid index");

        let state_opt = producer_instance.stream_state.clone();
        // TODO: Override capacity if the stream state already is present.

        let state = match state_opt {
            Some(st) => st,
            None => {
                let new_state = Arc::new(Mutex::new(ProducerSqesState::Receiving {
                    capacity,
                    deque: VecDeque::with_capacity(capacity),
                    waker: None,
                }));

                producer_instance.stream_state = Some(Arc::clone(&new_state));
                new_state
            }
        };

        ProducerSqes { state }
    }

    /// Create an asynchronous stream that represents the events coming from one of more file
    /// descriptors that are triggered when e.g. the file has changed, or is capable of reading new
    /// data, etc.
    // TODO: Is there an equivalent here for Linux?
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub unsafe fn fd_events(
        &self,
        ring: impl Into<RingId>,
        fd: SysFd,
        event_flags: EventFlags,
        oneshot: bool,
    ) -> FdEvents {
        assert!(!event_flags.contains(EventFlags::EVENT_IO_URING), "only the redox_iou reactor is allowed to use this flag unless io_uring API is used directly");

        let initial = FdEventsInitial {
            fd,
            event_flags,
            oneshot,
        };

        type PlaceholderPrepareFn = ();

        self.send_inner(ring, SendArg::<PlaceholderPrepareFn>::Stream(initial))
            .right()
            .expect("send_inner must return Right if is_stream is set to true")
    }

    fn completion_as_rw_io_result(cqe: SysCqe) -> Result<SysRetval> {
        // reinterpret the status as signed, to take an errors into account.
        #[cfg(target_os = "redox")]
        let signed = cqe.status as i64;
        #[cfg(target_os = "linux")]
        let signed = cqe.raw_result();

        match isize::try_from(signed) {
            Ok(s) => {
                #[cfg(target_os = "redox")]
                {
                    Error::demux(s as usize)
                }
                #[cfg(target_os = "linux")]
                {
                    if s >= 0 {
                        Ok(s as i32)
                    } else {
                        // TODO: Cross-platform error type.
                        Err(Error::new(s as i32))
                    }
                }
            }
            Err(_) => {
                log::warn!("Failed to cast 64 bit {{,p}}{{read,write}}{{,v}} status ({:?}), into pointer sized status.", Error::demux64(signed as u64));
                if let Ok(actual_bytes_read) = Error::demux64(signed as u64) {
                    let trunc =
                        std::cmp::min(isize::max_value() as u64, actual_bytes_read) as usize;
                    log::warn!("Truncating the number of bytes/written read as it could not fit usize, from {} to {}", signed, trunc);
                    #[cfg(target_os = "redox")]
                    return Ok(trunc as usize);
                    #[cfg(target_os = "linux")]
                    return Ok(trunc as i32);
                }
                Err(Error::new(EOVERFLOW))
            }
        }
    }
    async unsafe fn open_raw_unchecked_inner<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        path: &B,
        info: OpenInfo,
        at: OpenFrom,
    ) -> Result<SysFd>
    where
        B: AsOffsetLen + ?Sized,
    {
        let ring = ring.into();
        #[cfg(target_os = "redox")]
        let (at_fd64, path) = {
            let at_fd64 = match at {
                OpenFrom::At(fd) => Some(u64::try_from(fd)?),
                OpenFrom::CurrentDirectory => None,
            };
            let path = path
                .as_generic_slice(ring.is_primary())
                .ok_or(Error::new(EFAULT))?;

            (at_fd64, path)
        };

        let prepare_fn = |sqe: &mut SysSqeRef| {
            #[cfg(target_os = "redox")]
            {
                if let Some(fd64) = at_fd64 {
                    sqe.sys_openat(fd64, path, info.inner.0);
                } else {
                    sqe.sys_open(path, info.inner.0);
                }
            }
            #[cfg(target_os = "linux")]
            {
                let (flags, mode) = info.inner;

                // TODO: Workaround AsOffsetLen on Linux
                let slice = std::slice::from_raw_parts(
                    path.addr() as *const u8,
                    path.len().unwrap() as usize,
                );

                nul_check(slice);

                let cstr = std::ffi::CStr::from_bytes_with_nul_unchecked(slice);
                sqe.prep_openat(at.linux_fd(), cstr, flags, mode)
            }
        };

        let cqe = self.send_with_ctx(ring, ctx, prepare_fn).await?;

        let fd = Self::completion_as_rw_io_result(cqe)?;
        Ok(fd as SysFd)
    }

    /// Open a path represented by a byte slice (NUL-terminated on Linux), returning a new file
    /// descriptor for the file at by that path. This is the unsafe version of [`open_at`].
    ///
    /// # Safety
    ///
    /// An additional safety invariant is that on Linux, the buffer passed to the system call,
    /// _must_ be NUL-terminated, as the open(2), openat(2) and even openat2(2) all take a pointer
    /// to a NUL-terminated string (somehow). Since Rust does the right thing and also stores the
    /// length of all dynamically-sized data, this is checked _when debug assertions are enabled_.
    ///
    /// This invariant has no effect on Redox, which passes the length of the path to open.
    ///
    /// It is highly recommended that the regular [`open_at`] call be used instead, which takes
    /// care of guarding the memory until completion.
    ///
    /// [`open_at`]: Self::open_at
    pub async unsafe fn open_unchecked_at<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        path: &B,
        info: OpenInfo,
        // TODO: Merge "at" into "info".
        at: OpenFrom,
    ) -> Result<SysFd>
    where
        B: AsOffsetLen + ?Sized,
    {
        let ring = ring.into();

        self.open_raw_unchecked_inner(ring, ctx, path, info, at)
            .await
    }

    /// Open a file in a similar way to how [`open_unchecked_at`] works, but without the need to
    /// specify a file descriptor to initially search from, with `/` being the default.
    ///
    /// # Safety
    ///
    /// On Linux, this method will skip the check that the last byte of the path needs to be the
    /// NUL character.
    pub async unsafe fn open_unchecked<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        path: &B,
        info: OpenInfo,
    ) -> Result<SysFd>
    where
        B: AsOffsetLen + ?Sized,
    {
        let ring = ring.into();

        self.open_raw_unchecked_inner(ring, ctx, path, info, OpenFrom::CurrentDirectory)
            .await
    }

    /// Open a path represented by a byte slice, returning a new file descriptor for the file at
    /// that path.
    ///
    /// This is the safe version of [`open_unchecked`], but requires the path type to implement
    /// [`Guarded`], which only applies for owned pointers on the heap, or static references.  An
    /// optional `at` argument can also be specified, which will base the path on an open file
    /// descriptor of a directory, similar to _openat(2)_.
    ///
    /// [`open_unchecked`]: #method.open_unchecked
    /// [`Guarded`]: ../memory/struct.Guarded.html
    /// [`Guardable`]: ../memory/trait.Guardable.html
    pub async fn open_at<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        path_buf: B,
        info: OpenInfo,
        at: OpenFrom,
    ) -> (Result<SysFd>, B)
    where
        B: Guarded<Target = [u8]> + AsOffsetLen,
    {
        let path_buf = ManuallyDrop::new(path_buf);

        #[cfg(target_os = "linux")]
        nul_check((&*path_buf).borrow_guarded());

        let result =
            unsafe { self.open_raw_unchecked_inner(ring, ctx, &*path_buf, info, at) }.await;
        (result, ManuallyDrop::into_inner(path_buf))
    }

    /// Open a file in a similar way to how [`open_at`] works, but without specifying a file
    /// descriptor.
    pub async fn open<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        path_buf: B,
        info: OpenInfo,
    ) -> (Result<SysFd>, B)
    where
        B: Guarded<Target = [u8]> + AsOffsetLen,
    {
        self.open_at(ring.into(), ctx, path_buf, info, OpenFrom::CurrentDirectory)
            .await
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
        ctx: SubmissionContext,
        fd: SysFd,
        flags: SysCloseFlags,
    ) -> Result<()> {
        #[cfg(target_os = "redox")]
        let fd64 = fd.try_into().map_err(|_| Error::new(EBADF))?;

        let prepare_fn = |sqe: &mut SysSqeRef| {
            #[cfg(target_os = "redox")]
            {
                sqe.sys_close(fd64, flags);
            }
            #[cfg(target_os = "linux")]
            {
                let _ = flags;
                sqe.prep_close(fd);
            }
        };
        let cqe = self.send_with_ctx(ring, ctx, prepare_fn).await?;

        Self::completion_as_rw_io_result(cqe)?;

        Ok(())
    }

    /// Close a range of file descriptors, optionally flushing them if necessary. This functions
    /// exactly like multiple invocations of the [`close`] call, with the difference of only taking
    /// up one SQE and thus being more efficient when closing many adjacent file descriptors.
    ///
    /// This is a rare system call, but is useful for example when preparing a process when it is
    /// going to be cloned, since file descriptors in the range that are not open, will be ignored.
    ///
    /// ## Platform availability
    ///
    /// At the moment, this system call is only supported on Redox, but a blocking version of it is
    /// coming to Linux
    /// ([Close_Range](http://lkml.iu.edu/hypermail/linux/kernel/2008.0/03248.html)).
    ///
    ///
    /// # Safety
    ///
    /// Refer to the invariants documented in the [`close`] call.
    ///
    /// [`close`]: #method.close
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub async unsafe fn close_range(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        range: std::ops::Range<usize>,
        flags: CloseFlags,
    ) -> Result<()> {
        let start: u64 = range.start.try_into().or(Err(Error::new(EOVERFLOW)))?;
        let end: u64 = range.end.try_into().or(Err(Error::new(EOVERFLOW)))?;
        let count = end.checked_sub(start).ok_or(Error::new(EINVAL))?;

        let prepare_fn = |sqe: &mut SysSqeRef| {
            sqe.sys_close_range(start, count, flags);
        };
        let cqe = self.send_with_ctx(ring, ctx, prepare_fn).await?;

        Self::completion_as_rw_io_result(cqe)?;

        Ok(())
    }

    /// Read bytes, returning the number of bytes read, or zero if no more bytes are available.
    ///
    /// This is the unsafe variant of [`read`].
    ///
    /// # Safety
    ///
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    ///
    /// [`read`]: #method.read
    pub async unsafe fn read_unchecked(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        buf: &mut [u8],
        flags: ReadFlags,
    ) -> Result<SysRetval> {
        let ring = ring.into();

        #[cfg(target_os = "redox")]
        let (buf, fd64) = {
            let buf = buf
                .as_generic_slice_mut(ring.is_primary())
                .ok_or(Error::new(EFAULT))?;

            let fd64 = u64::try_from(fd).map_err(|_| Error::new(EBADF))?;

            (buf, fd64)
        };

        let cqe = self
            .send_with_ctx(ring, ctx, |sqe: &mut SysSqeRef| {
                #[cfg(target_os = "redox")]
                {
                    sqe.sys_read(fd64, buf, flags);
                }
                #[cfg(target_os = "linux")]
                {
                    sqe.prep_read(fd, buf, OFFSET_TREAT_AS_POSITIONED);
                    sqe.raw_mut().cmd_flags = uring_sys::cmd_flags {
                        rw_flags: flags.bits() as _,
                    };
                }
            })
            .await?;

        Self::completion_as_rw_io_result(cqe)
    }
    /// Read bytes, returning the number of bytes read, or zero if no more bytes are available.
    ///
    /// This is the safe variant of [`read_unchecked`].
    ///
    /// [`read_unchecked`]: #method.read_unchecked
    pub async fn read<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        buf: B,
        flags: ReadFlags,
    ) -> (Result<SysRetval>, B)
    where
        // TODO: Uninitialized memory and `ioslice`.
        B: GuardedMut<Target = [u8]> + AsOffsetLen,
    {
        let ring = ring.into();
        let mut buf = ManuallyDrop::new(buf);

        let result = unsafe {
            self.read_unchecked(ring, ctx, fd, buf.borrow_guarded_mut(), flags)
                .await
        };

        (result, ManuallyDrop::into_inner(buf))
    }
    /// Read bytes, vectored.
    ///
    /// At the moment there is no safe counterpart for this unchecked method, since this passes a
    /// list of buffers, rather than one single buffer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the buffers outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    // TODO: safe wrapper
    pub async unsafe fn readv_unchecked(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        bufs: &mut [SysIoVec],
        flags: ReadFlags,
    ) -> Result<SysRetval> {
        let ring = ring.into();

        #[cfg(target_os = "redox")]
        let fd64 = u64::try_from(fd).map_err(|_| Error::new(EBADF))?;

        let cqe = self
            .send_with_ctx(ring, ctx, |sqe: &mut SysSqeRef| {
                #[cfg(target_os = "redox")]
                {
                    sqe.sys_readv(fd64, bufs, flags);
                }
                #[cfg(target_os = "linux")]
                {
                    let bufs_unchecked = {
                        core::slice::from_raw_parts_mut(
                            bufs.as_mut_ptr() as *mut std::io::IoSliceMut,
                            bufs.len(),
                        )
                    };
                    sqe.prep_read_vectored(fd, bufs_unchecked, OFFSET_TREAT_AS_POSITIONED);
                    sqe.raw_mut().cmd_flags = uring_sys::cmd_flags {
                        rw_flags: flags.bits() as _,
                    };
                }
            })
            .await?;

        Self::completion_as_rw_io_result(cqe)
    }

    /// Read bytes from a specific offset. Does not change the file offset.
    ///
    /// This is the unsafe variant of [`pread`].
    ///
    /// # Safety
    ///
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    ///
    /// [`pread`]: #method.pread
    pub async unsafe fn pread_unchecked(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        buf: &mut [u8],
        offset: u64,
        flags: SysReadFlags,
    ) -> Result<usize> {
        let ring = ring.into();

        let data_mut = buf;

        #[cfg(target_os = "redox")]
        let (fd64, data_mut) = {
            let fd64 = u64::try_from(fd).map_err(|_| Error::new(EBADF))?;
            let data_mut = data_mut
                .as_generic_slice_mut(ring.is_primary())
                .ok_or(Error::new(EFAULT))?;

            (fd64, data_mut)
        };

        let prepare_fn = |sqe: &mut SysSqeRef| {
            #[cfg(target_os = "redox")]
            {
                sqe.sys_pread(fd64, data_mut, offset, flags);
            }
            #[cfg(target_os = "linux")]
            {
                let _ = flags;
                sqe.prep_read(fd, data_mut, offset);
                sqe.raw_mut().cmd_flags = uring_sys::cmd_flags {
                    rw_flags: flags.bits() as _,
                };
            }
        };

        let cqe = self.send_with_ctx(ring, ctx, prepare_fn).await?;
        let bytes_read = Self::completion_as_rw_io_result(cqe)? as usize;
        Ok(bytes_read as usize)
    }
    /// Read bytes from a specific offset. Does not change the file offset.
    ///
    /// This is the safe variant of [`pread_unchecked`].
    ///
    /// [`pread_unchecked`]: #method.pread_unchecked
    // TODO: Uninitialized buffers.
    // TODO: AsOffsetLenMut.
    pub async fn pread<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        buf: B,
        offset: u64,
        flags: SysReadFlags,
    ) -> (Result<usize>, B)
    where
        B: GuardedMut<Target = [u8]>,
    {
        let ring = ring.into();
        let mut buf = ManuallyDrop::new(buf);

        let result = unsafe {
            self.pread_unchecked(ring, ctx, fd, buf.borrow_guarded_mut(), offset, flags)
                .await
        };

        (
            result.map(|bytes_read| bytes_read as usize),
            ManuallyDrop::into_inner(buf),
        )
    }

    /// Read bytes from a specific offset, vectored. Does not change the file offset.
    ///
    /// At the moment there is no safe counterpart for this unchecked method, since this passes a
    /// list of buffers, rather than one single buffer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the buffers outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn preadv_unchecked(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        bufs: &mut [SysIoVec],
        offset: u64,
        flags: SysReadFlags,
    ) -> Result<usize> {
        #[cfg(target_os = "redox")]
        let fd64 = u64::try_from(fd).map_err(|_| Error::new(EBADF))?;

        let prepare_fn = |sqe: &mut SysSqeRef| {
            #[cfg(target_os = "redox")]
            {
                sqe.sys_preadv(fd64, bufs, offset, flags);
            }

            #[cfg(target_os = "linux")]
            {
                let bufs_unchecked = {
                    core::slice::from_raw_parts_mut(
                        bufs.as_mut_ptr() as *mut std::io::IoSliceMut,
                        bufs.len(),
                    )
                };
                sqe.prep_read_vectored(fd, bufs_unchecked, offset);

                // NOTE: Linux does indeed seem to support these flags, since it uses rw_flags from
                // sqe directly, to call kiocb_set_rw_flags, which AIO also calls, as well as the
                // regular vfs_read (which in turn is called by the actual read(2) syscall,
                // although only preadv2 and pwritev2 allow the _user_ to set these). And, those
                // flags having the prefix RWF_, for instance RWF_HIPRI, should be settable.
                //
                // However, they are not yet incorporated into liburing, for some reason, so we'll
                // need to set these manually.
                sqe.raw_mut().cmd_flags = uring_sys::cmd_flags {
                    rw_flags: flags.bits() as _,
                };
            }
        };
        let cqe = self.send_with_ctx(ring, ctx, prepare_fn).await?;

        let bytes_read = Self::completion_as_rw_io_result(cqe)?;

        Ok(bytes_read as usize)
    }

    /// Write bytes. Returns the number of bytes written, or zero if no more bytes could be
    /// written.
    ///
    /// This is the unsafe variant of the [`write`] method.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    ///
    /// [`write`]: #method.write
    // TODO: On Linux?
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub async unsafe fn write_unchecked<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        buf: &B,
        flags: WriteFlags,
    ) -> Result<usize>
    where
        B: AsOffsetLen + ?Sized,
    {
        let ring = ring.into();
        let fd64 = u64::try_from(fd).map_err(|_| Error::new(EBADF))?;

        let buf = buf
            .as_generic_slice(ring.is_primary())
            .ok_or(Error::new(EFAULT))?;

        let cqe = self
            .send_with_ctx(ring, ctx, |sqe: &mut SysSqeRef| {
                sqe.sys_write(fd64, buf, flags);
            })
            .await?;

        Self::completion_as_rw_io_result(cqe)
    }
    /// Write bytes. Returns the number of bytes written, or zero if no more bytes could be
    /// written.
    ///
    /// This is the safe variant of the [`write_unchecked`] method.
    ///
    /// [`write_unchecked`]: #method.write_unchecked
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub async fn write<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        buf: B,
        flags: WriteFlags,
    ) -> (Result<usize>, B)
    where
        B: Guarded<Target = [u8]> + AsOffsetLen,
    {
        let ring = ring.into();
        let buf = ManuallyDrop::new(buf);

        let result = unsafe { self.write_unchecked(ring, ctx, fd, &*buf, flags).await };
        (result, ManuallyDrop::into_inner(buf))
    }

    /// Write bytes, vectored.
    ///
    /// At the moment there is no safe counterpart for this unchecked method, since this passes a
    /// list of buffers, rather than one single buffer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the buffers outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    // TODO: Does Linux support system calls that change the file offset, when using io_uring?
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub async unsafe fn writev_unchecked(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        bufs: &[IoVec],
        flags: SysWriteFlags,
    ) -> Result<usize> {
        let fd64 = u64::try_from(fd).map_err(|_| Error::new(EBADF))?;

        let cqe = self
            .send_with_ctx(ring, ctx, |sqe: &mut SysSqeRef| {
                sqe.sys_writev(fd64, bufs, flags);
            })
            .await?;

        Self::completion_as_rw_io_result(cqe)
    }

    /// Write bytes to a specific offset, with an optional set of flags. Does not change the file
    /// offset.
    ///
    /// This is the unsafe variant of the [`pwrite`] method.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    ///
    /// [`pwrite`]: #method.pwrite
    pub async unsafe fn pwrite_unchecked<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        buf: &B,
        offset: u64,
        flags: SysWriteFlags,
    ) -> Result<usize>
    where
        B: AsOffsetLen + ?Sized,
    {
        let ring = ring.into();

        #[cfg(target_os = "redox")]
        let (fd64, buf) = {
            let fd64 = u64::try_from(fd).map_err(|_| Error::new(EBADF))?;
            let buf = buf
                .as_generic_slice(ring.is_primary())
                .ok_or(Error::new(EFAULT))?;

            (fd64, buf)
        };
        #[cfg(target_os = "linux")]
        let buf = buf.slice();

        let prepare_fn = |sqe: &mut SysSqeRef| {
            #[cfg(target_os = "redox")]
            {
                sqe.sys_pwrite(fd64, buf, offset, flags);
            }

            #[cfg(target_os = "linux")]
            {
                let _ = flags;
                sqe.prep_write(fd, buf, offset);
                sqe.raw_mut().cmd_flags = uring_sys::cmd_flags {
                    rw_flags: flags.bits() as _,
                };
            }
        };

        let cqe = { self.send_with_ctx(ring, ctx, prepare_fn).await? };
        let bytes_written = Self::completion_as_rw_io_result(cqe)?;
        Ok(bytes_written as usize)
    }
    /// Write bytes to a specific offset. Does not change the file offset.
    ///
    /// This is the safe variant of the [`pwrite_unchecked`] method.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the buffer outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    ///
    /// [`pwrite_unchecked`]: #method.pwrite_unchecked
    pub async fn pwrite<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        buf: B,
        offset: u64,
        flags: SysWriteFlags,
    ) -> (Result<usize>, B)
    where
        // TODO: Support MaybeUninit via the `ioslice` crate.
        B: Guarded<Target = [u8]> + AsOffsetLen,
    {
        let ring = ring.into();
        let buf = ManuallyDrop::new(buf);

        let result = unsafe {
            self.pwrite_unchecked(ring, ctx, fd, buf.borrow_guarded(), offset, flags)
                .await
        };

        (
            result.map(|bytes_written| bytes_written as usize),
            ManuallyDrop::into_inner(buf),
        )
    }

    /// Write bytes to a specific offset, vectored, with an optional set of flags. Does not change
    /// the file offset.
    ///
    /// At the moment there is no safe counterpart for this unchecked method, since this passes a
    /// list of buffers, rather than one single buffer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the buffers outlive the future using it, and that the buffer is
    /// not reclaimed until the command is either complete or cancelled.
    pub async unsafe fn pwritev_unchecked(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        bufs: &[IoVec],
        offset: u64,
        flags: SysWriteFlags,
    ) -> Result<usize> {
        #[cfg(target_os = "redox")]
        let fd64 = u64::try_from(fd).map_err(|_| Error::new(EBADF))?;

        let prep_fn = move |sqe: &mut SysSqeRef| {
            #[cfg(target_os = "redox")]
            {
                sqe.sys_pwritev(fd64, bufs, offset, flags);
            }
            #[cfg(target_os = "linux")]
            {
                // TODO: Use the `ioslice` crate for safe casts. Otherwise, it will suffice
                // to simply cast the slice unsafely, by reinterpreting its type. They must
                // be valid since this is unsafe.
                let bufs_unchecked = {
                    core::slice::from_raw_parts(
                        bufs.as_ptr() as *const std::io::IoSlice,
                        bufs.len(),
                    )
                };

                sqe.prep_write_vectored(fd as SysFd, bufs_unchecked, offset);
                sqe.raw_mut().cmd_flags = uring_sys::cmd_flags {
                    rw_flags: flags.bits() as _,
                };
            }
        };

        let cqe = self.send_with_ctx(ring, ctx, prep_fn).await?;

        Self::completion_as_rw_io_result(cqe).map(|bytes_written| bytes_written as usize)
    }

    /// "Duplicate" a file descriptor, returning a new one based on the old one.
    ///
    /// # Panics
    ///
    /// This function will panic if the parameter is set, but the flags don't contain
    /// [`DupFlags::PARAM`].
    ///
    /// # Safety
    ///
    /// If the parameter is used, that must point to a slice that is valid for the receiver.
    /// Additionally, that slice must also outlive the lifetime of this future, and if the future
    /// is dropped or forgotten, the slice must not be used afterwards, since that would lead to a
    /// data race.
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub async unsafe fn dup_unchecked<P>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: usize,
        flags: DupFlags,
        param: Option<&P>,
    ) -> Result<usize>
    where
        P: AsOffsetLen + ?Sized,
    {
        self.dup_unchecked_inner(ring, ctx, fd, flags, param).await
    }
    /// "Duplicate" a file descriptor, returning a new one based on the old one.
    ///
    /// This is the safe version of [`dup_unchecked`], that uses an owned guarded buffer for the
    /// parameter, if present. To work around issues with the generic guard type, prefer
    /// [`dup_parameterless`] instead, when the parameter isn't used.
    ///
    /// # Panics
    ///
    /// This will panic if param is some, and the [`DupFlags::PARAM`] isn't set, or vice versa.
    ///
    /// [`dup_parameterless`]: #method.dup_parameterless
    /// [`dup_unchecked`]: #method.dup_unchecked
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub async fn dup<G>(
        &self,
        id: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: usize,
        flags: DupFlags,
        param: Option<G>,
    ) -> Result<(usize, Option<G>)>
    where
        G: Guarded<Target = [u8]> + AsOffsetLen,
    {
        let param = param.map(ManuallyDrop::new);
        let fd = unsafe {
            self.dup_unchecked_inner(id, ctx, fd, flags, param.as_deref())
                .await?
        };
        Ok((fd, param.map(ManuallyDrop::into_inner)))
    }
    /// "Duplicate" a file descriptor, returning a new one based on the old one.
    ///
    /// This function is the same as [`dup`], but without the requirement of specifying a guard
    /// type when it isn't used.
    ///
    /// # Panics
    ///
    /// Since this doesn't pass a parameter, it'll panic if the flags contain [`DupFlags::PARAM`].
    ///
    /// [`dup`]: #method.dup
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub async fn dup_parameterless(
        &self,
        id: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: usize,
        flags: DupFlags,
    ) -> Result<usize> {
        let (fd, _) = self
            .dup(id.into(), ctx, fd, flags, Option::<&'static [u8]>::None)
            .await?;
        Ok(fd)
    }
    #[cfg(target_os = "redox")]
    async unsafe fn dup_unchecked_inner<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: usize,
        flags: DupFlags,
        param: Option<&B>,
    ) -> Result<SysFd>
    where
        B: AsOffsetLen + ?Sized,
    {
        let ring = ring.into();

        let fd64 = u64::try_from(fd).or(Err(Error::new(EBADF)))?;

        let slice = param
            .map(|param| {
                param
                    .as_generic_slice(ring.is_primary())
                    .ok_or(Error::new(EFAULT))
            })
            .transpose()?;

        let prepare_fn = move |sqe: &mut SysSqeRef| {
            sqe.sys_dup(fd64, flags, slice);
        };

        let fut = self.send_with_ctx(ring, ctx, prepare_fn);
        let cqe = fut.await?;

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
    ///
    /// [`MAP_FIXED`]: ../../syscall/flag/struct.MapFlags.html#associatedconstant.MAP_FIXED
    /// [`MAP_FIXED_NOREPLACE`]:
    /// ../../syscall/flag/struct.MapFlags.html#associatedconstant.MAP_FIXED_NOREPLACE
    #[allow(clippy::too_many_arguments)]
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub async unsafe fn mmap2(
        &self,
        // TODO: Simply function parameters with something like MmapInfo?
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
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

        let prep_fn = move |sqe: &mut SysSqeRef| {
            sqe.sys_mmap(fd64, flags, addr_hint64, len64, offset);
        };

        let cqe = self.send_with_ctx(ring, ctx, prep_fn).await?;

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
    /// [`mmap2`]: #method.mmap2
    /// [`MAP_FIXED`]: ../../syscall/flag/struct.MapFlags.html#associatedconstant.MAP_FIXED
    #[cfg(any(doc, target_os = "redox"))]
    #[doc(cfg(target_os = "redox"))]
    pub async unsafe fn mmap(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        flags: MapFlags,
        len: usize,
        offset: u64,
    ) -> Result<*const ()> {
        assert!(!flags.contains(MapFlags::MAP_FIXED));
        self.mmap2(ring, ctx, fd, flags, None, len, offset).await
    }

    /// Accept connections from a socket that has called _bind(2)_.
    ///
    /// # Safety
    // TODO
    #[cfg(any(doc, target_os = "linux"))]
    #[doc(cfg(target_os = "linux"))]
    // TODO: Some way to interface with the redox netstack.
    pub async unsafe fn accept_unchecked(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        sockaddr_storage: Option<&mut iou::sqe::SockAddrStorage>,
        flags: iou::sqe::SockFlag,
    ) -> Result<SysFd> {
        let return_value = self
            .send(ring, move |sqe: &mut SysSqeRef| {
                sqe.set_flags(ctx.sync().linux_sqe_flags());
                sqe.prep_accept(fd, sockaddr_storage, flags)
            })
            .await?;

        // TODO: Proper error code handling.
        Self::completion_as_rw_io_result(return_value).map(|retval| retval as SysFd)
    }
    /// Accept a connection from a socket, without being able to get the address where the incoming
    /// connection originated from.
    #[cfg(any(doc, target_os = "linux"))]
    #[doc(cfg(target_os = "linux"))]
    pub async fn accept_simple(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        flags: iou::sqe::SockFlag,
    ) -> Result<SysFd> {
        // SAFETY: The only safety invariant in accept_unchecked comes from buffer usage, which we
        // completely eliminate here.
        unsafe { self.accept_unchecked(ring, ctx, fd, None, flags) }.await
    }
    // TODO: Wrap the `struct sockaddr` so that it can be known upon future completion, that the
    // address can be read. We would probably take a maybe-uninit (the existing
    // iou::sqe::SockAddrStorage), and return an initialized wrapper with a safe accessor function.
    /// Accept a connection from a socket, with the address buffer being safely protected with a
    /// guard.
    #[cfg(any(doc, target_os = "linux"))]
    #[doc(cfg(target_os = "linux"))]
    pub async fn accept<B>(
        &self,
        ring: impl Into<RingId>,
        ctx: SubmissionContext,
        fd: SysFd,
        sockaddr_storage: Option<B>,
        flags: iou::sqe::SockFlag,
    ) -> Result<(SysFd, Option<B>)>
    where
        B: GuardedMut<Target = iou::sqe::SockAddrStorage> + Unpin,
    {
        match sockaddr_storage {
            Some(sockaddr_storage) => {
                let mut sockaddr_storage = ManuallyDrop::new(sockaddr_storage);

                let prep_fn = |sqe: &mut SysSqeRef| {
                    let sockaddr_storage_ref = (&mut *sockaddr_storage).borrow_guarded_mut();

                    sqe.set_flags(ctx.sync().linux_sqe_flags());
                    unsafe { sqe.prep_accept(fd, Some(sockaddr_storage_ref), flags) }
                };
                let future = unsafe { self.send(ring, prep_fn) };
                let cqe = future.await?;

                let fd = Self::completion_as_rw_io_result(cqe)? as SysFd;
                Ok((fd, Some(ManuallyDrop::into_inner(sockaddr_storage))))
            }
            None => {
                let fd = self.accept_simple(ring, ctx, fd, flags).await?;
                Ok((fd, None))
            }
        }
    }
}

/// An unsafe trait that abstract slices over offset-based addresses, and pointer-based
///
/// This happens as part of a buffer pool; two processes can obviously not share addresses safely,
/// without making sure that their address spaces look similar), and pointer-based addresses (when
/// communicating with the kernel.
pub unsafe trait AsOffsetLen {
    /// Get the offset within a buffer pool that the consumer and producer shares. This is never
    /// called for userspace-to-kernel instances.
    fn offset(&self) -> u64;
    /// Get the length of the slice. This method works for both types. The reason this returns an
    /// Option, is because the type may not be convertible, in which cause `EFAULT` will be
    /// returned.
    fn len(&self) -> Option<u64>;
    /// Get the pointer-based address. This will never be called for userspace-to-userspace rings.
    fn addr(&self) -> usize;

    /// Check whether the slice is empty.
    fn is_empty(&self) -> bool {
        self.len() == Some(0)
    }
}
/// An unsafe trait that abstract slices over offset-based addresses, and pointer based, mutably.
///
/// Refer to [`AsOffsetLen`].
pub unsafe trait AsOffsetLenMut: AsOffsetLen {
    /// Same as [`AsOffsetLen::offset`], but different since the default AsMut impl may point to a
    /// different slice.
    fn offset_mut(&mut self) -> u64;
    /// Same as [`AsOffsetLen::len`], but different since the default AsMut impl may point to a
    /// different slice.
    fn len_mut(&mut self) -> Option<u64>;
    /// Same as [`AsOffsetLen::addr`], but different since the default AsMut impl may point to a
    /// different slice.
    fn addr_mut(&mut self) -> usize;

    /// Same as [`AsOffsetLen::is_empty`], but different since the default AsMut impl may point to
    /// a different slice.
    fn is_empty_mut(&mut self) -> bool {
        self.len_mut() == Some(0)
    }
}

unsafe impl<'a, I, H, E, G, C> AsOffsetLen for crate::memory::BufferSlice<'a, I, E, G, H, C>
where
    I: redox_buffer_pool::Integer + Into<u64>,
    H: redox_buffer_pool::Handle<I, E>,
    E: Copy,
    G: redox_buffer_pool::marker::Marker,
    C: redox_buffer_pool::AsBufferPool<I, H, E>,
{
    fn offset(&self) -> u64 {
        redox_buffer_pool::BufferSlice::offset(self).into()
    }
    fn len(&self) -> Option<u64> {
        Some(redox_buffer_pool::BufferSlice::len(self).into())
    }
    fn addr(&self) -> usize {
        redox_buffer_pool::BufferSlice::as_slice(self).as_ptr() as usize
    }
}
unsafe impl<'a, I, H, E, G, C> AsOffsetLenMut for crate::memory::BufferSlice<'a, I, E, G, H, C>
where
    I: redox_buffer_pool::Integer + Into<u64>,
    H: redox_buffer_pool::Handle<I, E>,
    E: Copy,
    G: redox_buffer_pool::marker::Marker,
    C: redox_buffer_pool::AsBufferPool<I, H, E>,
{
    fn offset_mut(&mut self) -> u64 {
        redox_buffer_pool::BufferSlice::offset(self).into()
    }
    fn len_mut(&mut self) -> Option<u64> {
        Some(redox_buffer_pool::BufferSlice::len(self).into())
    }
    fn addr_mut(&mut self) -> usize {
        redox_buffer_pool::BufferSlice::as_slice_mut(self).as_mut_ptr() as usize
    }
}
mod private2 {
    pub trait Sealed {}
}
impl<T> private2::Sealed for T where T: AsOffsetLen + ?Sized {}

trait AsOffsetLenExt: AsOffsetLen + private2::Sealed {
    fn as_offset_generic_slice(&self) -> Option<GenericSlice<'_>> {
        Some(GenericSlice::from_offset_len(self.offset(), self.len()?))
    }
    fn as_pointer_generic_slice(&self) -> Option<GenericSlice<'_>> {
        Some(GenericSlice::from_offset_len(
            self.addr().try_into().ok()?,
            self.len()?,
        ))
    }
    fn as_generic_slice(&self, is_primary: bool) -> Option<GenericSlice<'_>> {
        if is_primary {
            self.as_pointer_generic_slice()
        } else {
            self.as_offset_generic_slice()
        }
    }
    fn slice(&self) -> &[u8] {
        unsafe {
            core::slice::from_raw_parts(
                self.addr() as *const u8,
                self.len().unwrap().try_into().unwrap(),
            )
        }
    }
}
trait AsOffsetLenMutExt: AsOffsetLenMut + private2::Sealed {
    fn as_offset_generic_slice_mut(&mut self) -> Option<GenericSliceMut<'_>> {
        Some(GenericSliceMut::from_offset_len(
            self.offset_mut(),
            self.len_mut()?,
        ))
    }
    fn as_pointer_generic_slice_mut(&mut self) -> Option<GenericSliceMut<'_>> {
        Some(GenericSliceMut::from_offset_len(
            self.addr_mut().try_into().ok()?,
            self.len_mut()?,
        ))
    }
    fn as_generic_slice_mut(&mut self, is_primary: bool) -> Option<GenericSliceMut<'_>> {
        if is_primary {
            self.as_pointer_generic_slice_mut()
        } else {
            self.as_offset_generic_slice_mut()
        }
    }
    fn slice_mut(&mut self) -> &mut [u8] {
        unsafe {
            core::slice::from_raw_parts_mut(
                self.addr() as *mut u8,
                self.len_mut().unwrap().try_into().unwrap(),
            )
        }
    }
}
impl<T> AsOffsetLenExt for T where T: AsOffsetLen + ?Sized {}
impl<T> AsOffsetLenMutExt for T where T: AsOffsetLenMut + ?Sized {}

macro_rules! slice_like(
    ($type:ty) => {
        unsafe impl AsOffsetLen for $type {
            fn offset(&self) -> u64 {
                panic!("cannot use regular slices for secondary io_urings")
            }
            fn len(&self) -> Option<u64> {
                <[u8]>::len(::core::convert::AsRef::<[u8]>::as_ref(self)).try_into().ok()
            }
            fn addr(&self) -> usize {
                ::core::convert::AsRef::<[u8]>::as_ref(self).as_ptr() as usize
            }
        }
    }
);
macro_rules! slice_like_mut(
    ($type:ty) => {
        unsafe impl AsOffsetLenMut for $type {
            fn offset_mut(&mut self) -> u64 {
                unreachable!()
            }
            fn len_mut(&mut self) -> Option<u64> {
                <[u8]>::len(::core::convert::AsMut::<[u8]>::as_mut(self)).try_into().ok()
            }
            fn addr_mut(&mut self) -> usize {
                ::core::convert::AsMut::<[u8]>::as_mut(self).as_ptr() as usize
            }
        }
    }
);
slice_like!([u8]);
slice_like!(&[u8]);
slice_like!(::std::vec::Vec<u8>);
slice_like!(::std::boxed::Box<[u8]>);
slice_like!(::std::sync::Arc<[u8]>);
slice_like!(::std::rc::Rc<[u8]>);
slice_like!(::std::borrow::Cow<'_, [u8]>);
slice_like!(::std::string::String);
slice_like!(str);
slice_like!(&str);
slice_like!([u8; 0]);
slice_like!(&[u8; 0]);

slice_like_mut!([u8]);
slice_like_mut!(::std::vec::Vec<u8>);
slice_like_mut!(::std::boxed::Box<[u8]>);
slice_like_mut!([u8; 0]);
slice_like!(&mut [u8; 0]);
// (`Arc` is never mutable)
// (`Rc` is never mutable)
// (`Cow` is mutable, but it won't work here)
// (`String` is AsMut, but not for [u8] (due to UTF-8))
// (`str` is AsMut, but not for [u8] (due to UTF-8))

#[cfg(target_os = "linux")]
fn nul_check(slice: &[u8]) {
    assert_eq!(
        slice.last(),
        Some(&0),
        "path passed to open_raw_unchecked_inner was not NUL-terminated"
    );
}

/// Extra information passed to the `open_at` system calls, with parameters such as file descriptor
/// mode, flags, and how the path is resolved (Linux).
#[derive(Debug)]
pub struct OpenInfo {
    #[cfg(target_os = "redox")]
    inner: (u64, OpenFlags),

    #[cfg(target_os = "linux")]
    inner: (iou::sqe::OFlag, iou::sqe::Mode),
}
/// The origin from which the open system call begins. The system call allows a file descriptor
/// that references an open directory, to be the base of the path resolution when opening.
#[derive(Clone, Copy, Debug)]
pub enum OpenFrom {
    /// Open relative paths from the current directory. Absolute paths are not affected by this.
    ///
    /// On Linux, this will make the system calls use the file descriptor placeholder
    /// [`libc::AT_FDCWD`].
    ///
    /// On Redox, this will set a flag to indicate that it is not opening from a file descriptor.
    CurrentDirectory,

    /// Open relative paths based on a directory at the file descriptor.
    At(SysFd),
}

impl Default for OpenFrom {
    fn default() -> Self {
        Self::CurrentDirectory
    }
}

impl Default for OpenInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenInfo {
    /// Use the default parameters, meaning no special flags (other than O_CLOEXEC), and with a
    /// 000-mode.
    pub fn new() -> Self {
        Self {
            #[cfg(target_os = "redox")]
            inner: (0, OpenFlags::empty()),

            #[cfg(target_os = "linux")]
            inner: (iou::sqe::OFlag::O_CLOEXEC, iou::sqe::Mode::empty()),
        }
    }
    /// Set the redox file mode.
    #[cfg(target_os = "redox")]
    pub fn with_redox_mode(self, mode: u64) -> Self {
        Self {
            inner: (mode, self.inner.1),
        }
    }
}
impl OpenFrom {
    /// Get the fd or placeholder which this wrapper corresponds to.
    #[cfg(target_os = "linux")]
    pub fn linux_fd(self) -> SysFd {
        match self {
            Self::CurrentDirectory => libc::AT_FDCWD,
            Self::At(at) => at,
        }
    }
}

enum SendArg<F> {
    #[cfg(target_os = "redox")]
    Stream(FdEventsInitial),
    Single(F),
}

unsafe impl<T> AsOffsetLen for guard_trait::Mapped<T, [u8]>
where
    T: Guarded,
{
    fn offset(&self) -> u64 {
        unreachable!()
    }
    fn len(&self) -> Option<u64> {
        self.get_ref().len().try_into().ok()
    }
    fn addr(&self) -> usize {
        self.get_ref().as_ptr() as usize
    }
}
unsafe impl<T> AsOffsetLen for guard_trait::MappedMut<T, [u8]>
where
    T: GuardedMut,
{
    fn offset(&self) -> u64 {
        unreachable!()
    }
    fn len(&self) -> Option<u64> {
        self.get_ref().len().try_into().ok()
    }
    fn addr(&self) -> usize {
        self.get_ref().as_ptr() as usize
    }
}
unsafe impl<T> AsOffsetLenMut for guard_trait::MappedMut<T, [u8]>
where
    T: GuardedMut,
{
    fn offset_mut(&mut self) -> u64 {
        unreachable!()
    }
    fn len_mut(&mut self) -> Option<u64> {
        self.get_mut().len().try_into().ok()
    }
    fn addr_mut(&mut self) -> usize {
        self.get_mut().as_mut_ptr() as usize
    }
}
unsafe impl<T> AsOffsetLen for guard_trait::AssertSafe<T>
where
    T: ops::Deref<Target = [u8]>,
{
    fn offset(&self) -> u64 {
        unreachable!()
    }
    fn len(&self) -> Option<u64> {
        self.borrow_guarded().len().try_into().ok()
    }
    fn addr(&self) -> usize {
        self.borrow_guarded().as_ptr() as usize
    }
}
unsafe impl<T> AsOffsetLenMut for guard_trait::AssertSafe<T>
where
    T: ops::Deref<Target = [u8]> + ops::DerefMut,
{
    fn offset_mut(&mut self) -> u64 {
        unreachable!()
    }
    fn len_mut(&mut self) -> Option<u64> {
        self.borrow_guarded_mut().len().try_into().ok()
    }
    fn addr_mut(&mut self) -> usize {
        self.borrow_guarded_mut().as_mut_ptr() as usize
    }
}
