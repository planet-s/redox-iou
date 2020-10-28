mod consumer_instance {
    use std::{mem, slice};

    use either::*;
    use parking_lot::RwLock;

    use syscall::data::Map;
    use syscall::error::{EINVAL, EIO};
    use syscall::error::{Error, Result};
    use syscall::flag::MapFlags;
    use syscall::flag::{O_CLOEXEC, O_CREAT, O_RDWR};

    use syscall::io_uring::v1::{
        BrokenRing, CqEntry32, CqEntry64, IoUringCreateFlags, Ring, SqEntry32, SqEntry64,
        CQ_ENTRIES_MMAP_OFFSET, CQ_HEADER_MMAP_OFFSET, SQ_ENTRIES_MMAP_OFFSET,
        SQ_HEADER_MMAP_OFFSET, CURRENT_MINOR, CURRENT_PATCH,
    };
    use syscall::io_uring::{IoUringCreateInfo, IoUringEnterFlags, IoUringVersion};

    use crate::redox::ring::{SpscReceiver, SpscSender};

    #[derive(Debug, Default)]
    struct InstanceBuilderCreateStageInfo {
        minor: Option<u8>,
        patch: Option<u8>,
        flags: Option<IoUringCreateFlags>,
        sq_entry_count: Option<usize>,
        cq_entry_count: Option<usize>,
    }
    #[derive(Debug)]
    struct InstanceBuilderMmapStageInfo {
        create_info: IoUringCreateInfo,
        ringfd: usize,
        sr_virtaddr: Option<usize>,
        se_virtaddr: Option<usize>,
        cr_virtaddr: Option<usize>,
        ce_virtaddr: Option<usize>,
    }
    #[derive(Debug)]
    struct InstanceBuilderAttachStageInfo {
        create_info: IoUringCreateInfo,
        ringfd: usize,
        sr_virtaddr: usize,
        se_virtaddr: usize,
        cr_virtaddr: usize,
        ce_virtaddr: usize,
    }
    #[derive(Debug)]
    enum InstanceBuilderStage {
        Create(InstanceBuilderCreateStageInfo),
        Mmap(InstanceBuilderMmapStageInfo),
        Attach(InstanceBuilderAttachStageInfo),
    }
    /// A builder for consumer instances, that cases care of all necessary initialization steps,
    /// including retrieval of the ring instance file descriptor, mmaps, and finally the attachment
    /// to a scheme or kernel.
    #[derive(Debug)]
    pub struct InstanceBuilder {
        stage: InstanceBuilderStage,
    }
    impl InstanceBuilder {
        /// Create a new instance builder.
        pub const fn new() -> Self {
            Self {
                stage: InstanceBuilderStage::Create(InstanceBuilderCreateStageInfo {
                    minor: None,
                    patch: None,
                    flags: None,
                    sq_entry_count: None,
                    cq_entry_count: None,
                }),
            }
        }

        fn as_create_stage(&mut self) -> Option<&mut InstanceBuilderCreateStageInfo> {
            if let InstanceBuilderStage::Create(ref mut stage_info) = self.stage {
                Some(stage_info)
            } else {
                None
            }
        }
        fn as_mmap_stage(&mut self) -> Option<&mut InstanceBuilderMmapStageInfo> {
            if let InstanceBuilderStage::Mmap(ref mut stage_info) = self.stage {
                Some(stage_info)
            } else {
                None
            }
        }
        fn consume_attach_state(self) -> Option<InstanceBuilderAttachStageInfo> {
            if let InstanceBuilderStage::Attach(stage_info) = self.stage {
                Some(stage_info)
            } else {
                None
            }
        }

        /// Set the SemVer-compatible minor version of the interface to use.
        ///
        /// # Panics
        ///
        /// This method will panic if called after the [`create_instance`] method.
        ///
        /// [`create_instance`]: #method.create_instance
        pub fn with_minor_version(mut self, minor: u8) -> Self {
            self.as_create_stage()
                .expect("cannot set minor version after kernel io_uring instance is created")
                .minor = Some(minor);
            self
        }
        /// Set the SemVer-compatible patch version of the interface to use.
        ///
        /// # Panics
        ///
        /// This method will panic if called after the [`create_instance`] method.
        ///
        /// [`create_instance`]: #method.create_instance
        pub fn with_patch_version(mut self, patch: u8) -> Self {
            self.as_create_stage()
                .expect("cannot set patch version after kernel io_uring instance is created")
                .patch = Some(patch);
            self
        }
        /// Set the flags used when creating the io_uring.
        ///
        /// # Panics
        ///
        /// This method will panic if called after the [`create_instance`] method.
        ///
        /// [`create_instance`]: #method.create_instance
        pub fn with_flags(mut self, flags: IoUringCreateFlags) -> Self {
            self.as_create_stage()
                .expect("cannot set flags after kernel io_uring instance is created")
                .flags = Some(flags);
            self
        }
        /// Set the submission entry count.
        ///
        /// Note that it only makes sense for the count, multiplied by the entry type (depending on
        /// the flags), to be divisible by the page size.
        ///
        /// The count _must_ also be a power of two.  The reason for this is to simplify the
        /// internal ring logic; with an entry count that is a power of two, divisions are much
        /// cheaper, and overflows do not even need to be handled at all.
        ///
        /// # Panics
        ///
        /// This method will panic if called after the [`create_instance`] method, or if the count
        /// is not a power of two.
        ///
        /// [`create_instance`]: #method.create_instance
        pub fn with_submission_entry_count(mut self, sq_entry_count: usize) -> Self {
            assert!(sq_entry_count.is_power_of_two());
            self.as_create_stage()
                .expect("cannot set submission entry count after kernel instance is created")
                .sq_entry_count = Some(sq_entry_count);
            self
        }
        /// Use the recommended submission entry count. This may change in the future, but is
        /// currently 256.
        ///
        /// # Panics
        ///
        /// This method will panic if called after the [`create_instance`] method.
        ///
        /// [`create_instance`]: #method.create_instance
        pub fn with_recommended_submission_entry_count(self) -> Self {
            self.with_submission_entry_count(256)
        }
        /// Use the recommended completion entry count. This may change in the future, but is
        /// currently 256.
        ///
        /// # Panics
        ///
        /// This method will panic if called after the [`create_instance`] method.
        ///
        /// [`create_instance`]: #method.create_instance
        pub fn with_recommended_completion_entry_count(self) -> Self {
            self.with_completion_entry_count(256)
        }
        /// Set the completion entry count. Note that it only makes sense for the count, multiplied
        /// by the entry type (depending on the flags), to be divisible by the page size.
        ///
        /// The number must also be a power of two, to simplify the internal ring logic, mainly for
        /// performance.
        ///
        /// # Panics
        ///
        /// This method will panic if called after the [`create_instance`] method, or if the count
        /// is not a power of two.
        ///
        /// [`create_instance`]: #method.create_instance
        pub fn with_completion_entry_count(mut self, cq_entry_count: usize) -> Self {
            assert!(cq_entry_count.is_power_of_two());
            self.as_create_stage()
                .expect("cannot set completion entry count after kernel instance is created")
                .cq_entry_count = Some(cq_entry_count);
            self
        }
        /// Get the currently-used version, which is the default version unless overriden.
        pub fn version(&self) -> IoUringVersion {
            match &self.stage {
                &InstanceBuilderStage::Create(ref info) => IoUringVersion {
                    major: 1,
                    minor: info.minor.unwrap_or(CURRENT_MINOR),
                    patch: info.patch.unwrap_or(CURRENT_PATCH),
                },
                &InstanceBuilderStage::Mmap(InstanceBuilderMmapStageInfo {
                    ref create_info,
                    ..
                })
                | InstanceBuilderStage::Attach(InstanceBuilderAttachStageInfo {
                    ref create_info,
                    ..
                }) => create_info.version,
            }
        }
        /// Get the currently-specified flags, or an empty set of flags if none have yet been
        /// specified.
        pub fn flags(&self) -> IoUringCreateFlags {
            match &self.stage {
                &InstanceBuilderStage::Create(ref info) => {
                    info.flags.unwrap_or(IoUringCreateFlags::empty())
                }
                &InstanceBuilderStage::Mmap(InstanceBuilderMmapStageInfo {
                    ref create_info,
                    ..
                })
                | InstanceBuilderStage::Attach(InstanceBuilderAttachStageInfo {
                    ref create_info,
                    ..
                }) => IoUringCreateFlags::from_bits(create_info.flags)
                    .expect("invalid io_uring flag bits"),
            }
        }
        /// Get the size in bytes, of the submission entry type.
        pub fn submission_entry_size(&self) -> usize {
            if self.flags().contains(IoUringCreateFlags::BITS_32) {
                mem::size_of::<SqEntry32>()
            } else {
                mem::size_of::<SqEntry64>()
            }
        }
        /// Get the size in bytes, of the completion entry type.
        pub fn completion_entry_size(&self) -> usize {
            if self.flags().contains(IoUringCreateFlags::BITS_32) {
                mem::size_of::<CqEntry32>()
            } else {
                mem::size_of::<CqEntry64>()
            }
        }
        /// Get the number of submission entries, currently specified.
        pub fn submission_entry_count(&self) -> usize {
            match &self.stage {
                InstanceBuilderStage::Create(ref info) => info
                    .sq_entry_count
                    .unwrap_or(4096 / self.submission_entry_size()),
                InstanceBuilderStage::Mmap(ref info) => info.create_info.sq_entry_count,
                InstanceBuilderStage::Attach(ref info) => info.create_info.sq_entry_count,
            }
        }
        /// Get the number of completion entries, currently specified.
        pub fn completion_entry_count(&self) -> usize {
            match &self.stage {
                InstanceBuilderStage::Create(ref info) => info
                    .cq_entry_count
                    .unwrap_or(4096 / self.completion_entry_size()),
                InstanceBuilderStage::Mmap(ref info) => info.create_info.cq_entry_count,
                InstanceBuilderStage::Attach(ref info) => info.create_info.cq_entry_count,
            }
        }
        /// Get the size in bytes, that the ring header ([`Ring`]), will occupy. This is always
        /// rounded up to the page size.
        pub fn ring_header_size(&self) -> usize {
            // TODO: Bring PAGE_SIZE to syscall
            4096
        }
        /// Get the number of bytes that will be occupied for storing the submission entries.
        pub fn submission_entries_bytesize(&self) -> usize {
            (self.submission_entry_count() * self.submission_entry_size() + 4095) / 4096 * 4096
        }
        /// Get the number of bytes that will be occupied for storing the completion entries.
        pub fn completion_entries_bytesize(&self) -> usize {
            (self.submission_entry_count() * self.submission_entry_size() + 4095) / 4096 * 4096
        }
        /// Get the create info that has been used or is going to be used during instance creation,
        /// depending on the current builder stage.
        pub fn create_info(&self) -> IoUringCreateInfo {
            match self.stage {
                InstanceBuilderStage::Create(_) => IoUringCreateInfo {
                    version: self.version(),
                    _rsvd: 0,
                    flags: self.flags().bits(),
                    len: mem::size_of::<IoUringCreateInfo>(),
                    sq_entry_count: self.submission_entry_count(),
                    cq_entry_count: self.completion_entry_count(),
                },
                InstanceBuilderStage::Mmap(ref info) => info.create_info,
                InstanceBuilderStage::Attach(ref info) => info.create_info,
            }
        }
        /// Create an instance based on the current options, transitioning the stage of this
        /// builder from the creating stage, into the mmapping stage.
        pub fn create_instance(mut self) -> Result<Self> {
            let ringfd = syscall::open("io_uring:instance", O_CREAT | O_CLOEXEC | O_RDWR)?;
            let create_info = self.create_info();

            let len = mem::size_of::<IoUringCreateInfo>();
            let bytes_written = syscall::write(ringfd, unsafe {
                slice::from_raw_parts(&create_info as *const _ as *const u8, len)
            })?;

            if bytes_written != len {
                // TODO: Add a real error enum.
                return Err(Error::new(EINVAL));
            }

            self.stage = InstanceBuilderStage::Mmap(InstanceBuilderMmapStageInfo {
                create_info,
                ringfd,
                sr_virtaddr: None,
                se_virtaddr: None,
                cr_virtaddr: None,
                ce_virtaddr: None,
            });

            Ok(self)
        }
        fn mmap(
            mut self,
            name: &str,
            mmap_flags: MapFlags,
            mmap_offset: usize,
            mmap_size: usize,
            f: impl FnOnce(&mut InstanceBuilderMmapStageInfo) -> (bool, &mut Option<usize>),
        ) -> Result<Self> {
            let mmap_stage_info = match self.as_mmap_stage() {
                Some(i) => i,
                None => panic!("mapping {} when not in the mmap stage", name),
            };
            let ringfd = mmap_stage_info.ringfd;

            let (only_addr_is_uninit, addr) = f(mmap_stage_info);

            if addr.is_some() {
                panic!("mapping {} again", name)
            }

            *addr = Some(unsafe {
                syscall::fmap(
                    ringfd,
                    &Map {
                        offset: mmap_offset,
                        size: mmap_size,
                        flags: mmap_flags,
                        address: 0,
                    },
                )?
            });

            if only_addr_is_uninit {
                let mmap_stage_info = if let InstanceBuilderStage::Mmap(info) = self.stage {
                    info
                } else {
                    unreachable!()
                };

                self.stage = InstanceBuilderStage::Attach(InstanceBuilderAttachStageInfo {
                    create_info: mmap_stage_info.create_info,
                    ringfd: mmap_stage_info.ringfd,
                    sr_virtaddr: mmap_stage_info.sr_virtaddr.unwrap(),
                    se_virtaddr: mmap_stage_info.se_virtaddr.unwrap(),
                    cr_virtaddr: mmap_stage_info.cr_virtaddr.unwrap(),
                    ce_virtaddr: mmap_stage_info.ce_virtaddr.unwrap(),
                });
            }

            Ok(self)
        }
        /// Map the submission ring header into memory.
        ///
        /// # Panics
        ///
        /// This will panic if the builder is not currently in the mmapping stage.
        pub fn map_submission_ring_header(self) -> Result<Self> {
            let len = self.ring_header_size();
            self.mmap(
                "the submission ring header",
                MapFlags::MAP_SHARED | MapFlags::PROT_READ | MapFlags::PROT_WRITE,
                SQ_HEADER_MMAP_OFFSET,
                len,
                |stage_info| {
                    (
                        stage_info.se_virtaddr.is_some()
                            && stage_info.cr_virtaddr.is_some()
                            && stage_info.ce_virtaddr.is_some(),
                        &mut stage_info.sr_virtaddr,
                    )
                },
            )
        }
        /// Map the completion ring header into memory.
        ///
        /// # Panics
        ///
        /// This will panic if the builder is not currently in the mmapping stage.
        pub fn map_completion_ring_header(self) -> Result<Self> {
            let len = self.ring_header_size();
            self.mmap(
                "the completion ring header",
                MapFlags::MAP_SHARED | MapFlags::PROT_READ | MapFlags::PROT_WRITE,
                CQ_HEADER_MMAP_OFFSET,
                len,
                |stage_info| {
                    (
                        stage_info.sr_virtaddr.is_some()
                            && stage_info.se_virtaddr.is_some()
                            && stage_info.ce_virtaddr.is_some(),
                        &mut stage_info.cr_virtaddr,
                    )
                },
            )
        }
        /// Map the submission entries into memory.
        ///
        /// # Panics
        ///
        /// This will panic if the builder is not currently in the mmapping stage.
        pub fn map_submission_entries(self) -> Result<Self> {
            let len = self.submission_entries_bytesize();
            self.mmap(
                "the submission entries",
                MapFlags::MAP_SHARED | MapFlags::PROT_WRITE,
                SQ_ENTRIES_MMAP_OFFSET,
                len,
                |stage_info| {
                    (
                        stage_info.sr_virtaddr.is_some()
                            && stage_info.cr_virtaddr.is_some()
                            && stage_info.ce_virtaddr.is_some(),
                        &mut stage_info.se_virtaddr,
                    )
                },
            )
        }
        /// Map the completion entries into memory.
        ///
        /// # Panics
        ///
        /// This will panic if the builder is not currently in the mmapping stage.
        pub fn map_completion_entries(self) -> Result<Self> {
            let len = self.completion_entries_bytesize();
            self.mmap(
                "the completion entries",
                MapFlags::MAP_SHARED | MapFlags::PROT_READ,
                CQ_ENTRIES_MMAP_OFFSET,
                len,
                |stage_info| {
                    (
                        stage_info.sr_virtaddr.is_some()
                            && stage_info.se_virtaddr.is_some()
                            && stage_info.cr_virtaddr.is_some(),
                        &mut stage_info.ce_virtaddr,
                    )
                },
            )
        }
        /// Map all the necessary memory locations; this includes the submission ring header, the
        /// submission entries, the completion ring header, and the completion entries.
        ///
        /// When all these are mmapped (which can also be accomplished by mmapping them
        /// separately), the builder will transition from the mmapping stage, into the attaching
        /// state.
        ///
        /// # Panics
        ///
        /// This will panic if the builder is not currently in the mmapping stage.
        pub fn map_all(self) -> Result<Self> {
            self.map_submission_ring_header()?
                .map_submission_entries()?
                .map_completion_ring_header()?
                .map_completion_entries()
        }
        /// Attach the ring to a kernel, effectively creating a userspace-to-kernel ring.
        ///
        /// When this method is complete, the builder transitions from the attaching state into its
        /// finished state, yielding a ready-to-use consumer instance.
        ///
        /// # Panics
        ///
        /// This method will panic if the builder is not in the attaching state.
        pub fn attach_to_kernel(self) -> Result<Instance> {
            self.attach_inner(b"io_uring:")
        }
        /// Attach the ring to a userspace scheme, or the kernel with the scheme name "io_uring:"
        /// (in which case [`attach_to_kernel`] is preferred). If this attaches to a userspace
        /// scheme, the kernel will RPC into that process, with the [`SYS_RECV_IORING`] scheme
        /// handler, creating a userspace-to-userspace ring.
        ///
        /// When this method is complete, the builder transitions from the attaching state into its
        /// finished state, yielding a ready-to-use consumer instance.
        ///
        /// # Panics
        ///
        /// This method will panic if the builder is not in the attaching state.
        ///
        /// [`attach_to_kernel`]: #method.attach_to_kernel
        /// [`SYS_RECV_IORING`]: ../../syscall/number/constant.SYS_RECV_IORING.html
        pub fn attach<N: AsRef<[u8]>>(self, scheme_name: N) -> Result<Instance> {
            self.attach_inner(scheme_name.as_ref())
        }
        fn attach_inner(self, scheme_name: &[u8]) -> Result<Instance> {
            let kernel = scheme_name == b"io_uring:";

            let sq_entry_count = self.submission_entry_count();
            let cq_entry_count = self.completion_entry_count();

            assert!(sq_entry_count.is_power_of_two());
            assert!(cq_entry_count.is_power_of_two());

            let sq_log2_entry_count = sq_entry_count.trailing_zeros();
            let cq_log2_entry_count = cq_entry_count.trailing_zeros();

            let init_flags = self.flags();
            let attach_info = self
                .consume_attach_state()
                .expect("attaching an io_uring before the builder was in its attach stage");

            syscall::io_uring_attach(attach_info.ringfd, scheme_name)?;

            fn init_sender<Sqe>(
                info: &InstanceBuilderAttachStageInfo,
                sq_log2_entry_count: u32,
            ) -> SpscSender<Sqe> {
                unsafe {
                    SpscSender::from_raw(
                        info.sr_virtaddr as *const Ring<Sqe>,
                        4096, // TODO
                        info.se_virtaddr as *mut Sqe,
                        sq_log2_entry_count,
                    )
                }
            }
            fn init_receiver<Cqe>(
                info: &InstanceBuilderAttachStageInfo,
                cq_log2_entry_count: u32,
            ) -> SpscReceiver<Cqe> {
                unsafe {
                    SpscReceiver::from_raw(
                        info.cr_virtaddr as *const Ring<Cqe>,
                        4096, // TODO
                        info.ce_virtaddr as *const Cqe,
                        cq_log2_entry_count,
                    )
                }
            }

            Ok(Instance {
                with_kernel: kernel,
                ringfd: attach_info.ringfd,
                sender: RwLock::new(if init_flags.contains(IoUringCreateFlags::BITS_32) {
                    GenericSender::Bits32(init_sender(&attach_info, sq_log2_entry_count))
                } else {
                    GenericSender::Bits64(init_sender(&attach_info, sq_log2_entry_count))
                }),
                receiver: RwLock::new(if init_flags.contains(IoUringCreateFlags::BITS_32) {
                    GenericReceiver::Bits32(init_receiver(&attach_info, cq_log2_entry_count))
                } else {
                    GenericReceiver::Bits64(init_receiver(&attach_info, cq_log2_entry_count))
                }),
            })
        }
    }
    impl Default for InstanceBuilder {
        fn default() -> Self {
            Self::new()
        }
    }
    /// The possible different types of senders (which for consumers, are always submission rings),
    /// with either 32-bit or 64-bit entry types.
    #[derive(Debug)]
    pub enum GenericSender {
        /// A sender using [`SqEntry32`], which is uncommon, but lightweight.
        Bits32(SpscSender<SqEntry32>),
        /// A sender using [`SqEntry64`], which is the recommended as it is more versatile, and
        /// allows larger parameters, but is heavier to use.
        Bits64(SpscSender<SqEntry64>),
    }
    impl GenericSender {
        /// Check whether the 32-bit entry type is used.
        pub fn is_32(&self) -> bool {
            matches!(self, Self::Bits32(_))
        }
        /// Check whether the 64-bit entry type is used.
        pub fn is_64(&self) -> bool {
            matches!(self, Self::Bits64(_))
        }
        /// Cast this into a sender with the 32-bit entry type, if it had that type, by an
        /// immutable reference.
        pub fn as_32(&self) -> Option<&SpscSender<SqEntry32>> {
            match self {
                Self::Bits32(ref s) => Some(s),
                _ => None,
            }
        }
        /// Cast this into a sender with the 64-bit entry type, if it had that type, by an
        /// immutable reference.
        pub fn as_64(&self) -> Option<&SpscSender<SqEntry64>> {
            match self {
                Self::Bits64(ref s) => Some(s),
                _ => None,
            }
        }
        /// Cast this into a sender with the 32-bit entry type, if it had that type, by a mutable
        /// reference.
        pub fn as_32_mut(&mut self) -> Option<&mut SpscSender<SqEntry32>> {
            match self {
                Self::Bits32(ref mut s) => Some(s),
                _ => None,
            }
        }
        /// Cast this into a sender with the 64-bit entry type, if it had that type, by a mutable
        /// reference.
        pub fn as_64_mut(&mut self) -> Option<&mut SpscSender<SqEntry64>> {
            match self {
                Self::Bits64(ref mut s) => Some(s),
                _ => None,
            }
        }
        /// Notify ourselves (the sender) that new entries can be pushed, by pretending that
        /// entries have been popped.
        pub fn notify_self_about_pop(&self) {
            match self {
                Self::Bits32(ref sender32) => sender32.notify_self_about_pop(),
                Self::Bits64(ref sender64) => sender64.notify_self_about_pop(),
            }
        }
        /// Notify the receiver that new entries have been pushed.
        pub fn notify_about_push(&self) {
            match self {
                Self::Bits32(ref sender32) => sender32.notify_about_push(),
                Self::Bits64(ref sender64) => sender64.notify_about_push(),
            }
        }
    }

    /// The possible entry types that a consumer instance can use, for the receiver of a completion
    /// ring.
    #[derive(Debug)]
    pub enum GenericReceiver {
        /// A receiver with [`CqEntry32`], which is uncommon, but lightweight.
        Bits32(SpscReceiver<CqEntry32>),
        /// A receiver with [`CqEntry64`], which is more versatile and allows larger parameters,
        /// but is heavier and takes up more space.
        Bits64(SpscReceiver<CqEntry64>),
    }
    impl GenericReceiver {
        /// Check whether the the receiver uses the 32-bit entry type.
        pub fn is_32(&self) -> bool {
            matches!(self, Self::Bits32(_))
        }
        /// Check whether the the receiver uses the 64-bit entry type.
        pub fn is_64(&self) -> bool {
            matches!(self, Self::Bits64(_))
        }
        /// Cast this entry type into a sender with the 32-bit entry type, if it had that type, by
        /// an immutable reference.
        pub fn as_32(&self) -> Option<&SpscReceiver<CqEntry32>> {
            match self {
                Self::Bits32(ref s) => Some(s),
                _ => None,
            }
        }
        /// Cast this entry type into a sender with the 64-bit entry type, if it had that type, by
        /// an immutable reference.
        pub fn as_64(&self) -> Option<&SpscReceiver<CqEntry64>> {
            match self {
                Self::Bits64(ref s) => Some(s),
                _ => None,
            }
        }
        /// Cast this entry type into a sender with the 32-bit entry type, if it had that type, by
        /// a mutable reference.
        pub fn as_32_mut(&mut self) -> Option<&mut SpscReceiver<CqEntry32>> {
            match self {
                Self::Bits32(ref mut s) => Some(s),
                _ => None,
            }
        }
        /// Cast this entry type into a sender with the 64-bit entry type, if it had that type, by
        /// a mutable reference.
        pub fn as_64_mut(&mut self) -> Option<&mut SpscReceiver<CqEntry64>> {
            match self {
                Self::Bits64(ref mut s) => Some(s),
                _ => None,
            }
        }
        /// Notify to ourselves (the receiver) that there are new entries, by pretending that a
        /// push has occured when it has not.
        pub fn notify_self_about_push(&self) {
            match self {
                Self::Bits32(ref sender32) => sender32.notify_self_about_push(),
                Self::Bits64(ref sender64) => sender64.notify_self_about_push(),
            }
        }
        /// Notify the other side of the ring, that this receives has popped entries.
        pub fn notify_about_pop(&self) {
            match self {
                Self::Bits32(ref sender32) => sender32.notify_about_pop(),
                Self::Bits64(ref sender64) => sender64.notify_about_pop(),
            }
        }
    }

    /// A wrapper for consumer instances, that provides convenient setup and management.
    #[derive(Debug)]
    pub struct Instance {
        ringfd: usize,
        sender: RwLock<GenericSender>,
        receiver: RwLock<GenericReceiver>,
        with_kernel: bool,
    }
    impl Instance {
        /// Create a new consumer instance builder, that will build this type.
        #[inline]
        pub const fn builder() -> InstanceBuilder {
            InstanceBuilder::new()
        }
        fn deinit(&mut self) -> Result<()> {
            syscall::close(self.ringfd)?;
            Ok(())
        }
        /// Retrieve the underlying lock protecting the sender.
        #[inline]
        pub const fn sender(&self) -> &RwLock<GenericSender> {
            &self.sender
        }
        /// Retrieve the underlying lock protecting the receiver.
        #[inline]
        pub const fn receiver(&self) -> &RwLock<GenericReceiver> {
            &self.receiver
        }
        /// Close the instance, causing the kernel to shut down the ring if it wasn't already. This
        /// is equivalent to the Drop handler, but is recommended in lieu, as it allows getting a
        /// potential error code rather than silently dropping the error.
        #[cold]
        pub fn close(mut self) -> Result<()> {
            self.deinit()?;
            mem::forget(self);
            Ok(())
        }
        /// Get the file descriptor that represents the io_uring instance by the kernel.
        #[inline]
        pub const fn ringfd(&self) -> usize {
            self.ringfd
        }
        /// Wait for the `io_uring` to be able to pop additional completion entries, while
        /// giving the kernel some time for processing submission entries if needed (if
        /// attached to the kernel).
        ///
        /// Allows giving a minimum of completion events before notification, through
        /// `min_complete`.
        #[inline]
        pub fn enter(
            &self,
            min_complete: usize,
            min_submit: usize,
            flags: IoUringEnterFlags,
        ) -> Result<usize> {
            syscall::io_uring_enter(self.ringfd, min_complete, min_submit, flags)
        }
        /// Call `SYS_ENTER_IORING` just like [`enter`] does, but with the `ONLY_NOTIFY` flag. This
        /// will not cause the syscall to block (even though it may take some time), but only
        /// notify the waiting context.
        ///
        /// [`enter`]: #method.enter
        #[inline]
        pub fn enter_for_notification(&self) -> Result<usize> {
            self.enter(0, 0, IoUringEnterFlags::ONLY_NOTIFY)
        }
        /// Check if the instance is attached to the kernel, or to a userspace process.
        #[inline]
        pub const fn is_attached_to_kernel(&self) -> bool {
            // TODO: Add this functionality to producers too.
            self.with_kernel
        }

        /// Retrieve the number of free SQEs, that can be _pushed_.
        #[inline]
        pub fn sq_free_entry_count(&self) -> Result<usize, BrokenRing> {
            match &*self.sender.read() {
                GenericSender::Bits32(ref sender) => sender.free_entry_count(),
                GenericSender::Bits64(ref sender) => sender.free_entry_count(),
            }
        }
        /// Retrieve the number of free SQEs, that can be _popped_.
        #[inline]
        pub fn sq_available_entry_count(&self) -> Result<usize, BrokenRing> {
            match &*self.sender.read() {
                GenericSender::Bits32(ref sender) => sender.available_entry_count(),
                GenericSender::Bits64(ref sender) => sender.available_entry_count(),
            }
        }
        /// Retrieve the number of free CQEs, that can be _pushed_.
        #[inline]
        pub fn cq_free_entry_count(&self) -> Result<usize, BrokenRing> {
            match &*self.receiver.read() {
                GenericReceiver::Bits32(ref receiver) => receiver.free_entry_count(),
                GenericReceiver::Bits64(ref receiver) => receiver.free_entry_count(),
            }
        }
        /// Retrieve the number of available CQEs, that can be _popped_.
        #[inline]
        pub fn cq_available_entry_count(&self) -> Result<usize, BrokenRing> {
            match &*self.receiver.read() {
                GenericReceiver::Bits32(ref receiver) => receiver.available_entry_count(),
                GenericReceiver::Bits64(ref receiver) => receiver.available_entry_count(),
            }
        }
    }
    impl Drop for Instance {
        #[cold]
        fn drop(&mut self) {
            let _ = self.deinit();
        }
    }
    impl PartialEq for Instance {
        #[inline]
        fn eq(&self, other: &Self) -> bool {
            self.ringfd == other.ringfd
        }
    }
    impl Eq for Instance {}
}
pub use consumer_instance::{
    GenericReceiver as ConsumerGenericReceiver, GenericSender as ConsumerGenericSender,
    Instance as ConsumerInstance, InstanceBuilder as ConsumerInstanceBuilder,
};

mod producer_instance {
    use either::*;
    use parking_lot::RwLock;

    use syscall::io_uring::v1::{CqEntry32, CqEntry64, Ring, SqEntry32, SqEntry64};
    use syscall::io_uring::{IoUringRecvFlags, IoUringRecvInfo};

    use syscall::error::{Error, Result};
    use syscall::error::{EINVAL, ENOSYS};

    use crate::redox::ring::{SpscReceiver, SpscSender};

    /// The possible different types of senders (which for producers, are always completion rings)
    /// for producer instances, with either 32-bit or 64-bit completion entries.
    #[derive(Debug)]
    pub enum GenericSender {
        /// A sender using [`CqEntry32`], which is uncommon but lightweight (16 bytes).
        Bits32(SpscSender<CqEntry32>),
        /// A sender using [`CqEntry64`], which is more versatile and the recommended, but heavier
        /// (32 bytes).
        Bits64(SpscSender<CqEntry64>),
    }
    impl GenericSender {
        /// Cast this enum into the 32-bit variant, if it currently has that value, by a shared
        /// reference.
        pub fn as_32(&self) -> Option<&SpscSender<CqEntry32>> {
            if let Self::Bits32(ref recv) = self {
                Some(recv)
            } else {
                None
            }
        }
        /// Cast this enum into the 32-bit variant, if it currently has that value, by an exclusive
        /// reference.
        pub fn as_32_mut(&mut self) -> Option<&mut SpscSender<CqEntry32>> {
            if let Self::Bits32(ref mut recv) = self {
                Some(recv)
            } else {
                None
            }
        }
        /// Cast this enum into the 64-bit variant, if it currently has that value, by a shared
        /// reference.
        pub fn as_64(&self) -> Option<&SpscSender<CqEntry64>> {
            if let Self::Bits64(ref recv) = self {
                Some(recv)
            } else {
                None
            }
        }
        /// Cast this enum into the 64-bit variant, if it currently has that value, by an exclusive
        /// reference.
        pub fn as_64_mut(&mut self) -> Option<&mut SpscSender<CqEntry64>> {
            if let Self::Bits64(ref mut recv) = self {
                Some(recv)
            } else {
                None
            }
        }
    }
    /// The possible different types of receives (which for producers, are always submission
    /// rings), for producers.
    #[derive(Debug)]
    pub enum GenericReceiver {
        /// A receiver using [`SqEntry32`], which is uncommon but lightweight.
        Bits32(SpscReceiver<SqEntry32>),
        /// A receiver using [`SqEntry64`], which is more versatile (accepts 64-bit pointers more
        /// often, for instance), but heavier.
        Bits64(SpscReceiver<SqEntry64>),
    }
    impl GenericReceiver {
        /// Cast this enum into the 32-bit variant, if it currently has that value, by a shared
        /// reference.
        pub fn as_32(&self) -> Option<&SpscReceiver<SqEntry32>> {
            if let Self::Bits32(ref recv) = self {
                Some(recv)
            } else {
                None
            }
        }
        /// Cast this enum into the 32-bit variant, if it currently has that value, by an exclusive
        /// reference.
        pub fn as_32_mut(&mut self) -> Option<&mut SpscReceiver<SqEntry32>> {
            if let Self::Bits32(ref mut recv) = self {
                Some(recv)
            } else {
                None
            }
        }
        /// Cast this enum into the 64-bit variant, if it currently has that value, by a shared
        /// reference.
        pub fn as_64(&self) -> Option<&SpscReceiver<SqEntry64>> {
            if let Self::Bits64(ref recv) = self {
                Some(recv)
            } else {
                None
            }
        }
        /// Cast this enum into the 64-bit variant, if it currently has that value, by an exclusive
        /// reference.
        pub fn as_64_mut(&mut self) -> Option<&mut SpscReceiver<SqEntry64>> {
            if let Self::Bits64(ref mut recv) = self {
                Some(recv)
            } else {
                None
            }
        }
    }

    /// A wrapper type for producer instances, providing convenient management all the way from the
    /// [`SYS_RECV_IORING`] handler, to deinitialization.
    ///
    /// [`SYS_RECV_IORING`]: ../../syscall/number/constant.SYS_RECV_IORING.html
    #[derive(Debug)]
    pub struct Instance {
        sender: RwLock<GenericSender>,
        receiver: RwLock<GenericReceiver>,
        ringfd: usize,
        with_kernel: bool,
    }

    impl Instance {
        /// Create a new instance, from the info that was part of the [`SYS_RECV_IORING`] RPC from
        /// the kernel.
        ///
        /// [`SYS_RECV_IORING`]: ../../syscall/number/constant.SYS_RECV_IORING.html
        #[cold]
        pub fn new(recv_info: &IoUringRecvInfo) -> Result<Self> {
            if recv_info.version.major != 1 {
                return Err(Error::new(ENOSYS));
            } // TODO: Better error code
            let flags = IoUringRecvFlags::from_bits(recv_info.flags).ok_or(Error::new(EINVAL))?;

            fn init_sender<C>(info: &IoUringRecvInfo) -> SpscSender<C> {
                unsafe {
                    SpscSender::from_raw(
                        info.cr_virtaddr as *const Ring<C>,
                        4096, // TODO
                        info.ce_virtaddr as *mut C,
                        {
                            assert!(info.sq_entry_count.is_power_of_two());
                            info.sq_entry_count.trailing_zeros()
                        },
                    )
                }
            }
            fn init_receiver<S>(info: &IoUringRecvInfo) -> SpscReceiver<S> {
                unsafe {
                    SpscReceiver::from_raw(
                        info.sr_virtaddr as *const Ring<S>,
                        4096, // TODO
                        info.se_virtaddr as *mut S,
                        {
                            assert!(info.cq_entry_count.is_power_of_two());
                            info.cq_entry_count.trailing_zeros()
                        },
                    )
                }
            }

            Ok(Self {
                sender: RwLock::new(if flags.contains(IoUringRecvFlags::BITS_32) {
                    GenericSender::Bits32(init_sender(recv_info))
                } else {
                    GenericSender::Bits64(init_sender(recv_info))
                }),
                receiver: RwLock::new(if flags.contains(IoUringRecvFlags::BITS_32) {
                    GenericReceiver::Bits32(init_receiver(recv_info))
                } else {
                    GenericReceiver::Bits64(init_receiver(recv_info))
                }),
                ringfd: recv_info.producerfd,
                with_kernel: flags.contains(IoUringRecvFlags::FROM_KERNEL),
            })
        }
        /// Get the lock protecting the sender.
        #[inline]
        pub const fn sender(&self) -> &RwLock<GenericSender> {
            &self.sender
        }
        /// Get the lock protecting the receiver.
        #[inline]
        pub const fn receiver(&self) -> &RwLock<GenericReceiver> {
            &self.receiver
        }
        /// Get the file descriptor that is tied to the instance and the rings, by the kernel.
        #[inline]
        pub const fn ringfd(&self) -> usize {
            self.ringfd
        }
    }
}
pub use producer_instance::{
    GenericReceiver as ProducerGenericReceiver, GenericSender as ProducerGenericSender,
    Instance as ProducerInstance,
};
