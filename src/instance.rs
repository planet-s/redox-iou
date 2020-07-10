mod consumer_instance {
    use std::{mem, slice};

    use syscall::data::Map;
    use syscall::error::EINVAL;
    use syscall::error::{Error, Result};
    use syscall::flag::MapFlags;
    use syscall::flag::{O_CLOEXEC, O_CREAT, O_RDWR};

    use syscall::io_uring::v1::{
        CqEntry32, CqEntry64, IoUringCreateFlags, Ring, SqEntry32, SqEntry64,
    };
    use syscall::io_uring::v1::{
        CQ_ENTRIES_MMAP_OFFSET, CQ_HEADER_MMAP_OFFSET, SQ_ENTRIES_MMAP_OFFSET,
        SQ_HEADER_MMAP_OFFSET,
    };
    use syscall::io_uring::v1::{CURRENT_MINOR, CURRENT_PATCH};
    use syscall::io_uring::{IoUringCreateInfo, IoUringEnterFlags, IoUringVersion};

    use crate::ring::{SpscReceiver, SpscSender};

    #[derive(Default)]
    struct InstanceBuilderCreateStageInfo {
        minor: Option<u8>,
        patch: Option<u8>,
        flags: Option<IoUringCreateFlags>,
        sq_entry_count: Option<usize>,
        cq_entry_count: Option<usize>,
    }
    struct InstanceBuilderMmapStageInfo {
        create_info: IoUringCreateInfo,
        ringfd: usize,
        sr_virtaddr: Option<usize>,
        se_virtaddr: Option<usize>,
        cr_virtaddr: Option<usize>,
        ce_virtaddr: Option<usize>,
    }
    struct InstanceBuilderAttachStageInfo {
        create_info: IoUringCreateInfo,
        ringfd: usize,
        sr_virtaddr: usize,
        se_virtaddr: usize,
        cr_virtaddr: usize,
        ce_virtaddr: usize,
    }
    enum InstanceBuilderStage {
        Create(InstanceBuilderCreateStageInfo),
        Mmap(InstanceBuilderMmapStageInfo),
        Attach(InstanceBuilderAttachStageInfo),
    }
    pub struct InstanceBuilder {
        stage: InstanceBuilderStage,
    }
    impl InstanceBuilder {
        pub fn new() -> Self {
            Self {
                stage: InstanceBuilderStage::Create(InstanceBuilderCreateStageInfo::default()),
            }
        }

        fn as_create_stage(&mut self) -> Option<&mut InstanceBuilderCreateStageInfo> {
            if let &mut InstanceBuilderStage::Create(ref mut stage_info) = &mut self.stage {
                Some(stage_info)
            } else {
                None
            }
        }
        fn as_mmap_stage(&mut self) -> Option<&mut InstanceBuilderMmapStageInfo> {
            if let &mut InstanceBuilderStage::Mmap(ref mut stage_info) = &mut self.stage {
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

        pub fn with_minor_version(mut self, minor: u8) -> Self {
            self.as_create_stage()
                .expect("cannot set minor version after kernel io_uring instance is created")
                .minor = Some(minor);
            self
        }
        pub fn with_patch_version(mut self, patch: u8) -> Self {
            self.as_create_stage()
                .expect("cannot set patch version after kernel io_uring instance is created")
                .patch = Some(patch);
            self
        }
        pub fn with_flags(mut self, flags: IoUringCreateFlags) -> Self {
            self.as_create_stage()
                .expect("cannot set flags after kernel io_uring instance is created")
                .flags = Some(flags);
            self
        }
        pub fn with_submission_entry_count(mut self, sq_entry_count: usize) -> Self {
            self.as_create_stage()
                .expect("cannot set submission entry count after kernel instance is created")
                .sq_entry_count = Some(sq_entry_count);
            self
        }
        pub fn with_recommended_submission_entry_count(self) -> Self {
            self.with_submission_entry_count(256)
        }
        pub fn with_recommended_completion_entry_count(self) -> Self {
            self.with_completion_entry_count(256)
        }
        pub fn with_completion_entry_count(mut self, cq_entry_count: usize) -> Self {
            self.as_create_stage()
                .expect("cannot set completion entry count after kernel instance is created")
                .cq_entry_count = Some(cq_entry_count);
            self
        }
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
        pub fn submission_entry_size(&self) -> usize {
            if self.flags().contains(IoUringCreateFlags::BITS_32) {
                mem::size_of::<SqEntry32>()
            } else {
                mem::size_of::<SqEntry64>()
            }
        }
        pub fn completion_entry_size(&self) -> usize {
            if self.flags().contains(IoUringCreateFlags::BITS_32) {
                mem::size_of::<CqEntry32>()
            } else {
                mem::size_of::<CqEntry64>()
            }
        }
        pub fn submission_entry_count(&self) -> usize {
            match &self.stage {
                InstanceBuilderStage::Create(ref info) => info
                    .sq_entry_count
                    .unwrap_or(4096 / self.submission_entry_size()),
                InstanceBuilderStage::Mmap(ref info) => info.create_info.sq_entry_count,
                InstanceBuilderStage::Attach(ref info) => info.create_info.sq_entry_count,
            }
        }
        pub fn completion_entry_count(&self) -> usize {
            match &self.stage {
                InstanceBuilderStage::Create(ref info) => info
                    .cq_entry_count
                    .unwrap_or(4096 / self.completion_entry_size()),
                InstanceBuilderStage::Mmap(ref info) => info.create_info.cq_entry_count,
                InstanceBuilderStage::Attach(ref info) => info.create_info.cq_entry_count,
            }
        }
        pub fn ring_header_size(&self) -> usize {
            // TODO: Bring PAGE_SIZE to syscall
            4096
        }
        pub fn submission_entries_bytesize(&self) -> usize {
            (self.submission_entry_count() * self.submission_entry_size() + 4095) / 4096 * 4096
        }
        pub fn completion_entries_bytesize(&self) -> usize {
            (self.submission_entry_count() * self.submission_entry_size() + 4095) / 4096 * 4096
        }
        pub fn create_info(&self) -> IoUringCreateInfo {
            match &self.stage {
                &InstanceBuilderStage::Create(_) => IoUringCreateInfo {
                    version: self.version(),
                    _rsvd: 0,
                    flags: self.flags().bits(),
                    len: mem::size_of::<IoUringCreateInfo>(),
                    sq_entry_count: self.submission_entry_count(),
                    cq_entry_count: self.completion_entry_count(),
                },
                &InstanceBuilderStage::Mmap(ref info) => info.create_info,
                &InstanceBuilderStage::Attach(ref info) => info.create_info,
            }
        }
        pub fn create_instance(mut self) -> Result<Self> {
            let ringfd = syscall::open("io_uring:", O_CREAT | O_CLOEXEC | O_RDWR)?;
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
        pub fn map_all(self) -> Result<Self> {
            self.map_submission_ring_header()?
                .map_submission_entries()?
                .map_completion_ring_header()?
                .map_completion_entries()
        }
        pub fn attach<N: AsRef<str>>(self, scheme_name: N) -> Result<Instance> {
            self.attach_raw(scheme_name.as_ref().as_bytes())
        }
        pub fn attach_to_kernel(self) -> Result<Instance> {
            self.attach_raw(b":")
        }
        pub fn attach_raw<N: AsRef<[u8]>>(self, scheme_name: N) -> Result<Instance> {
            let init_flags = self.flags();
            let attach_info = self
                .consume_attach_state()
                .expect("attaching an io_uring before the builder was in its attach stage");

            syscall::attach_iouring(attach_info.ringfd, scheme_name.as_ref())?;

            fn init_sender<S>(info: &InstanceBuilderAttachStageInfo) -> SpscSender<S> {
                unsafe {
                    SpscSender::from_raw(
                        info.sr_virtaddr as *const Ring<S>,
                        info.se_virtaddr as *mut S,
                    )
                }
            }
            fn init_receiver<C>(info: &InstanceBuilderAttachStageInfo) -> SpscReceiver<C> {
                unsafe {
                    SpscReceiver::from_raw(
                        info.cr_virtaddr as *const Ring<C>,
                        info.ce_virtaddr as *const C,
                    )
                }
            }

            Ok(Instance {
                ringfd: attach_info.ringfd,
                sender: if init_flags.contains(IoUringCreateFlags::BITS_32) {
                    GenericSender::Bits32(init_sender(&attach_info))
                } else {
                    GenericSender::Bits64(init_sender(&attach_info))
                },
                receiver: if init_flags.contains(IoUringCreateFlags::BITS_32) {
                    GenericReceiver::Bits32(init_receiver(&attach_info))
                } else {
                    GenericReceiver::Bits64(init_receiver(&attach_info))
                },
            })
        }
    }
    #[derive(Debug)]
    pub enum GenericSender {
        Bits32(SpscSender<SqEntry32>),
        Bits64(SpscSender<SqEntry64>),
    }
    impl GenericSender {
        pub fn is_32(&self) -> bool {
            if let Self::Bits32(_) = self {
                true
            } else {
                false
            }
        }
        pub fn is_64(&self) -> bool {
            if let Self::Bits64(_) = self {
                true
            } else {
                false
            }
        }
        pub fn as_32(&self) -> Option<&SpscSender<SqEntry32>> {
            match self {
                &Self::Bits32(ref s) => Some(s),
                _ => None,
            }
        }
        pub fn as_64(&self) -> Option<&SpscSender<SqEntry64>> {
            match self {
                &Self::Bits64(ref s) => Some(s),
                _ => None,
            }
        }
        pub fn as_32_mut(&mut self) -> Option<&mut SpscSender<SqEntry32>> {
            match self {
                Self::Bits32(ref mut s) => Some(s),
                _ => None,
            }
        }
        pub fn as_64_mut(&mut self) -> Option<&mut SpscSender<SqEntry64>> {
            match self {
                Self::Bits64(ref mut s) => Some(s),
                _ => None,
            }
        }
    }

    #[derive(Debug)]
    pub enum GenericReceiver {
        Bits32(SpscReceiver<CqEntry32>),
        Bits64(SpscReceiver<CqEntry64>),
    }
    impl GenericReceiver {
        pub fn is_32(&self) -> bool {
            if let Self::Bits32(_) = self {
                true
            } else {
                false
            }
        }
        pub fn is_64(&self) -> bool {
            if let Self::Bits64(_) = self {
                true
            } else {
                false
            }
        }
        pub fn as_32(&self) -> Option<&SpscReceiver<CqEntry32>> {
            match self {
                &Self::Bits32(ref s) => Some(s),
                _ => None,
            }
        }
        pub fn as_64(&self) -> Option<&SpscReceiver<CqEntry64>> {
            match self {
                &Self::Bits64(ref s) => Some(s),
                _ => None,
            }
        }
        pub fn as_32_mut(&mut self) -> Option<&mut SpscReceiver<CqEntry32>> {
            match self {
                &mut Self::Bits32(ref mut s) => Some(s),
                _ => None,
            }
        }
        pub fn as_64_mut(&mut self) -> Option<&mut SpscReceiver<CqEntry64>> {
            match self {
                &mut Self::Bits64(ref mut s) => Some(s),
                _ => None,
            }
        }
    }

    #[derive(Debug)]
    pub struct Instance {
        ringfd: usize,
        // TODO: Add finer-grained locks here, when lock_api can be used in the kernel.
        sender: GenericSender,
        receiver: GenericReceiver,
    }
    impl Instance {
        fn deinit(&mut self) -> Result<()> {
            syscall::close(self.ringfd)?;
            Ok(())
        }
        pub fn sender_mut(&mut self) -> &mut GenericSender {
            &mut self.sender
        }
        pub fn receiver_mut(&mut self) -> &mut GenericReceiver {
            &mut self.receiver
        }
        pub fn sender(&self) -> &GenericSender {
            &self.sender
        }
        pub fn receiver(&self) -> &GenericReceiver {
            &self.receiver
        }
        pub fn close(mut self) -> Result<()> {
            self.deinit()?;
            mem::forget(self);
            Ok(())
        }
        pub fn ringfd(&self) -> usize {
            self.ringfd
        }
        /// Wait for the `io_uring` to be able to pop additional completion entries, while
        /// giving the kernel some time for processing submission entries if needed (if
        /// attached to the kernel).
        ///
        /// Allows giving a minimum of completion events before notification, through
        /// `min_complete`.
        pub fn wait(&self, min_complete: usize, flags: IoUringEnterFlags) -> Result<usize> {
            syscall::enter_iouring(self.ringfd, min_complete, flags)
        }
    }
    impl Drop for Instance {
        fn drop(&mut self) {
            let _ = self.deinit();
        }
    }
    impl PartialEq for Instance {
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
    use syscall::io_uring::v1::{CqEntry32, CqEntry64, Ring, SqEntry32, SqEntry64};
    use syscall::io_uring::{IoUringRecvFlags, IoUringRecvInfo};

    use syscall::error::{Error, Result};
    use syscall::error::{EINVAL, ENOSYS};

    use crate::ring::{SpscReceiver, SpscSender};

    #[derive(Debug)]
    pub enum GenericSender {
        Bits32(SpscSender<CqEntry32>),
        Bits64(SpscSender<CqEntry64>),
    }
    #[derive(Debug)]
    pub enum GenericReceiver {
        Bits32(SpscReceiver<SqEntry32>),
        Bits64(SpscReceiver<SqEntry64>),
    }

    #[derive(Debug)]
    pub struct Instance {
        sender: GenericSender,
        receiver: GenericReceiver,
    }

    impl Instance {
        pub fn new(recv_info: &IoUringRecvInfo) -> Result<Self> {
            if recv_info.version.major != 1 {
                return Err(Error::new(ENOSYS));
            } // TODO: Better error code
            let flags = IoUringRecvFlags::from_bits(recv_info.flags).ok_or(Error::new(EINVAL))?;

            fn init_sender<C>(info: &IoUringRecvInfo) -> SpscSender<C> {
                unsafe {
                    SpscSender::from_raw(
                        info.cr_virtaddr as *const Ring<C>,
                        info.ce_virtaddr as *mut C,
                    )
                }
            }
            fn init_receiver<S>(info: &IoUringRecvInfo) -> SpscReceiver<S> {
                unsafe {
                    SpscReceiver::from_raw(
                        info.sr_virtaddr as *const Ring<S>,
                        info.se_virtaddr as *mut S,
                    )
                }
            }

            Ok(Self {
                sender: if flags.contains(IoUringRecvFlags::BITS_32) {
                    GenericSender::Bits32(init_sender(recv_info))
                } else {
                    GenericSender::Bits64(init_sender(recv_info))
                },
                receiver: if flags.contains(IoUringRecvFlags::BITS_32) {
                    GenericReceiver::Bits32(init_receiver(recv_info))
                } else {
                    GenericReceiver::Bits64(init_receiver(recv_info))
                },
            })
        }
        pub fn sender(&mut self) -> &mut GenericSender {
            &mut self.sender
        }
        pub fn receiver(&mut self) -> &mut GenericReceiver {
            &mut self.receiver
        }
    }
}
pub use producer_instance::{
    GenericReceiver as ProducerGenericReceiver, GenericSender as ProducerGenericSender,
    Instance as ProducerInstance,
};
