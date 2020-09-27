extern crate redox_iou;

use std::error::Error;
use std::sync::Arc;

use guard_trait::Guarded;

use redox_iou::executor::Executor;
use redox_iou::reactor::{Handle, OpenInfo, ReactorBuilder, SubmissionContext, SubmissionSync};

#[cfg(target_os = "linux")]
use redox_iou::linux::ConsumerInstance;

#[cfg(target_os = "linux")]
#[test]
fn basic_file_io() -> Result<(), Box<dyn Error + 'static>> {
    env_logger::init();

    let io_uring = iou::IoUring::new(16)?;
    let instance = ConsumerInstance::from_iou(io_uring);

    let reactor = ReactorBuilder::new()
        .with_primary_instance(instance)
        .build();
    let executor = Executor::with_reactor(Arc::clone(&reactor));

    let handle = reactor.handle();

    let ring = reactor.primary_instances().next().unwrap();

    executor.run(async move {
        // TODO: IntoGuardable trait?
        let (fd, _) = handle
            .open_at(
                ring,
                SubmissionContext::default(),
                Guarded::wrap_static_slice(&b"assets/test.txt\0"[..]),
                OpenInfo::new(),
                libc::AT_FDCWD,
            )
            .await?;

        let mut buffer = vec! [0u8; 4096];
        let guarded_buffer = Guarded::new(buffer);

        let (bytes_read, guarded_buffer) = handle.pread(
            ring,
            SubmissionContext::new()
                .with_sync(SubmissionSync::Drain),
            fd,
            guarded_buffer,
            0,
        ).await?;

        unsafe { handle.close(
            ring,
            SubmissionContext::new()
                .with_sync(SubmissionSync::Drain),
            fd,
            false,
        ).await?; }

        Ok(())
    })
}
