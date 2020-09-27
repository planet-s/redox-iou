extern crate redox_iou;

use std::error::Error;
use std::sync::Arc;

use guard_trait::Guarded;

use redox_iou::executor::Executor;
use redox_iou::reactor::{Handle, OpenInfo, ReactorBuilder, SubmissionContext};

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
        let root_dir = handle
            .open_at(
                ring,
                SubmissionContext::default(),
                Guarded::wrap_static_slice(&b"assets/test.txt\0"[..]),
                OpenInfo::new(),
                libc::AT_FDCWD,
            )
            .await?;

        Ok(())
    })
}
