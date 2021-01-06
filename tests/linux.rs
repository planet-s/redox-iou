extern crate redox_iou;

use std::error::Error;
use std::sync::Arc;

use guard_trait::GuardedMutExt;

use redox_iou::executor::Executor;
use redox_iou::reactor::{OpenInfo, ReactorBuilder, SubmissionContext, SubmissionSync};

#[cfg(target_os = "linux")]
use redox_iou::linux::{ConsumerInstance, ReadFlags};

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
        let (fd_result, _) = handle
            .open(
                ring,
                SubmissionContext::default(),
                &b"assets/test.txt\0"[..],
                OpenInfo::new(),
            )
            .await;
        let fd = fd_result?;

        let buffer = vec![0u8; 4096];
        let guarded_buffer = GuardedMutExt::map_mut(buffer, |buffer| &mut buffer[..165]);

        let (bytes_read_result, guarded_buffer) = handle
            .pread(
                ring,
                SubmissionContext::new().with_sync(SubmissionSync::Drain),
                fd,
                guarded_buffer,
                0,
                ReadFlags::empty(),
            )
            .await;
        let bytes_read = bytes_read_result?;

        let _buffer = guarded_buffer.into_original();

        assert_eq!(bytes_read, 165);

        //println!("Content: {}", String::from_utf8_lossy(&buffer));

        unsafe {
            handle
                .close(
                    ring,
                    SubmissionContext::new().with_sync(SubmissionSync::Drain),
                    fd,
                    (),
                )
                .await?;
        }

        Ok(())
    })
}
