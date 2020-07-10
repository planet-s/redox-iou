pub use syscall::io_uring::*;

pub use futures::io::AsyncBufRead;

pub mod executor;
pub mod future;
pub mod instance;
pub mod reactor;

/*
pub struct Buffer<'a> {
    bytes: *mut u8,
    size: usize,
    finished: AtomicBool<bool>,
    _marker: PhantomData<&'a mut [u8]>,
}
pub type OwnedBuffer = Buffer<'static>;

impl<'a> Buffer<'a> {
    pub unsafe fn from_slice(slice: &'a mut [u8]) -> Self {
        Self {
            bytes: slice.as_mut_ptr(),
            size: slice.len(),
            finished: AtomicBool::new(true),
            _marker: PhantomData,
        }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if !self.finished {
            #[cfg(debug_assertions)]
            eprintln!("Leaking currently kernel-owned buffer at {:p}, size {}", self.bytes, self.size);

            // if the kernel owns the buffer and someone was stupid enough to drop it, the only
            // thing that can be done is to leak it
            return;
        }

        Box::from_raw(unsafe { slice::from_raw_parts(self.bytes, self.size) as *const [u8] })
    }
}

pub struct File {
    fd: usize,
    buffer: Buffer,
}

impl AsyncBufRead for File {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
    }
    fn consume(self: Pin<&mut Self>, amt: usize) {
    }
}*/
