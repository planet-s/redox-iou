#![feature(get_mut_unchecked, option_expect_none, vec_into_raw_parts)]
#![cfg_attr(test, feature(slice_fill))]
// TODO: This lint was probably introduced before const fns even existed, and is completely useless
// here since it's marked as "perf" while it can reduce performance in some scenarios (for debug
// builds, that is. release builds will likely optimize this away nevertheless).
//
// So, the problem is that this clippy lint ignores whether a function is const or not, and
// `fd_opt.ok_or(Error::new(EBADF))?` is no worse than `fd_opt.ok_or_else(|| Error::new(EBADF))?`.
//
// TODO: Now that you've read it, please an issue to https://github.com/rust-lang/rust, describing
// why clippy should ignore this lint for const fns. Alternatively implement this yourself ;)
#![allow(clippy::or_fun_call)]

pub use syscall::io_uring::*;

pub use futures::io::AsyncBufRead;

pub mod executor;
pub mod future;
pub mod instance;
pub mod reactor;
pub mod ring;

#[cfg(feature = "buffer_pool")]
pub mod memory;

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
