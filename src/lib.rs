#![feature(get_mut_unchecked, option_expect_none, vec_into_raw_parts)]
#![cfg_attr(feature = "buffer_pool", feature(maybe_uninit_ref))]
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
