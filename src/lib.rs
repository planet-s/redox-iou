//! `redox-iou`
//!
//! This crate provides a library on top of the [raw io_uring interface](::syscall::io_uring), with
//! helpers for conveniently sending and receiving entries from the ring, as opposed to doing this
//! semi-manually, as well as a fully-featured executor and reactor that can handle the most common
//! syscalls completely asynchronously, like libraries such as `tokio` and `async-std` do.

#![feature(
    doc_cfg,
    maybe_uninit_ref,
    new_uninit,
    get_mut_unchecked,
    option_expect_none,
    vec_into_raw_parts
)]
// TODO: Once Redox starts using a newer compiler version, remove this.
#![cfg_attr(all(test, target_os = "redox"), feature(slice_fill))]
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
#![deny(missing_debug_implementations, missing_docs, missing_crate_level_docs)]

pub extern crate syscall as redox_syscall;
pub use redox_syscall::io_uring as interface;

/// An executor, capable of spawning tasks and organizing futures in a runqueue.
pub mod executor;
/// Future types used by the reactor, that indirectly represent pending entries from rings.
pub mod future;
/// A buffer pool that is mainly used for userspace-to-userspace rings, to share memory.
pub mod memory;
/// The reactor for io_uring, capable of polling multiple rings, both for producers and for
/// consumers.
pub mod reactor;

#[cfg(any(doc, target_os = "redox"))]
#[doc(cfg(target_os = "redox"))]
/// Redox-specific functionality.
pub mod redox;

#[cfg(any(doc, target_os = "linux"))]
#[doc(cfg(target_os = "linux"))]
/// Linux-specific functionality.
pub mod linux;

// TODO: Windows's, but also perhaps Solaris's and AIX's I/O Completion Ports (IOCP) interface.
// TODO: Linux AIO, maybe?
