#![feature(option_expect_none)]
#![cfg_attr(test, feature(slice_fill, vec_into_raw_parts))]

use std::borrow::{Borrow, BorrowMut};
use std::convert::TryInto;
use std::ptr::NonNull;
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{cmp, fmt, mem, ops, slice};

use syscall::error::{Error, Result};
use syscall::error::{EADDRINUSE, EEXIST, ENOMEM, EOVERFLOW};

use cranelift_bforest::{Comparator, Map, MapForest};
use either::*;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use thiserror::Error;

/// A comparator that only compares the offsets of two ranges.
struct RangeOffsetComparator;

/// A comparator that compares the offsets of two ranges, and then the occupiedness of them
/// (occupied is greater than not occupied).
struct RangeOffsetThenUsedComparator;

/// A comparator that first compares the occupiedness of the keys (occupied is greater than not
/// occupied), and then their offsets.
struct RangeUsedThenOffsetComparator;

#[derive(Clone, Copy, Ord, Eq, Hash, PartialOrd, PartialEq)]
struct OccOffsetHalf {
    offset: u32,
}
impl fmt::Debug for OccOffsetHalf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OccOffsetHalf")
            .field("offset", &self.offset())
            .field("is_used", &self.is_used())
            .finish()
    }
}

const RANGE_OFF_OFFSET_MASK: u32 = 0x7FFF_FFFF;
const RANGE_OFF_OFFSET_SHIFT: u8 = 0;
const RANGE_OFF_OCCUPD_SHIFT: u8 = 31;
const RANGE_OFF_OCCUPD_BIT: u32 = 1 << RANGE_OFF_OCCUPD_SHIFT;

impl OccOffsetHalf {
    const fn offset(&self) -> u32 {
        (self.offset & RANGE_OFF_OFFSET_MASK) >> RANGE_OFF_OFFSET_SHIFT
    }
    const fn is_used(&self) -> bool {
        self.offset & RANGE_OFF_OCCUPD_BIT != 0
    }
    const fn is_free(&self) -> bool {
        !self.is_used()
    }
    fn unused(offset: u32) -> Self {
        assert_eq!(offset & RANGE_OFF_OFFSET_MASK, offset);
        Self { offset }
    }
    fn used(offset: u32) -> Self {
        assert_eq!(offset & RANGE_OFF_OFFSET_MASK, offset);
        Self {
            offset: offset | RANGE_OFF_OCCUPD_BIT,
        }
    }
}
impl Comparator<OccOffsetHalf> for RangeOffsetComparator {
    fn cmp(&self, a: OccOffsetHalf, b: OccOffsetHalf) -> cmp::Ordering {
        Ord::cmp(&a.offset(), &b.offset())
    }
}
impl Comparator<OccOffsetHalf> for RangeOffsetThenUsedComparator {
    fn cmp(&self, a: OccOffsetHalf, b: OccOffsetHalf) -> cmp::Ordering {
        RangeOffsetComparator
            .cmp(a, b)
            .then(Ord::cmp(&a.is_used(), &b.is_used()))
    }
}
impl Comparator<OccOffsetHalf> for RangeUsedThenOffsetComparator {
    fn cmp(&self, a: OccOffsetHalf, b: OccOffsetHalf) -> cmp::Ordering {
        Ord::cmp(&a.is_used(), &b.is_used()).then(RangeOffsetComparator.cmp(a, b))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct OccInfoHalf {
    size: u32,
}
impl OccInfoHalf {
    const fn with_size(size: u32) -> Self {
        Self { size }
    }
}
struct OccMap {
    map: Map<OccOffsetHalf, OccInfoHalf>,
    forest: MapForest<OccOffsetHalf, OccInfoHalf>,
}
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct MmapOffsetHalf {
    offset: u32,
}
const MMAP_OFF_OFFSET_MASK: u32 = 0x7FFF_FFFF;
const MMAP_OFF_OFFSET_SHIFT: u8 = 0;
const MMAP_OFF_PENDING_SHIFT: u8 = 31;
const MMAP_OFF_PENDING_BIT: u32 = 1 << MMAP_OFF_PENDING_SHIFT;

impl MmapOffsetHalf {
    fn ready_or_pending(offset: u32) -> Self {
        assert_eq!(offset & MMAP_OFF_OFFSET_MASK, offset);

        Self { offset }
    }
    fn ready(offset: u32) -> Self {
        Self::ready_or_pending(offset)
    }
    fn pending(offset: u32) -> Self {
        assert_eq!(offset & MMAP_OFF_OFFSET_MASK, offset);
        Self {
            offset: offset | MMAP_OFF_PENDING_BIT,
        }
    }
    const fn offset(&self) -> u32 {
        (self.offset & MMAP_OFF_OFFSET_MASK) >> MMAP_OFF_OFFSET_SHIFT
    }
    const fn is_pending(&self) -> bool {
        self.offset & MMAP_OFF_PENDING_BIT != 0
    }
    const fn is_ready(&self) -> bool {
        !self.is_pending()
    }
}
impl MmapInfoHalf {
    const fn null() -> Self {
        Self {
            // reasonable since size == 0 implies _no pointer access at all_
            addr: NonNull::dangling(),
            size: 0,
        }
    }
}
/// Compares whether the mmap is pending (pending is greater than non-pending), and then the actual
/// offset.
struct MmapComparatorOffset;

impl Comparator<MmapOffsetHalf> for MmapComparatorOffset {
    fn cmp(&self, a: MmapOffsetHalf, b: MmapOffsetHalf) -> cmp::Ordering {
        Ord::cmp(&a.offset(), &b.offset())
    }
}

#[derive(Clone, Copy, Debug)]
struct MmapInfoHalf {
    size: u32,
    addr: NonNull<u8>,
}

struct MmapMap {
    map: Map<MmapOffsetHalf, MmapInfoHalf>,
    forest: MapForest<MmapOffsetHalf, MmapInfoHalf>,
}
pub struct BufferPool<H: Handle> {
    handle: Option<H>,

    // TODO: Concurrent B-tree
    // TODO: Don't use forests!
    guarded_occ_count: AtomicUsize,

    // The map all occupations
    occ_map: RwLock<OccMap>,
    mmap_map: RwLock<MmapMap>,
}
unsafe impl<H: Handle + Send> Send for BufferPool<H> {}
unsafe impl<H: Handle + Sync> Sync for BufferPool<H> {}

impl<H: Handle> fmt::Debug for BufferPool<H>
where
    H: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferPool")
            .field("handle", &self.handle)
            // TODO: maps
            .finish()
    }
}

pub enum NoGuard {}
impl Guard for NoGuard {
    fn try_release(&self) -> bool {
        unreachable!("NoGuard cannot be initialized")
    }
}

// TODO: Support mutable/immutable slices, maybe even with refcounts? A refcount of 1 would mean
// exclusive, while a higher refcount would mean shared.
#[derive(Debug)]
pub struct BufferSlice<'a, H: Handle, G: Guard = NoGuard> {
    start: u32,
    size: u32,
    pointer: *mut u8,

    pool: Either<&'a BufferPool<H>, Weak<BufferPool<H>>>,
    guard: Option<G>,
}

unsafe impl<'a, H, G: Guard> Send for BufferSlice<'a, H, G>
where
    H: Send + Sync + Handle,
    G: Send,
{}
unsafe impl<'a, H, G: Guard> Sync for BufferSlice<'a, H, G>
where
    H: Send + Sync + Handle,
    G: Sync,
{}

#[derive(Debug, Error)]
#[error("failed to add guard, due to another guard already existing")]
pub struct AddGuardError;

impl From<AddGuardError> for Error {
    fn from(_: AddGuardError) -> Error {
        Error::new(EEXIST)
    }
}

pub struct WithGuardError<T> {
    pub this: T,
}
impl<T> fmt::Debug for WithGuardError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WithGuardError")
            // TODO
            .finish()
    }
}

impl<T> fmt::Display for WithGuardError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to replace guard, due to another guard already existing")
    }
}
impl<T> std::error::Error for WithGuardError<T> {}
impl<T> From<WithGuardError<T>> for AddGuardError {
    fn from(_: WithGuardError<T>) -> Self {
        Self
    }
}
impl<T> From<WithGuardError<T>> for Error {
    fn from(_: WithGuardError<T>) -> Error {
        Error::new(EEXIST)
    }
}
pub struct ReclaimError<T> {
    pub this: T,
}
impl<T> fmt::Display for ReclaimError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to reclaim buffer slice, since it was in use")
    }
}
impl<T> fmt::Debug for ReclaimError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReclaimError")
            .finish()
    }
}
impl<T> std::error::Error for ReclaimError<T> {}
impl<T> From<ReclaimError<T>> for Error {
    fn from(_: ReclaimError<T>) -> Error {
        Error::new(EADDRINUSE)
    }
}

pub struct CloseError<T> {
    pub this: T,
}
impl<T> fmt::Display for CloseError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to close since buffers were in use")
    }
}
impl<T> fmt::Debug for CloseError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloseError")
            // TODO
            .finish()
    }
}

impl<'a, H, G> BufferSlice<'a, H, G>
where
    G: Guard,
    H: Handle,
{
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.pointer as *const u8, self.size.try_into().unwrap()) }
    }
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.pointer, self.size.try_into().unwrap()) }
    }
    /// Forcefully remove a guard from a future, from this slice, returning it if there was a guard
    /// already.
    ///
    /// # Safety
    /// This is unsafe because it allows removing guards set by pending futures; although this is
    /// completely fine when there are no pending ones, the buffer slice will be reclaimed without
    /// the guard, causing UB if any producer keeps using its pointer.
    pub unsafe fn unguard(&mut self) -> Option<G> {
        match self.guard.take() {
            Some(g) => {
                let arc;

                let pool = match self.pool {
                    Left(reference) => reference,
                    Right(ref weak) => {
                        arc = weak.upgrade().expect("calling unguard on a weakly-owned buffer slice where the pool died");
                        &*arc
                    }
                };
                let prev = pool.guarded_occ_count.fetch_sub(1, Ordering::Release);
                assert_ne!(prev, 0, "someone forgot to increment the guarded_occ_count, now I'm getting a subtraction overflow!");
                Some(g)
            }
            None => None,
        }
    }
    /// Adds a guard to this buffer, preventing it from deallocating unless the guard accepts that.
    /// This is crucial when memory is shared with another component that may be outside this
    /// process's address space. If there is a pending io_uring submission or a pending NVME
    /// command for instance, this guard will fail if the buffer is in use by a command, and leak
    /// the memory instead when dropping.
    ///
    /// This will error with `EEXIST` if there is already an active guard.
    pub fn guard(&mut self, guard: G) -> Result<(), AddGuardError> {
        if self.guard.is_some() {
            return Err(AddGuardError);
        }
        self.guard = Some(guard);

        let arc;

        let pool = match self.pool {
            Left(pool) => pool,
            Right(ref pool_weak) => {
                arc = pool_weak
                    .upgrade()
                    .expect("Guarding buffer slice which pool has been dropped");
                &*arc
            }
        };
        // TODO: Is Relaxed ok here?
        pool.guarded_occ_count.fetch_add(1, Ordering::Release);

        Ok(())
    }
    /// Tries to add a guard of potentially a different type than the guard type in this slice.
    /// Because of that this, this will consume self and construct a different `BufferSlice` with
    /// a different guard type, or error with `self` if there was already a guard present.
    pub fn with_guard<OtherGuard: Guard>(self, other: OtherGuard) -> Result<BufferSlice<'a, H, OtherGuard>, WithGuardError<Self>> {
        if self.guard.is_some() {
            return Err(WithGuardError {
                this: self
            });
        }
        let start = self.start;
        let pointer = self.pointer;
        let pool = match self.pool {
            Left(r) => Left(r),
            Right(ref w) => Right(Weak::clone(w)),
        };
        let size = self.size;

        mem::forget(self);

        let mut slice = BufferSlice {
            start,
            pointer,
            pool,
            size,
            guard: None,
        };
        slice.guard(other).unwrap();
        Ok(slice)
    }
    fn reclaim_inner(&mut self) -> bool {
        let arc;

        let pool = match self.pool {
            Left(reference) => reference,
            Right(ref weak) => {
                arc = match weak.upgrade() {
                    Some(a) => a,
                    None => return true,
                };
                &*arc
            }
        };
        let (was_guarded, can_be_reclaimed) = match self.guard {
            Some(ref guard) => (true, guard.try_release()),
            None => (false, true),
        };
        if can_be_reclaimed {
            unsafe {
                if was_guarded {
                    self.unguard().unwrap();
                }
                pool.reclaim_slice_inner(&*self);
            }
            true
        } else {
            false
        }
    }
    /// Reclaim the buffer slice, equivalent to dropping but with a Result. If the buffer slice was
    /// guarded by a future, this will fail with [`EADDRINUSE`] if the future hadn't completed when
    /// this was called.
    pub fn reclaim(mut self) -> Result<(), ReclaimError<Self>> {
        match self.reclaim_inner() {
            true => {
                mem::forget(self);
                Ok(())
            }
            false => Err(ReclaimError {
                this: self,
            }),
        }
    }

    /// Get the internal offset from the mmap file.
    pub fn offset(&self) -> u32 {
        self.start
    }
    /// Get the length of the slice.
    pub fn len(&self) -> u32 {
        self.size
    }
    /// Check whether the slice is empty or not.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
impl<'a, H, G> Drop for BufferSlice<'a, H, G>
where
    G: Guard,
    H: Handle,
{
    fn drop(&mut self) {
        match self.reclaim_inner() {
            true => (),
            false => {
                log::debug!(
                    "Trying to drop a BufferSlice that is in use, leaking memory",
                );
            }
        }
    }
}
impl<'a, H: Handle, G: Guard> ops::Deref for BufferSlice<'a, H, G> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}
impl<'a, H: Handle, G: Guard> ops::DerefMut for BufferSlice<'a, H, G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_slice_mut()
    }
}
impl<'a, H: Handle, G: Guard> Borrow<[u8]> for BufferSlice<'a, H, G> {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}
impl<'a, H: Handle, G: Guard> BorrowMut<[u8]> for BufferSlice<'a, H, G> {
    fn borrow_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}
impl<'a, H: Handle, G: Guard> AsRef<[u8]> for BufferSlice<'a, H, G> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl<'a, H: Handle, G: Guard> AsMut<[u8]> for BufferSlice<'a, H, G> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}
pub struct ExpandHandle<'a, H: Handle> {
    offset: u32,
    len: u32,
    pool: &'a BufferPool<H>,
}
impl<'a, H: Handle> ExpandHandle<'a, H> {
    pub fn len(&self) -> u32 {
        self.len
    }
    pub fn offset(&self) -> u32 {
        self.offset
    }
    /// Provide the handle with an actually allocated pointer, initializing the memory range within
    /// the buffer pool.
    ///
    /// # Safety
    ///
    /// For this to be safe, the pointer must be a valid allocation (anywhere) of the size
    /// originally inputted. TODO: alignment
    pub unsafe fn initialize(self, pointer: *mut u8) -> Result<()> {
        let mut write_guard = self.pool.mmap_map.write();
        let mmap_map = &mut *write_guard;

        let new_offset_half = MmapOffsetHalf::ready(self.offset());
        let old_offset_half = MmapOffsetHalf::pending(self.offset());

        let new_info_half = MmapInfoHalf {
            addr: NonNull::new(pointer as *const u8 as *mut u8).ok_or(Error::new(ENOMEM))?,
            size: self.len(),
        };

        // Remove the previous entry marked "pending", and insert a new entry marked "ready".
        mmap_map
            .map
            .remove(old_offset_half, &mut mmap_map.forest, &())
            .expect("pending mmap range was not ");

        mmap_map.map
            .insert(new_offset_half, new_info_half, &mut mmap_map.forest, &())
            .expect_none(
                "somehow the mmap range that was supposed to go from \"pending\" to \"ready\", was already inserted as \"ready\""
            );

        // Before releasing the guard and allowing new slices from be acquired, we'll do a last
        // lock of the occ map, to mark the range as free.
        let mut occ_write_guard = self.pool.occ_map.write();
        let occ_map = &mut *occ_write_guard;

        debug_assert!(occ_map
            .map
            .get_or_less(
                OccOffsetHalf::unused(self.offset()),
                &occ_map.forest,
                &RangeOffsetComparator
            )
            .map_or(false, |(k, v)| k.offset() < self.offset()
                && k.offset() + v.size < self.offset() + self.len()));
        occ_map
            .map
            .insert(
                OccOffsetHalf::unused(self.offset()),
                OccInfoHalf::with_size(self.len()),
                &mut occ_map.forest,
                &RangeOffsetComparator,
            )
            .expect_none("expected newly-acquired slice not to conflict with any existing");

        Ok(())
    }
}
impl<H: Handle> BufferPool<H> {
    pub fn begin_expand(&self, additional: u32) -> Result<ExpandHandle<'_, H>> {
        let new_offset = {
            // Get an intent guard (in other words, upgradable read guard), which allows regular
            // readers to continue acquiring new slices etc, but only allows this thread to be able
            // to upgrade into an exclusive lock (which we do later during the actual insert).
            let mmap_intent_guard = self.mmap_map.upgradable_read();

            // Get the last mmapped range, no matter whether it's pending or ready to use.
            let new_offset = mmap_intent_guard
                .map
                .get_or_less(
                    MmapOffsetHalf::ready(0),
                    &mmap_intent_guard.forest,
                    // Only compare the offsets, we don't care about it being ready or pending when
                    // we only want to find the last offset and calculate the next offset from
                    // that.
                    &MmapComparatorOffset,
                )
                .map_or(
                    // If there somehow weren't any remaining mmap regions, we just implicitly set the
                    // next offset to zero.
                    Ok(0),
                    |(last_key, last_value)| {
                        last_key
                            .offset()
                            .checked_add(last_value.size)
                            .ok_or(Error::new(EOVERFLOW))
                    },
                )?;

            if new_offset & MMAP_OFF_OFFSET_MASK != new_offset {
                // TODO: Reclaim old ranges if possible, rather than failing. Perhaps one could use
                // a circular ring buffer to allow for O(1) range acquisition, with O(log n)
                // freeing, if we combine a ring buffer for contiguous ranges, with a B-tree.
                return Err(Error::new(EOVERFLOW));
            }

            let mut mmap_write_guard = RwLockUpgradableReadGuard::upgrade(mmap_intent_guard);
            let ref_mut = &mut *mmap_write_guard;

            // Insert a new region marked as "pending", with an uninitialized pointer.
            let new_offset_half = MmapOffsetHalf::pending(new_offset);
            let new_info_half = MmapInfoHalf::null();
            ref_mut
                .map
                .insert(new_offset_half, new_info_half, &mut ref_mut.forest, &());

            // Implicitly drop the intent guard, allowing other threads to also expand this buffer
            // pool. There is no race condition here whatsoever, since we have marked our
            // prereserved range as "pending".
            new_offset
        };
        Ok(ExpandHandle {
            offset: new_offset,
            len: additional,
            pool: self,
        })
    }

    // TODO: Shrink support

    pub fn handle(&self) -> Option<&H> {
        self.handle.as_ref()
    }

    /// Close an thus free the entire pool. If there are any pending commands that have guarded
    /// buffer slices from this pool, the entire memory will be leaked (TODO: Free as much memory
    /// as possible when this happens, rather than the entire pool).
    pub fn close(mut self) -> Result<Option<H>, CloseError<Self>> {
        if self.guarded_occ_count.load(Ordering::Acquire) > 0 {
            return Err(CloseError {
                this: self,
            });
        }

        let handle = self.handle.take();
        Ok(handle)
    }
    pub fn new(handle: Option<H>) -> Self {
        Self {
            occ_map: RwLock::new(OccMap {
                forest: MapForest::new(),
                map: Map::new(),
            }),
            mmap_map: RwLock::new(MmapMap {
                forest: MapForest::new(),
                map: Map::new(),
            }),
            guarded_occ_count: AtomicUsize::new(0),
            handle,
        }
    }
    /// Convenience wrapper over `Arc::new(self)`.
    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }
    fn acquire_slice(&self, len: u32, alignment: u32) -> Option<(ops::Range<u32>, *mut u8)> {
        // Begin by obtaining an intent guard. This will unfortunately prevent other threads from
        // simultaneously searching the map for partitioning it; however, there can still be other
        // threads checking whether it's safe to munmap certain offsets.
        let intent_guard = self.occ_map.upgradable_read();

        fn align(off: u32, alignment: u32) -> Option<u32> {
            assert_ne!(alignment, 0);
            assert!(alignment.is_power_of_two());

            if alignment == 1 {
                return Some(off);
            }

            off.checked_add(alignment - 1)?
                .checked_div(alignment)?
                .checked_mul(alignment)
        }

        let (k, _) = intent_guard.map.iter(&intent_guard.forest).find(|(k, v)| {
            k.is_free()
                && v.size >= len
                && align(k.offset(), alignment)
                    .map_or(false, |aligned| v.size - (aligned - k.offset()) >= len)
        })?;

        let aligned_off =
            align(k.offset(), alignment).expect("bypassed alignment check in iterator");
        let align_advancement = aligned_off - k.offset();

        let new_offset = {
            let mut write_guard = RwLockUpgradableReadGuard::upgrade(intent_guard);
            let occ_map = &mut *write_guard;

            let mut v = occ_map
                .map
                .remove(k, &mut occ_map.forest, &RangeOffsetThenUsedComparator)
                .expect("expected entry not to be removed by itself when acquiring slice");

            if v.size >= len {
                // Reinsert the free entry, but with a reduced length.
                assert!(k.is_free());
                v.size -= len;

                let k_for_reinsert = OccOffsetHalf::unused(k.offset() + len);
                occ_map
                    .map
                    .insert(
                        k_for_reinsert,
                        v,
                        &mut occ_map.forest,
                        &RangeOffsetComparator,
                    )
                    .expect_none("expected previous entry not to have been reinserted by itself");
            }
            if align_advancement > 0 {
                // If there was unused space due to alignment, insert that small region marked
                // unused as well.
                let k = OccOffsetHalf::unused(k.offset());
                let v = OccInfoHalf::with_size(align_advancement);

                occ_map
                    .map
                    .insert(k, v, &mut occ_map.forest, &RangeOffsetComparator)
                    .expect_none("somehow the small alignment region was already mapped");
            }

            let new_offset = aligned_off;
            let new_k = OccOffsetHalf::used(new_offset);
            let new_v = OccInfoHalf::with_size(len);
            occ_map
                .map
                .insert(new_k, new_v, &mut occ_map.forest, &RangeOffsetComparator)
                .expect_none("expected new entry not to already be inserted");

            new_offset
        };
        let pointer = {
            let read_guard = self.mmap_map.read();
            let (mmap_k, mmap_v) = read_guard
                .map
                .get_or_less(
                    MmapOffsetHalf::ready(new_offset),
                    &read_guard.forest,
                    &MmapComparatorOffset,
                )
                .expect(
                    "expected all free entries in the occ map to have a corresponding mmap entry",
                );

            assert!(mmap_k.is_ready());
            assert!(mmap_k.offset() <= new_offset);
            assert!(mmap_k.offset() + mmap_v.size >= new_offset + len);
            let base_pointer = mmap_v.addr;
            unsafe {
                base_pointer
                    .as_ptr()
                    .add((new_offset - mmap_k.offset()).try_into().unwrap())
                    as *mut u8
            }
        };

        let offset = aligned_off;

        Some((offset..offset + len, pointer))
    }
    pub fn acquire_borrowed_slice<G: Guard>(&self, len: u32, alignment: u32) -> Option<BufferSlice<'_, H, G>> {
        let (range, pointer) = self.acquire_slice(len, alignment)?;

        Some(BufferSlice {
            start: range.start,
            size: range.end - range.start,
            pointer,
            pool: Left(self),
            guard: None,
        })
    }
    pub fn acquire_weak_slice<G: Guard>(
        self: &Arc<Self>,
        len: u32,
        alignment: u32,
    ) -> Option<BufferSlice<'static, H, G>> {
        let (range, pointer) = self.acquire_slice(len, alignment)?;

        Some(BufferSlice {
            start: range.start,
            size: range.end - range.start,
            pointer,
            pool: Right(Arc::downgrade(self)),
            guard: None,
        })
    }
    fn remove_free_offset_below(occ_map: &mut OccMap, start: &mut u32, size: &mut u32) -> bool {
        let previous_start = *start;
        let lower_offset = match previous_start.checked_sub(1) {
            Some(l) => l,
            None => return false,
        };

        let lower_partial_key = OccOffsetHalf::unused(lower_offset);
        if let Some((lower_actual_key, lower_value)) =
            occ_map
                .map
                .get_or_less(lower_partial_key, &occ_map.forest, &())
        {
            if lower_actual_key.is_used() {
                return false;
            }

            if lower_actual_key.offset() + lower_value.size != previous_start {
                // There is another occupied range between these.
                return false;
            }
            let v = occ_map
                .map
                .remove(
                    lower_actual_key,
                    &mut occ_map.forest,
                    &RangeOffsetThenUsedComparator,
                )
                .expect("expected previously found key to exist in the b-tree map");

            assert_eq!(v, lower_value);
            *start = lower_actual_key.offset;
            *size += lower_value.size;

            true
        } else {
            false
        }
    }
    fn remove_free_offset_above(occ_map: &mut OccMap, start: &mut u32, size: &mut u32) -> bool {
        let end = *start + *size;

        let higher_key = OccOffsetHalf::unused(end);

        if let Some(higher_value) = occ_map.map.get(higher_key, &occ_map.forest, &()) {
            if higher_key.offset == *start {
                // The entry was not above the initially freed one.
                return false;
            }

            // We don't have to check that there is no range between, since there cannot exist
            // multiple overlapping ranges (yet).
            if higher_key.is_used() {
                return false;
            }

            let v = occ_map
                .map
                .remove(higher_key, &mut occ_map.forest, &())
                .expect("expected previously found key to exist in the b-tree map");

            assert_eq!(v, higher_value);
            *size += higher_value.size;

            true
        } else {
            false
        }
    }

    unsafe fn reclaim_slice_inner<G: Guard>(&self, slice: &BufferSlice<'_, H, G>) {
        let mut occ_write_guard = self.occ_map.write();
        let occ_map = &mut *occ_write_guard;

        let mut start = slice.start;
        let mut size = slice.size;

        let v = occ_map
            .map
            .remove(
                OccOffsetHalf::used(slice.start),
                &mut occ_map.forest,
                &RangeOffsetComparator,
            )
            .expect("expected occ map to contain buffer slice when reclaiming it");
        assert_eq!(v.size, slice.size);

        while Self::remove_free_offset_below(occ_map, &mut start, &mut size) {}
        while Self::remove_free_offset_above(occ_map, &mut start, &mut size) {}

        occ_map
            .map
            .insert(
                OccOffsetHalf::unused(start),
                OccInfoHalf::with_size(size),
                &mut occ_map.forest,
                &RangeOffsetComparator,
            )
            .expect_none(
                "expected newly resized free range not to start existing again before insertion",
            );
    }
    fn drop_impl(&mut self) {
        let count = self.guarded_occ_count.load(Ordering::Acquire);

        if count == 0 {
            if let Some(h) = self.handle.as_mut() {
                let _ = h.close();
            }
        } else {
            log::warn!("Leaking entire buffer pool, since there were {} slices that were guarded by futures that haven't been completed", count);
        }
    }
}

impl<H: Handle> Drop for BufferPool<H> {
    fn drop(&mut self) {
        self.drop_impl();
    }
}

pub trait Handle {
    fn close(&mut self) -> Result<()>;
}
pub trait Guard {
    /// Try to release the guard, returning either true for success or false for failure.
    fn try_release(&self) -> bool;
}
#[cfg(test)]
mod tests {
    use super::*;

    use std::convert::TryFrom;
    use std::{mem, thread};

    struct TestHandle;
    impl Handle for TestHandle {
        fn close(&mut self) -> Result<()> {
            // Who cares about memory leaks in tests anyway?
            Ok(())
        }
    }

    fn setup_pool(maps: impl IntoIterator<Item = Vec<u8>>) -> (BufferPool<TestHandle>, u32) {
        let mut mmap_map = MmapMap {
            map: Map::new(),
            forest: MapForest::new(),
        };
        let mut occ_map = OccMap {
            map: Map::new(),
            forest: MapForest::new(),
        };
        let mut total_size = 0;

        for mut memory in maps {
            memory.shrink_to_fit();

            let (addr, len, _) = memory.into_raw_parts();
            let size = u32::try_from(len).unwrap();

            mmap_map.map.insert(
                MmapOffsetHalf::ready(0),
                MmapInfoHalf {
                    size,
                    addr: NonNull::new(addr).unwrap().into(),
                },
                &mut mmap_map.forest,
                &(),
            );
            occ_map.map.insert(
                OccOffsetHalf::unused(0),
                OccInfoHalf::with_size(size),
                &mut occ_map.forest,
                &(),
            );
            total_size += size;
        }
        let pool = BufferPool {
            handle: None,
            mmap_map: RwLock::new(mmap_map),
            occ_map: RwLock::new(occ_map),
            guarded_occ_count: AtomicUsize::new(0),
        };
        (pool, total_size)
    }
    fn setup_default_pool() -> (BufferPool<TestHandle>, u32) {
        setup_pool(vec![vec![0u8; 32768], vec![0u8; 4096], vec![0u8; 65536]])
    }

    #[test]
    fn occ_map_acquisition_single_mmap() {
        let (pool, _) = setup_default_pool();

        let mut slices = Vec::new();

        loop {
            let mut slice = match pool.acquire_borrowed_slice::<NoGuard>(4096, 1) {
                Some(s) => s,
                None => break,
            };

            let text = b"Hello, world!";
            slice[..text.len()].copy_from_slice(text);
            assert_eq!(&slice[..text.len()], text);
            slices.push(slice);
        }
        drop(slices);

        mem::forget(pool);
    }
    #[test]
    fn occ_multithreaded() {
        // This test is not about aliasing, but rather to get all the assertions and expects, to
        // work when there are multiple threads constantly trying to acquire and release slices.

        let (pool, _) = setup_default_pool();
        let pool = pool.shared();

        const THREAD_COUNT: usize = 8;

        #[cfg(not(miri))]
        const N: usize = 1000;

        #[cfg(miri)]
        const N: usize = 128;

        let threads = (0..THREAD_COUNT).map(|_| {
            let pool = Arc::clone(&pool);
            thread::spawn(move || {
                use rand::Rng;

                let mut thread_rng = rand::thread_rng();

                for _ in 0..N {
                    'retry: loop {
                        let len = thread_rng.gen_range(64, 4096);
                        let align = 1 << thread_rng.gen_range(0, 3);
                        match pool.acquire_borrowed_slice::<NoGuard>(len, align) {
                            Some(_) => break 'retry,
                            None => continue 'retry,
                        }
                    }
                }
            })
        });
        for thread in threads {
            thread.join().unwrap();
        }
    }
    #[test]
    fn no_aliasing() {
        let (pool, _) = setup_default_pool();
        const SIZE: u32 = 512;

        let mut slices = Vec::new();

        loop {
            let slice = match pool.acquire_borrowed_slice::<NoGuard>(SIZE, 1) {
                Some(s) => s,
                None => break,
            };
            slices.push(slice);
        }
        for slice in &mut slices {
            assert!(slice.iter().all(|&byte| byte == 0));
            slice.fill(63);
        }
    }
    #[test]
    fn alignment() {
        let (pool, _) = setup_pool(vec![vec![0u8; 4096]]);

        fn get_and_check_slice(
            pool: &BufferPool<TestHandle>,
            size: u32,
            align: u32,
            fill_byte: u8,
        ) -> BufferSlice<TestHandle> {
            let mut slice = pool.acquire_borrowed_slice(size, align).unwrap();
            assert!(slice.iter().all(|&byte| byte == 0));
            slice.fill(fill_byte);
            assert!(slice.iter().all(|&byte| byte == fill_byte));
            assert_eq!(slice.len(), size);
            assert_eq!(slice.offset() % align, 0);
            slice
        }

        {
            let _small_begin_slice = get_and_check_slice(&pool, 64, 1, 0x01);
            let _aligned_slice = get_and_check_slice(&pool, 128, 128, 0x02);
            let _half_page = get_and_check_slice(&pool, 2048, 2048, 0xFE);
        }
    }
}
