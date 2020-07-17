use std::borrow::{Borrow, BorrowMut};
use std::convert::{TryFrom, TryInto};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::{cmp, fmt, mem, ops, slice};

use syscall::data::Map as Mmap;
use syscall::error::{Error, Result};
use syscall::error::{EADDRINUSE, EOVERFLOW};
use syscall::flag::MapFlags;
use syscall::io_uring::operation::Dup2Flags;

use cranelift_bforest::{Comparator, Map, MapForest};
use either::*;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};

use crate::future::{CommandFuture, CommandFutureRepr, State as CommandFutureState};
use crate::reactor::Handle;

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
            addr: None,
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
    addr: Option<NonNull<u8>>,
}

struct MmapMap {
    map: Map<MmapOffsetHalf, MmapInfoHalf>,
    forest: MapForest<MmapOffsetHalf, MmapInfoHalf>,
}
pub struct BufferPool {
    fd: usize,
    handle: Option<Handle>,

    // TODO: Concurrent B-tree
    // TODO: Don't use forests!
    guarded_occ_count: AtomicUsize,

    // The map all occupations
    occ_map: RwLock<OccMap>,
    mmap_map: RwLock<MmapMap>,
}
unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

impl fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferPool")
            .field("fd", &self.fd)
            .field("handle", &self.handle)
            // TODO: maps
            .finish()
    }
}

// TODO: Support mutable/immutable slices, maybe even with refcounts? A refcount of 1 would mean
// exclusive, while a higher refcount would mean shared.
#[derive(Debug)]
pub struct BufferSlice<'a> {
    start: u32,
    size: u32,
    pointer: *mut u8,

    pool: Either<&'a BufferPool, Weak<BufferPool>>,
    guard: Option<Weak<Mutex<CommandFutureState>>>,
}

unsafe impl<'a> Send for BufferSlice<'a> {}
unsafe impl<'a> Sync for BufferSlice<'a> {}

impl<'a> BufferSlice<'a> {
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.pointer as *const u8, self.size.try_into().unwrap()) }
    }
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.pointer, self.size.try_into().unwrap()) }
    }
    /// Forcefully remove a guard from a future, from this slice.
    ///
    /// # Safety
    /// This is unsafe because it allows removing guards set by pending futures; although this is
    /// completely fine when there are no pending ones, the buffer slice will be reclaimed without
    /// the guard, causing UB if any producer keeps using its pointer.
    pub unsafe fn unguard(&mut self) {
        self.guard = None;
    }
    fn reclaim_inner(&mut self) -> Result<()> {
        let arc;

        let pool = match self.pool {
            Left(reference) => reference,
            Right(ref weak) => {
                arc = match weak.upgrade() {
                    Some(a) => a,
                    None => return Ok(()),
                };
                &*arc
            }
        };
        let (was_guarded, can_be_reclaimed) = match self.guard {
            Some(ref weak) => {
                if let Some(arc) = weak.upgrade() {
                    // Only allow reclaiming buffer slices when their guarded future has actually
                    // completed.
                    let is_complete = matches!(&*arc.lock(), CommandFutureState::Cancelled | CommandFutureState::Completed(_));
                    if is_complete {
                        unsafe {
                            self.unguard();
                        }
                    }
                    (true, is_complete)
                } else {
                    // Future is not used anymore, we can safely remove the guard now.
                    unsafe {
                        self.unguard();
                    }
                    (false, true)
                }
            }
            None => (false, true),
        };
        if can_be_reclaimed {
            unsafe { pool.reclaim_slice_inner(&*self) };
            if was_guarded {
                let prev = pool.guarded_occ_count.fetch_sub(1, Ordering::Release);
                assert_ne!(prev, 0, "someone forgot to increment the guarded_occ_count, now I'm getting a subtraction overflow!");
            }
            Ok(())
        } else {
            Err(Error::new(EADDRINUSE))
        }
    }
    /// Reclaim the buffer slice, equivalent to dropping but with a Result. If the buffer slice was
    /// guarded by a future, this will fail with [`EADDRINUSE`] if the future hadn't completed when
    /// this was called.
    pub fn reclaim(mut self) -> Result<()> {
        self.reclaim_inner()?;
        mem::forget(self);
        Ok(())
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
impl<'a> Drop for BufferSlice<'a> {
    fn drop(&mut self) {
        match self.reclaim_inner() {
            Ok(()) => (),
            Err(err) => {
                log::debug!(
                    "Trying to drop a BufferSlice that is in use, leaking memory: {}",
                    err
                );
            }
        }
    }
}
impl<'a> ops::Deref for BufferSlice<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}
impl<'a> ops::DerefMut for BufferSlice<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_slice_mut()
    }
}
impl<'a> Borrow<[u8]> for BufferSlice<'a> {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}
impl<'a> BorrowMut<[u8]> for BufferSlice<'a> {
    fn borrow_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}
impl<'a> AsRef<[u8]> for BufferSlice<'a> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl<'a> AsMut<[u8]> for BufferSlice<'a> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}
impl CommandFuture {
    /// Protect a slice with a future guard, preventing the memory from being reclaimed until the
    /// future has completed. This will cause the buffer slice to leak memory if dropped too early,
    /// but prevents undefined behavior.
    pub fn guard<'a>(&self, slice: &mut BufferSlice<'a>) {
        let weak = match self.inner.repr {
            CommandFutureRepr::Direct { ref state, .. } => Arc::downgrade(state),
            CommandFutureRepr::Tagged { tag, .. } => {
                if let Some(reactor) = self.inner.reactor.upgrade() {
                    if let Some(state) = reactor.tag_map.read().get(&tag) {
                        Arc::downgrade(state)
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
        };
        slice.guard = Some(weak);

        let arc;

        let pool = match slice.pool {
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
    }
}

impl Handle {
    pub async fn create_buffer_pool(
        &self,
        _creation_command_priority: u16,
        initial_len: u32,
    ) -> Result<BufferPool> {
        let reactor = self
            .reactor
            .upgrade()
            .expect("can't create_buffer_pool: reactor is dead");

        let ringfd = reactor.main_instance.consumer_instance.read().ringfd();
        let fd = unsafe { self.dup2(reactor.primary_instance(), ringfd, Dup2Flags::PARAM, Some(b"pool")) }.await?;

        Ok(unsafe {
            BufferPool::new_from_raw(fd, Some(Handle::clone(self)))
                .initialize(initial_len)
                .await?
        })
    }
}
impl BufferPool {
    pub async fn expand(&self, _expansion_command_priority: u16, additional: u32) -> Result<()> {
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
        let map_flags = MapFlags::MAP_SHARED | MapFlags::PROT_READ | MapFlags::PROT_WRITE;
        let len = usize::try_from(additional).or(Err(Error::new(EOVERFLOW)))?;

        // Note that this is not expected to take that long, however there's no point of blocking
        // other threads from acquiring their buffers, so that's why the lock is released and then
        // acquired again.
        let pointer = match self.handle {
            Some(ref h) => unsafe {
                h.mmap(h.reactor().primary_instance(), self.fd, map_flags, len, u64::from(new_offset))
                    .await?
            },
            None => unsafe {
                syscall::fmap(
                    self.fd,
                    &Mmap {
                        offset: new_offset.try_into().unwrap(),
                        size: len,
                        flags: map_flags,
                    },
                )? as *const ()
            },
        };

        let mut write_guard = self.mmap_map.write();
        let mmap_map = &mut *write_guard;

        let new_offset_half = MmapOffsetHalf::ready(new_offset);
        let old_offset_half = MmapOffsetHalf::pending(new_offset);

        let new_info_half = MmapInfoHalf {
            addr: Some(NonNull::new(pointer as *const u8 as *mut u8).expect("mmap returned null")),
            size: additional,
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
        let mut occ_write_guard = self.occ_map.write();
        let occ_map = &mut *occ_write_guard;

        debug_assert!(occ_map
            .map
            .get_or_less(
                OccOffsetHalf::unused(new_offset),
                &occ_map.forest,
                &RangeOffsetComparator
            )
            .map_or(false, |(k, v)| k.offset() < new_offset
                && k.offset() + v.size < new_offset + additional));
        occ_map
            .map
            .insert(
                OccOffsetHalf::unused(new_offset),
                OccInfoHalf::with_size(additional),
                &mut occ_map.forest,
                &RangeOffsetComparator,
            )
            .expect_none("expected newly-acquired slice not to conflict with any existing");

        Ok(())
    }

    // TODO: Shrink support

    /// Close an thus free the entire pool. If there are any pending commands that have guarded
    /// buffer slices from this pool, the entire memory will be leaked (TODO: Free as much memory
    /// as possible when this happens, rather than the entire pool).
    pub async fn close(self) -> Result<()> {
        if self.guarded_occ_count.load(Ordering::Acquire) > 0 {
            return Err(Error::new(EADDRINUSE));
        }

        match self.handle {
            Some(ref h) => unsafe {
                // Closing will automagically unmap all mmaps.
                h.close(h.reactor().primary_instance(), self.fd, false).await?;
            },
            None => {
                syscall::close(self.fd)?;
            }
        }
        Ok(())
    }
    unsafe fn new_from_raw(fd: usize, handle: Option<Handle>) -> Self {
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
            fd,
            handle,
        }
    }
    async fn initialize(mut self, initial_len: u32) -> Result<Self> {
        let map_flags = MapFlags::MAP_SHARED | MapFlags::PROT_READ | MapFlags::PROT_WRITE;
        let size = initial_len.try_into().or(Err(Error::new(EOVERFLOW)))?;

        let addr = match self.handle {
            Some(ref h) => unsafe { h.mmap(h.reactor().primary_instance(), self.fd, map_flags, size, 0).await? },
            None => unsafe {
                syscall::fmap(
                    self.fd,
                    &Mmap {
                        offset: 0,
                        flags: map_flags,
                        size,
                    },
                )? as *const ()
            },
        };

        let mmap_map = self.mmap_map.get_mut();

        let offset_half = MmapOffsetHalf::ready(0);
        let info_half = MmapInfoHalf {
            addr: Some(NonNull::new(addr as *const u8 as *mut u8).expect("mmap yielded null")),
            size: initial_len,
        };

        mmap_map
            .map
            .insert(offset_half, info_half, &mut mmap_map.forest, &());

        Ok(self)
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
            let base_pointer = mmap_v
                .addr
                .expect("Expected ready mmap entry to have a valid pointer");
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
    pub fn acquire_borrowed_slice(&self, len: u32, alignment: u32) -> Option<BufferSlice<'_>> {
        let (range, pointer) = self.acquire_slice(len, alignment)?;

        Some(BufferSlice {
            start: range.start,
            size: range.end - range.start,
            pointer,
            pool: Left(self),
            guard: None,
        })
    }
    pub fn acquire_weak_slice(
        self: &Arc<Self>,
        len: u32,
        alignment: u32,
    ) -> Option<BufferSlice<'static>> {
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

    unsafe fn reclaim_slice_inner(&self, slice: &BufferSlice<'_>) {
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
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        let count = self.guarded_occ_count.load(Ordering::Acquire);

        if count == 0 {
            let _ = syscall::close(self.fd);
        } else {
            log::warn!("Leaking entire buffer pool, since there were {} slices that were guarded by futures that haven't been completed", count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{mem, thread};

    fn setup_pool(maps: impl IntoIterator<Item = Vec<u8>>) -> (BufferPool, u32) {
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
            // redox_syscall should panic on other systems than Redox, and if this test was run on
            // Redox, this file descriptor would *most likely* be invalid
            fd: !0,
            handle: None,
            mmap_map: RwLock::new(mmap_map),
            occ_map: RwLock::new(occ_map),
            guarded_occ_count: AtomicUsize::new(0),
        };
        (pool, total_size)
    }
    fn setup_default_pool() -> (BufferPool, u32) {
        setup_pool(vec![vec![0u8; 32768], vec![0u8; 4096], vec![0u8; 65536]])
    }

    #[test]
    fn occ_map_acquisition_single_mmap() {
        let (pool, _) = setup_default_pool();

        let mut slices = Vec::new();

        loop {
            let mut slice = match pool.acquire_borrowed_slice(4096, 1) {
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
                        match pool.acquire_borrowed_slice(len, align) {
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
            let slice = match pool.acquire_borrowed_slice(SIZE, 1) {
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
            pool: &BufferPool,
            size: u32,
            align: u32,
            fill_byte: u8,
        ) -> BufferSlice {
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
