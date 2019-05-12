#include "AtomicHashTable2.h"

#include "bittricks.h"
#include "Timing.h"
#include "AtomicHelpers.h"
#include "RefCount.h"

#define RETURN_ON_ERROR(x) { int returnOnErrorResult = x; if(returnOnErrorResult != 0) return returnOnErrorResult; }

// (sizeof(InsideReference) because our tracked allocations are this much smaller)

// Means when we add more bits, overlaps stay as neighbors (which is a useful property when resizing)

#define getSlotBaseIndex atomicHashTable_getSlotBaseIndex

uint64_t log2RoundDown(uint64_t v) {
	return log2(v * 2) - 1;
}

// 3 64 bits for indirection, 1 for pool, 1 for ref count, 1 for hash and 1 BIT to know if the allocation is free.
//  Way to save space:
//  - The indirection could be on offset, as every value in each table redirects to the same thing, so that would work. It would also make moving a lot faster,
//      and we could potentially use that to redesign our redirection, maybe not even having to call redirect on every value. But... it is nice to have
//      redirect be abstract, and changing it now is too much work.
//  - The pool could be passed in everywhere, but that is a massive headache, and although that might use less memory, it will almost certainly slow down the code.
//  - The ref count could be less, but it's holding structure has to be 64 bit aligned anyway.
//  - The hash could be less than 64 bits, but... the downside by increasing hash collisions isn't worth it. If anything we should make hashing use 128 bits.
#define SIZE_PER_VALUE_ALLOC(table) (table->VALUE_SIZE + sizeof(HashValue) + InsideReferenceSize + MemPoolHashed_VALUE_OVERHEAD)
#define SIZE_PER_COUNT(table) (SIZE_PER_VALUE_ALLOC(table) + sizeof(AtomicSlot))

uint64_t minSlotCount(AtomicHashTable2* this) {
    uint64_t sizePerCount = SIZE_PER_COUNT(this);
    
    #define TARGET_PAGES 1
    #ifdef EVENT_ID_COUNT
    #undef TARGET_PAGES
    #define TARGET_PAGES EVENT_ID_COUNT
    #endif

    uint64_t minCount = (PAGE_SIZE * TARGET_PAGES - sizeof(AtomicHashTableBase) - InsideReferenceSize - sizeof(MemPoolHashed)) / sizePerCount;

    // Must be a power of 2, for getSlotBaseIndex
    minCount = 1ll << log2RoundDown(minCount);

    return minCount;
}
uint64_t getTableSize(AtomicHashTable2* this, uint64_t slotCount) {
    uint64_t tableSize = SIZE_PER_COUNT(this) * slotCount + sizeof(AtomicHashTableBase) + sizeof(MemPoolHashed);
	return tableSize;
}


uint64_t newShrinkSize(AtomicHashTable2* this, uint64_t slotCount, uint64_t fillCount) {
    uint64_t minCount = minSlotCount(this);
    if(fillCount < slotCount / 10) {
        return max(minCount, slotCount / 2);
    }
    // (so implicitly, never shrink to 0. Because then... why?)
    return 0;
}
uint64_t newGrowSize(AtomicHashTable2* this, uint64_t slotCount, uint64_t fillCount) {
    // Grow threshold and shrink threshold can't be within a factor of the grow/shrink factor, or else
    //  a grow will trigger a shrink...
    uint64_t minCount = minSlotCount(this);
    if(fillCount > slotCount / 10 * 4) {
        return max(minCount, slotCount * 2);
    }
    return 0;
}


void atomicHashTable2_memPoolFreeCallback(AtomicHashTable2* this, InsideReference* ref) {
    if(!Reference_HasBeenRedirected(ref)) {
        // And if it hasn't, it never will be, as the free callback is only called when all outside and inside references are gone...
        void* userValue = (byte*)ref + InsideReferenceSize + sizeof(HashValue);
        this->deleteValue(userValue);
    }
}

//todonext
// Wait... when we attempt to get a reference, we might temporarily turn a tombstone into having a count, which will make it look like it is moved?
//  Crap...

// Has to be a value copied from shared storage, or else this will race...
//  (which is why we don't put parentheses around value, as if using it raw fails to compile, then it probably isn't a locally copied variable...)
#define IS_VALUE_MOVED(value) (value.isNull && value.valueForSet != BASE_NULL.valueForSet && value.valueForSet != BASE_NULL_MAX.valueForSet)

#define IS_BLOCK_END(value) (value.valueForSet == 0 || value.valueForSet == BASE_NULL_MAX.valueForSet || value.valueForSet == BASE_NULL2.valueForSet)


// Doesn't do any moves, just makes undoing reserve updates easier, because undoing
//  a reserve update shouldn't really trigger a resize anyway... (and it definitely
//  won't be relied upon to).
void atomicHashTable2_updateReservedUndo(
    AtomicHashTable2* this,
    AtomicHashTableBase* curAlloc,
    int64_t slotsReservedDelta,
    int64_t slotsReservedWithNullsDelta
) {
    if(slotsReservedDelta < 0 && curAlloc->slotsReserved == 0) {
        // We can't go negative...
        OnError(3);
        return;
    }
    //printf("Add (undo) %lld to slotsReserved\n", slotsReservedDelta);
    InterlockedAdd64((LONG64*)&curAlloc->slotsReserved, (LONG64)slotsReservedDelta);
    InterlockedAdd64((LONG64*)&curAlloc->slotsReservedWithNulls, (LONG64)slotsReservedWithNullsDelta);
}
// Doesn't apply any moves.
// Returns 1 when it hasn't updated the reserved count
int atomicHashTable2_updateReserved(
    AtomicHashTable2* this,
    AtomicHashTableBase* curAlloc,
    OutsideReference* pNewAlloc,
    int64_t slotsReservedDelta,
    int64_t slotsReservedWithNullsDelta
) {
    if(curAlloc) {
        //printf("Add %lld to slotsReserved\n", slotsReservedDelta);
        InterlockedAdd64((LONG64*)&curAlloc->slotsReserved, (LONG64)slotsReservedDelta);
        InterlockedAdd64((LONG64*)&curAlloc->slotsReservedWithNulls, (LONG64)slotsReservedWithNullsDelta);

        if(curAlloc->newAllocation.valueForSet != 0) {
            if(curAlloc->slotsReservedWithNulls >= curAlloc->slotsCount) {
                if(curAlloc->newAllocation.isNull) {
                    return 1;
                }
                // This means that we used up all existing entries before we could finish our move,
                //  which means functions that modify updateReserved are doing enough work to move entries
                //  before they increase it. Either they aren't moving anything, or the constant amount they move
                //  is too low.
                // But... because we move entire groups at once... this should really be impossible...
                OnError(5);
                return 5;
            }
            // If we are already moving don't both checking if we should move again... because we can't...
            return 0;
        }
    }

    uint64_t newSlotCount = 0;
    uint64_t curSlotCount = curAlloc ? curAlloc->slotsCount : 0;
    uint64_t slotsReservedNulls = curAlloc ? curAlloc->slotsReservedWithNulls : 1;
    uint64_t slotsReservedNoNulls = curAlloc ? curAlloc->slotsReserved : 1;
    // If we really don't have that many nulls, then don't use the count with nulls. This makes sure
    //  we only do same size resizes when we have enough nulls, instead of when we are near a threshold and
    //  have only a few nulls (as resizing to the same size prematurely can make all operations O(N)).
    // (We require at least 20% nulls to do a resize of the same size, because if we use slotsReservedNulls then
    //      any resize will increase or decrease our size)
    if(slotsReservedNoNulls * 12 / 10 > slotsReservedNulls) {
        slotsReservedNulls = slotsReservedNoNulls;
    }

	#ifdef DEBUG
    if(curAlloc && curAlloc->slotsCount == 32 && slotsReservedNulls > 28) {
        breakpoint();
    }

    if(curAlloc) {
        if(slotsReservedNoNulls * 100 < curAlloc->slotsCount && curAlloc->slotsCount > 1024 && curAlloc->finishedMovingInto) {
			// Why haven't we shrunk before this?
            breakpoint();
        }
    }
	#endif

    newSlotCount = newShrinkSize(this, curSlotCount, slotsReservedNoNulls);
    newSlotCount = newSlotCount != 0 && newSlotCount != curSlotCount ? newSlotCount : newGrowSize(this, curSlotCount, slotsReservedNulls);
    if(!newSlotCount || newSlotCount == curSlotCount) {
        return 0;
    }

    // Again with but now to get the actual size needed after a move (so after nulls, as in tombstones, are removed).
    newSlotCount = newShrinkSize(this, curSlotCount, slotsReservedNoNulls);
    newSlotCount = newSlotCount ? newSlotCount : newGrowSize(this, curSlotCount, slotsReservedNoNulls);
    if(!newSlotCount) {
        newSlotCount = curAlloc->slotsCount;
    }

    if(curAlloc && !curAlloc->finishedMovingInto) {
        if(newSlotCount <= curSlotCount) {
            // We aren't going to shrink WHILE we are being moved into. That doesn't make any sense, obviously
            //  our count will increase as we continue moving...
            return 0;
        }

        // Of course, while moving to deal with null entries we might grow, and then want to grow again while moving. This is fine,
        //  we will finish the previous move before we get dangerously empty

        if(slotsReservedNoNulls > curSlotCount / 10 * 8) {
            // We are within 80% of being filled. This is dangerously close, this shouldn't happen, we should move enough entries
            //  on each insert to prevent this from happening...
            OnError(9);
        }
        return 0;
    }

    // Create and prepare the new allocation
    OutsideReference newAllocation;
    AtomicHashTableBase* newTable;
    uint64_t tableSize = getTableSize(this, newSlotCount);
    Reference_Allocate((MemPool*)&memPoolSystem, &newAllocation, &newTable, tableSize, 0);
    if(!newTable) {
        return 3;
    }



    newTable->slotsCount = newSlotCount;
    uint64_t logSlotsCount = log2(newSlotCount);
    newTable->logSlotsCount = logSlotsCount;
    newTable->slots = (void*)((byte*)newTable + sizeof(AtomicHashTableBase));
    newTable->valuePool = (void*)((byte*)newTable->slots + sizeof(AtomicSlot) * newSlotCount);
    newTable->moveState.destIndex = UINT32_MAX;
    newTable->moveState.sourceIndex = 0;
    newTable->newAllocation.valueForSet = 0;
    newTable->slotsReserved = 0;
    newTable->slotsReservedWithNulls = 0;
    newTable->finishedMovingInto = false;

    

    if(pNewAlloc != &this->currentAllocation) {
        memset(newTable->slots, 0xFF, newSlotCount * sizeof(uint64_t));
    } else {
        memset(newTable->slots, 0x00, newSlotCount * sizeof(uint64_t));
        newTable->finishedMovingInto = true;
    }

    InsideReference* newTableRef = Reference_AcquireInside(&newAllocation);

    MemPoolHashed pool = MemPoolHashedDefault(SIZE_PER_VALUE_ALLOC(this), newSlotCount, logSlotsCount, newTableRef, this, atomicHashTable2_memPoolFreeCallback);
    *newTable->valuePool = pool;
    MemPoolHashed_Initialize(newTable->valuePool);

	/*
    if(logSlotsCount > 10) {
        breakpoint();
        MemPoolHashed_Allocate(newTable->valuePool, newTable->valuePool->VALUE_SIZE - MemPoolHashed_VALUE_OVERHEAD, (newSlotCount - 1) << (64 - logSlotsCount));
    }
	*/

    if(InterlockedCompareExchange64(
        (LONG64*)pNewAlloc,
        newAllocation.valueForSet,
        0
    ) != 0) {
        Reference_Release(&newAllocation, newTableRef);
        DestroyUniqueOutsideRef(&newAllocation);
        return 1;
    }
    //printf("starting new move size %llu to %llu\n", curAlloc ? curAlloc->slotsCount : 0, newTable->slotsCount);
    return 0;
}

void atomicHashTable2_replaceNullsInBlock(
    uint64_t sourceBlockStart,
    uint64_t sourceIndex,
    AtomicHashTableBase* curAlloc,
    AtomicHashTableBase* newAlloc
) {
    bool growing = newAlloc->slotsCount > curAlloc->slotsCount;
    bool shrinking = newAlloc->slotsCount < curAlloc->slotsCount;

    // Source block finished, so we know that the corresponding dest block is also finished.
    //  This means we need to set all BASE_NULL_MAXs to 0, and reset 
        
    // We know that the source block won't be extended, as we set BASE_NULL in this source slot, and so anything trying to fill in that slot will
    //  instead help apply this move.

    // Hmm... I think the rounding that shrinking experiences should work itself out... Not 100% sure though...
    // TODO: We should verify the rounding is okay...
    uint64_t destBlockEnd = (sourceIndex % curAlloc->slotsCount) + 1;
    if(growing) {
        destBlockEnd = destBlockEnd * 2;
    } else if(shrinking) {
        //breakpoint();
        destBlockEnd = destBlockEnd / 2;
    }
    uint64_t destBlockCur = sourceBlockStart != UINT32_MAX ? sourceBlockStart : (sourceIndex % curAlloc->slotsCount);
    if(growing) {
        destBlockCur = destBlockCur * 2;
    } else if(shrinking) {
        destBlockCur = destBlockCur / 2;
    }

    while(destBlockCur != destBlockEnd) {
        OutsideReference value = newAlloc->slots[destBlockCur].value;
        if(value.valueForSet == BASE_NULL_MAX.valueForSet) {
            InterlockedCompareExchange64(
                (LONG64*)&newAlloc->slots[destBlockCur].value,
                0,
                BASE_NULL_MAX.valueForSet
            );
        }
        destBlockCur++;
    }
}


// MUST be called before newAlloc is accessed.
// Returns 0 if moves were applied,
// Returns > 1 on error
//  (never returns 1)
int atomicHashTable2_applyMoveTableOperationInner(
    AtomicHashTable2* this,
    AtomicHashTableBase* curAlloc,
    AtomicHashTableBase* newAlloc
) {
    uint64_t HASH_VALUE_SIZE = this->HASH_VALUE_SIZE;
    uint64_t VALUE_SIZE = this->VALUE_SIZE;


    // Okay, inserts...

    //todonext
    // Alright... how could moving, skip over a value that definitely exists? Because that is what happening, a value not deleted,
    //  doesn't get moved, it just completely gets skipped over. Maybe... print all cases we skip over, and why?

    // Hash block checking doesn't work because of wrap around (at least, it can't be efficient because of wrap around).
    //  So instead we just move A block, and if we ran into a moved element in a block, then either
    //  that block was already moved, or we just moved it... so this works...

	// This has to be a certain size above our grow/shrink thresholds or else we will try to grow before we finish moving!
    uint64_t minApplyCount = 64;

    

    uint64_t countApplied = 0;

    MoveStateInner* pMoveState = &curAlloc->moveState;

    uint64_t loops = 0;
    uint64_t retries = 0;
    uint64_t savedRetries = 0;
    uint64_t lastRetryId = 0;
    uint64_t lastLastRetryId = 0;
    bool setDest = false;

    MoveStateInner firstObservedState = *pMoveState;

    MoveStateInner moveState = { 0 };
    while(true) {
        loops++;
        if(loops > curAlloc->slotsCount * 10) {
            OnError(9);
            return 9;
        }

        // #define IS_VALUE_MOVED(value) (value.isNull && value.valueForSet != BASE_NULL.valueForSet)

        // Move state is small enough that access are implicitly atomic (as it is 64 bits, and 64 bit aligned)
        MoveStateInner prevMoveState = moveState;
        moveState = *pMoveState;
        if(moveState.sourceIndex != prevMoveState.sourceIndex) {
            if(lastRetryId) {
                lastLastRetryId = lastRetryId;
            }
            savedRetries += retries;
            retries = 0;
            lastRetryId = 0;
            setDest = false;
        }

        if(moveState.sourceIndex >= curAlloc->slotsCount) {
            InsideReference* newAllocRef = (void*)((byte*)newAlloc - InsideReferenceSize);
            newAlloc->finishedMovingInto = true;

            InsideReference* curAllocRef = (void*)((byte*)curAlloc - InsideReferenceSize);

            OutsideReference newAllocOutside = Reference_CreateOutsideReference(newAllocRef);
            if(!Reference_ReplaceOutside(&this->currentAllocation, curAllocRef, newAllocOutside)) {
                DestroyUniqueOutsideRef(&newAllocOutside);
            }

            Reference_DestroyOutsideMakeNull(&curAlloc->newAllocation, newAllocRef);

            // Hmm... does order matter with destroying the mem pool and redirecting the allocation?
            //  It seems better to destroy the mem pool after we redirect and allocation...
            MemPoolHashed_Destruct(curAlloc->valuePool);
            return 0;
        }

        bool endOfBlock = false;

        OutsideReference* pSource = &curAlloc->slots[moveState.sourceIndex].value;
        // Make sure curAlloc->slots[sourceIndex] is frozen, and marked as moved
        OutsideReference source = *pSource;

        {
            // We HAVE to make sure all source values become moved, or else inserts might insert into old allocations,
            //  and then wonder where their inserts are (as in, the old allocation could have lots of BASE_NULL_MAXs, so
            //  seem fine, so it wouldn't be able to know it inserted in the wrong allocation, without comparing against
            //  currentAllocation again... but that is... extra work?)
            if(!IS_VALUE_MOVED(source)) {
                if(source.valueForSet == 0 || source.valueForSet == BASE_NULL_MAX.valueForSet) {
                    if(InterlockedCompareExchange64(
                        (LONG64*)pSource,
                        BASE_NULL2.valueForSet,
                        source.valueForSet
                    ) != source.valueForSet) {
                        retries++;
                        lastRetryId = 1;
                        continue;
                    }
                    endOfBlock = true;
                } else if(source.valueForSet == BASE_NULL2.valueForSet) {
                    endOfBlock = true;
                }
                else if(source.valueForSet == BASE_NULL.valueForSet) {
                    if(InterlockedCompareExchange64(
                        (LONG64*)pSource,
                        BASE_NULL1.valueForSet,
                        source.valueForSet
                    ) != source.valueForSet) {
                        retries++;
                        lastRetryId = 2;
                        continue;
                    }
                }
                else {
                    

                    // Freeze it, BEFORE we redirect it.
                    InsideReference* sourceRef = Reference_Acquire(pSource);
                    if(!sourceRef) {
                        retries++;
                        lastRetryId = 3;
                        continue;
                    }
                    if(!Reference_ReduceToZeroRefsAndSetIsNull(
                        pSource,
                        sourceRef
                    )) {
						byte testCopy[InsideReferenceSize] = { 0 };
						memcpy(testCopy, sourceRef, InsideReferenceSize);
                        InsideReference* pTestCopy = (void*)testCopy;
                        IsInsideRefCorrupt(sourceRef);
                        Reference_Release(pSource, sourceRef);
                        retries++;
                        lastRetryId = 4;
                        continue;
                    }
                    IsInsideRefCorrupt(sourceRef);
                    Reference_Release(&emptyReference, sourceRef);
                }
            }
        }

        bool alreadyInserted = false;

        // Redirect it
        InsideReference* redirectedValue = nullptr;
        {
            InsideReference* sourceRef = Reference_AcquireIfNull(pSource);
            if(!sourceRef) {
                alreadyInserted = true;
            } else {
                //todonext
                // Maybe... instead of this weird stuff, just call a function to explicitly follow redirections. Which we will call before
                //  and after we redirect.

                if(MemPoolHashed_IsInPool(curAlloc->valuePool, sourceRef)) {
                    HashValue* value = Reference_GetValue(sourceRef);
                    OutsideReference newValueRef = { 0 };
                    HashValue* newValue = nullptr;
                    Reference_Allocate((MemPool*)newAlloc->valuePool, &newValueRef, &newValue, this->HASH_VALUE_SIZE, value->hash);
                    if(!newValue) {
                        IsInsideRefCorrupt(sourceRef);
                        Reference_Release(&emptyReference, sourceRef);
                        // We might just have completely finished the move, and have had the newAlloc already been destructed!
                        if(newAlloc->valuePool->countForSet.destructed) {
                            return 1;
                        }
                        return 3;
                    }

                    newValue->hash = value->hash;
                    memcpy(
                        (byte*)newValue + sizeof(HashValue),
                        (byte*)value + sizeof(HashValue),
                        VALUE_SIZE
                    );

                    InsideReference* newValueRefInside = Reference_Acquire(&newValueRef);
                    IsInsideRefCorrupt(newValueRefInside);
                    IsInsideRefCorrupt(sourceRef);
                    Reference_RedirectReference(
                        sourceRef,
                        newValueRefInside);
                    Reference_DestroyOutside(&newValueRef, newValueRefInside);
                    Reference_Release(&emptyReference, newValueRefInside);
                } else {
                    printf("not in pool\n");
                }

                redirectedValue = Reference_FollowRedirects(sourceRef);
                IsInsideRefCorrupt(redirectedValue);
            }
        }

        // Rule for finding where to put it:
        // Either it is in newAlloc, sourceIndex has been updated, or it hasn't been inserted every yet?
        //  - Because once we insert it, to remove it applyMove has to be run, which updates sourceIndex
        // This means, that once we find a destIndex, if we try to insert it and find the dest slot is already being used,
        //  and not by our value, if we find sourceIndex is the same, we can atomically rollback destIndex.

        // Find a destination for the source, reserving it (now that the source is frozen, the hash won't change)
        if(!alreadyInserted && moveState.destIndex == UINT32_MAX) {
            HashValue* value = Reference_GetValue(redirectedValue);

            uint64_t baseIndex = getSlotBaseIndex(newAlloc, value->hash);
            uint64_t index = baseIndex;
            bool forceRetry = false;
            while(true) {
                OutsideReference* pDest = &newAlloc->slots[index].value;
                OutsideReference dest = *pDest;
                if(FAST_CHECK_POINTER(dest, redirectedValue)) {
                    Reference_Release(&emptyReference, redirectedValue);
                    moveState.destIndex = (uint32_t)index;
                    alreadyInserted = true;
                    break;
                }
                if(dest.valueForSet == BASE_NULL_MAX.valueForSet) {
                    MoveStateInner newMoveState = moveState;
                    newMoveState.destIndex = (uint32_t)index;
                    if(InterlockedCompareExchange64(
                        (LONG64*)pMoveState,
                        newMoveState.valueForSet,
                        moveState.valueForSet
                    ) != moveState.valueForSet) {
						forceRetry = true;
                        lastRetryId = 11;
						break;
                    }
                    moveState = newMoveState;
                    break;
                }
                index = (index + 1) % newAlloc->slotsCount;
                if(index == baseIndex) {
                    forceRetry = true;
                    lastRetryId = 5;
                    // We ran out of space while moving. This must be because everything has already finished moving, right?
                    
                    MoveStateInner moveState = *pMoveState;
                    if(moveState.sourceIndex < curAlloc->slotsCount) {
                        Reference_Release(&emptyReference, redirectedValue);
                        // Ran out of space... the BASE_NULL_MAXes do get used up by new inserts, but those should help us
                        //  most, and so we shouldn't be running our of BASE_NULL_MAXes to insert into...

                        OnError(9);
                        return 9;
                    }
                    break;
                }
            }
            if(forceRetry) {
                Reference_Release(&emptyReference, redirectedValue);
                retries++;
                continue;
            }
        }

        // alreadyInserted is set for non-pointer values, so if a hanging piece of code runs, then it will either try to get a destIndex,
        //  and fail because sourceIndex has been updated, OR try to use the destIndex, but skip this because it saw the final null, and then
        //  try to update the sourceIndex (and fail).
        //  - (Or, of course, see the inserted value into dest, and use that)
        if(!alreadyInserted) {
            // Now moveState.destIndex will have a value
            OutsideReference* pDest = &newAlloc->slots[moveState.destIndex].value;

            OutsideReference newDest = Reference_CreateOutsideReference(redirectedValue);

            InterlockedIncrement64(&newAlloc->slotsReserved);
            InterlockedIncrement64(&newAlloc->slotsReservedWithNulls);
            if(InterlockedCompareExchange64(
                (LONG64*)pDest,
                newDest.valueForSet,
                BASE_NULL_MAX.valueForSet
            ) != BASE_NULL_MAX.valueForSet) {
                InterlockedDecrement64(&newAlloc->slotsReserved);
                InterlockedDecrement64(&newAlloc->slotsReservedWithNulls);

                Reference_DestroyOutside(&newDest, redirectedValue);
                IsInsideRefCorrupt(redirectedValue);

                // Rollback destIndex, and try again to find a new one (if this hangs, it is fine. Worst case we rollback
                //  when destIndex has already been set and inserted... which is fine, because the next loop will see that
                //  it has been inserted (and it can't get deleted, as before deletions happen in newAlloc atomicHashTable2_applyMoveTableOperationInner
                //  has to be called, which will run this code again, making the move finalize first).
                // AND operations only access newAlloc WHEN they see a move is inside (or after) the block they want to in curAlloc, which
                //  means either they access curAlloc completely, or they make sure the entire block they want to access in curAlloc (that
                //  we might be moving), is moved... which means, anything we might be moving will be fully moved before a delete deletes it...

                OutsideReference newDest = *pDest;
                if(!FAST_CHECK_POINTER(newDest, redirectedValue)) {
                    MoveStateInner curMoveState = *pMoveState;
                    if(curMoveState.sourceIndex == moveState.sourceIndex) {
                        // So actually... doesn't that mean in this case destIndex can't be deleted out, and actually it must have been inserted?
                        //  So this should be impossible...
                        // Also, shouldn't it be impossible to insert here without running move too? Cause that is the other case this could happen, if
                        //  an insert happens to that slot before we can do our insert...
                        breakpoint();
                        MoveStateInner newMoveState = moveState;
                        newMoveState.destIndex = UINT32_MAX;
                        InterlockedCompareExchange64(
                            (LONG64*)pMoveState,
                            newMoveState.valueForSet,
                            moveState.valueForSet
                        );
                    }
                }

                Reference_Release(&emptyReference, redirectedValue);

                retries++;
                lastRetryId = 6;
                continue;
            } else {
                setDest = true;
            }
            IsInsideRefCorrupt(redirectedValue);
            Reference_Release(pDest, redirectedValue);
        }

        MoveStateInner newMoveState = moveState;
        newMoveState.sourceIndex++;
        newMoveState.destIndex = UINT32_MAX;
        if(InterlockedCompareExchange64(
            (LONG64*)pMoveState,
            newMoveState.valueForSet,
            moveState.valueForSet
        ) == moveState.valueForSet) {
            if(moveState.destIndex != UINT32_MAX) {
                OutsideReference* pDest = &newAlloc->slots[moveState.destIndex].value;
                InsideReference* destRef = Reference_Acquire(pDest);
                if(destRef) {
                    IsInsideRefCorrupt(destRef);
                    Reference_Release(pDest, destRef);
                }
            }
            //todonext
            // So... a thread hung at or in Reference_DestroyThroughNull, the other thread continued, and then when the original thread
            //  got around to finishing this, it destroyed the source value, but then when freeing the nextRedirectValue it found
            //  the redirected (the value at moveState.destIndex), was now corrupt, with a leaked outside reference... So...
            //  it looks like one of the retries is leaving us in a bad state...
            Reference_DestroyThroughNull(pSource, BASE_NULL1);
            countApplied++;
            //todonext
            // Okay, with two threads, both moving, I had one thread with a lower sourceIndex find a bad value,
            //  and the higher thread is at a higher index, doing fine. So... I think the higher thread broke that value,
            //  in the move (although...). So... check pDest again here?
            if(moveState.destIndex != UINT32_MAX) {
                OutsideReference* pDest = &newAlloc->slots[moveState.destIndex].value;
                InsideReference* destRef = Reference_Acquire(pDest);
                if(destRef) {
                    IsInsideRefCorrupt(destRef);
                    Reference_Release(pDest, destRef);
                }
            }

            if(endOfBlock && countApplied > minApplyCount) {
                // Not only is it the endOfBlock, but... because we replace it with BASE_NULL1, the original block in curAlloc will never
                //  grow, so we will never move anything from this block to newAlloc again.
                return 0;
            }
        }
    }

	// Unreachable
	OnError(9);
	return 9;
}



int atomicHashTable2_removeInner2(
    AtomicHashTable2* this,
    uint64_t hash,
	void* callbackContext,
	bool(*callback)(void* callbackContext, void* value),
    AtomicHashTableBase* table
) {
    uint64_t index = getSlotBaseIndex(table, hash);
    uint64_t loopCount = 0;
    while(true) {
        #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
        InterlockedIncrement64((LONG64*)&this->searchLoops);
        #endif
        OutsideReference* pValue = &table->slots[index].value;
        OutsideReference valueRef = *pValue;

        if(IS_VALUE_MOVED(valueRef)) {
            return 1;
        }
		if (IS_BLOCK_END(valueRef)) {
			return 0;
		}
        if(valueRef.valueForSet != BASE_NULL.valueForSet) {
            InsideReference* ref = Reference_Acquire(pValue);
            if(!ref) {
                continue;
            }
            HashValue* value = Reference_GetValue(ref);
            if(value->hash == hash) {
                void* valueVoid = (byte*)value + sizeof(HashValue);
                bool shouldRemove = callback(callbackContext, valueVoid);
                if(shouldRemove) {
                    bool destroyedRef = Reference_DestroyOutsideMakeNull(pValue, ref);
                    if(!destroyedRef) {
                        Reference_Release(pValue, ref);
                        continue;
                    }
                    else {
                        atomicHashTable2_updateReserved(this, table, &table->newAllocation, -1, 0);
                        //printf("Removed %llu for %llu\n", index, hash);
                    }
                }
            }
            Reference_Release(pValue, ref);
        }

        index = (index + 1) % table->slotsCount;
        if(index == 0) {
            loopCount++;
            if(loopCount > 10) {
                // Too many loops around the table...
                OnError(9);
                return 9;
            }
        }
    }

	// Unreachable
	OnError(13);
	return 0;
}

int atomicHashTable2_removeInner(
	AtomicHashTable2* this,
    uint64_t hash,
	void* callbackContext,
	bool(*callback)(void* callbackContext, void* value),
    AtomicHashTableBase* table,
    AtomicHashTableBase* newTable
) {
    int result = atomicHashTable2_removeInner2(this, hash, callbackContext, callback, table);
    if(result > 1) {
        return result;
    }
    // It is important to move after checking table, that way either the move is on the cusp of what we read
    //  (and so this move makes the newTable have the info we need), newTable already had the data we needed,
    //  OR table had the data we needed. If we do it at the beginning we could easily leave the move in the middle
    //  of where we check, requiring an extra iteration.
    // (and of course, we always apply a move, even if the operation succeeded, because we want to finish up the move,
    //      as just the presense of newTable slows down every operation).
    if(newTable) {
        int moveResult = atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);
        if(moveResult > 1) {
            return moveResult;
        }
        if(result == 1) {
            return atomicHashTable2_removeInner2(this, hash, callbackContext, callback, newTable);
        }
    }
    return result;
}

int AtomicHashTable2_remove(
	AtomicHashTable2* this,
	uint64_t hash,
	void* callbackContext,
	// On true, removes the value from the table
    //  May be called multiple times for the value for one call.
	bool(*callback)(void* callbackContext, void* value)
) {
    #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
    InterlockedIncrement64((LONG64*)&this->searchStarts);
    #endif
    while(true) {
        InsideReference* tableRef = Reference_Acquire(&this->currentAllocation);
        AtomicHashTableBase* table = Reference_GetValue(tableRef);
        if(!table) {
            return 0;
        }

        InsideReference* newTableRef = Reference_Acquire(&table->newAllocation);
        AtomicHashTableBase* newTable = Reference_GetValue(newTableRef);

        int result = atomicHashTable2_removeInner(this, hash, callbackContext, callback, table, newTable);

        Reference_Release(&table->newAllocation, newTableRef);
        Reference_Release(&this->currentAllocation, tableRef);

        if(result != 1) {
            return result;
        }
        #ifdef DEBUG
        if(IsSingleThreadedTest) {
            // We shouldn't have to retry while single threaded
            OnError(3);
        }
        #endif
    }
}



#pragma pack(push, 1)
typedef struct {
    OutsideReference* refSource;
    InsideReference* ref;
    HashValue* value;
} FindResult;
#pragma pack(pop)


// May return 1 just because of a move, and not because of a pValuesFoundLimit changed
int atomicHashTable2_findInner4(
    AtomicHashTable2* this,
    AtomicHashTableBase* table,
    FindResult* valuesFound,
    uint64_t* pValuesFoundCur,
    uint64_t* pValuesFoundLimit,
    uint64_t hash,
    AtomicHashTableBase* newTable
) {
    uint64_t index = getSlotBaseIndex(table, hash);
    uint64_t baseIndex = index;
    uint64_t loopCount = 0;
    while(true) {
        #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
        InterlockedIncrement64((LONG64*)&this->searchLoops);
        #endif
        OutsideReference* pValue = &table->slots[index].value;
        OutsideReference value = *pValue;

        if(IS_BLOCK_END(value)) {
            //printf("Finished found %llu proof from %llu to %llu of %llu\n", *pValuesFoundCur, baseIndex, index, hash);
            return 0;
        }
        if(value.valueForSet != BASE_NULL.valueForSet) {
			if (IS_VALUE_MOVED(value)) {
                if(newTable) {
                    // If we have a newTable, then we aren't the new table, so seeing moved values is okay
                    if(IS_FROZEN_POINTER(value)) {
                        // But if it is presently being moved, we have to finish the move, so we can read it when we search in the newTable
                        //  (but we don't need to research the current entry, the move will just set it to some BASE_NULL value.
                        RETURN_ON_ERROR(atomicHashTable2_applyMoveTableOperationInner(this, table, newTable));
                    }
                } else {
                    return 1;
                }
			}
			else {
				InsideReference* valueRef = Reference_Acquire(pValue);
				#ifdef DEBUG
				if (IsSingleThreadedTest) {
					if (Reference_HasBeenRedirected(valueRef)) {
						breakpoint();
					}
				}
				#endif
				if (!valueRef) {
					// Might be because of move, but is more likely just because the value was set to the tombstone value
					//  (BASE_NULL), so we just just try rereading it
					continue;
				}
				HashValue* hashValue = Reference_GetValue(valueRef);
				if (hashValue->hash != hash) {
					Reference_Release(pValue, valueRef);
				}
				else {
					FindResult result;
					result.refSource = pValue;
					result.ref = valueRef;
					result.value = hashValue;

					uint64_t valuesFoundCur = *pValuesFoundCur;
					uint64_t valuesFoundLimit = *pValuesFoundLimit;
					valuesFoundCur++;
					if (valuesFoundCur > valuesFoundLimit) {
						Reference_Release(pValue, valueRef);
						// Eh... *10, because we don't know how many entries there might be. We could loop and count how many values
						//  there might be, and allocate less... but I don't think we need to optimize the case of many results right now...
						*pValuesFoundLimit = valuesFoundLimit * 10 + 1;
						//printf("found count retry\n");
						return -1;
					}

					//printf("found at %llu for %llu\n", index, hash);
					*pValuesFoundCur = valuesFoundCur;
					valuesFound[valuesFoundCur - 1] = result;
				}
			}
        }
        
        index = (index + 1) % table->slotsCount;
        // If we wrap around, we have to retry, as wrapping around makes it possible to double count values...
        if(index == baseIndex) {
            #ifdef DEBUG
            if(IsSingleThreadedTest) {
                breakpoint();
            }
            #endif
            //printf("full loop retry\n");
            return -1;
        }
    }
}
bool stop = false;
int atomicHashTable2_findInner3(
    AtomicHashTable2* this,
    uint64_t hash,
    FindResult* valuesFound,
    uint64_t* pValuesFoundCur,
    uint64_t* pValuesFoundLimit,
    void* callbackContext,
    void(*callback)(void* callbackContext, void* value),
    AtomicHashTableBase* table,
    AtomicHashTableBase* newTable
) {
    //todonext
    // If we make all accessors like this, then we can change atomicHashTable2_applyMoveTableOperationInner to not need to complete blocks!
    //  So this means, move first, and then check for IS_FROZEN_POINTER, moving if that is true.
    //      - This means... that we only do moves equals to the number of times we read an entry while someone else is moving it.
    //          Which I haven't been able to make a test case that triggers this often, so it will probably almost never happen?

    if(newTable) {
        int moveResult = atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);
        if(moveResult > 0) {
            return moveResult;
        }
    }

    int result = atomicHashTable2_findInner4(this, table, valuesFound, pValuesFoundCur, pValuesFoundLimit, hash, newTable);
    if(result > 1) {
        return result;
    }

    if(newTable) {
        result = atomicHashTable2_findInner4(this, newTable, valuesFound, pValuesFoundCur, pValuesFoundLimit, hash, nullptr);
        if(result == 0) {
            // So... values might be cross tables, which means we may have seen the same value twice. In which case, the first value will always
            //  have been redirected (as it is redirected before it gets moved), so if anything is redirected... we probably read duplicates...
            //	(only checked if we used newTable. Otherwise even if the values we returned are redirected, they were not moved when we read them,
            //		so our read was consistent, even if their location is different now).
            for (uint64_t i = 0; i < *pValuesFoundCur; i++) {
                if (Reference_HasBeenRedirected(valuesFound[i].ref)) {
                    #ifdef DEBUG
                    if (IsSingleThreadedTest) {
                        breakpoint();
                    }
                    #endif
                    result = 1;
                    break;
                }
            }
        }
    } else {
		// This should be hit at sometime...
		//breakpoint();
    }
    return result;
}

int atomicHashTable2_findInner2(
    AtomicHashTable2* this,
    uint64_t hash,
    FindResult* valuesFound,
    uint64_t* pValuesFoundLimit,
    void* callbackContext,
    void(*callback)(void* callbackContext, void* value),
    AtomicHashTableBase* table,
    AtomicHashTableBase* newTable
) {
    uint64_t valuesFoundCur = 0;
    int result = atomicHashTable2_findInner3(this, hash, valuesFound, &valuesFoundCur, pValuesFoundLimit, callbackContext, callback, table, newTable);
    if(result == 0) {
        for(uint64_t i = 0; i < valuesFoundCur; i++) {
            void* value = (byte*)valuesFound[i].value + sizeof(HashValue);
            callback(callbackContext, value);
        }
    }
    for(uint64_t i = 0; i < valuesFoundCur; i++) {
        Reference_Release(valuesFound[i].refSource, valuesFound[i].ref);
    }
    return result;
}

int atomicHashTable2_findInner(
    AtomicHashTable2* this,
    uint64_t hash,
    FindResult* valuesFound,
    uint64_t* pValuesFoundLimit,
    void* callbackContext,
    void(*callback)(void* callbackContext, void* value)
) {
    uint64_t loops = 0;
    while(true) {
        InsideReference* tableRef = Reference_Acquire(&this->currentAllocation);
        AtomicHashTableBase* table = Reference_GetValue(tableRef);
        if(!table) {
            return 0;
        }

        InsideReference* newTableRef = table->newAllocation.valueForSet ? Reference_Acquire(&table->newAllocation) : 0;
        AtomicHashTableBase* newTable = Reference_GetValue(newTableRef);

        int result = atomicHashTable2_findInner2(this, hash, valuesFound, pValuesFoundLimit, callbackContext, callback, table, newTable);
        //todonext
        // Okay... why do we not need to check if the tables we searched are still the active tables? Because... I am seeing
        //  a problem where a value doesn't appear until we search for it twice, which makes me think it is because
        //  we were searching while moving, and that somehow made us not find a value... But that shouldn't be possible, right?
        //todonext
        // Okay, so now, we found it, but testGetSome didn't. So... definitely, moves are making it look like our values are temporarily gone.

        uint64_t size = table->slotsCount;

        Reference_Release(&table->newAllocation, newTableRef);
        Reference_Release(&this->currentAllocation, tableRef);

        if(result != 1) {
            return result;
        }
        #ifdef DEBUG
        if(IsSingleThreadedTest) {
            // We shouldn't have to retry while single threaded
            OnError(3);
        }
        #endif

        loops++;
		if (loops > size * 9) {
			breakpoint();
		}
        if(loops > size * 10) {
            // Too many loops, likely an internal error, or way too much contention.
            OnError(9);
            return 9;
        }
    }
}

int atomicHashTable2_findFull(
    AtomicHashTable2* this,
    uint64_t hash,
    void* callbackContext,
    // May return results that don't equal the given hash, so a deep comparison should be done on the value
	//	to determine if it is the one you want.
	// (and obviously, may call this callback multiple times for one call, but not more than one time per call
    //  for the same value, assuming no value is added to the table more than once).
    void(*callback)(void* callbackContext, void* value)
) {
    if(this->currentAllocation.valueForSet == 0) {
        return 0;
    }

	FindResult stackValuesFound[8];

    uint64_t valuesFoundLimit = sizeof(stackValuesFound) / sizeof(FindResult);
	FindResult* valuesFound = stackValuesFound;
    while(true) {
        if(this->currentAllocation.valueForSet == 0) {
            return 0;
        }

        int result = atomicHashTable2_findInner(this, hash, valuesFound, &valuesFoundLimit, callbackContext, callback);
		if (valuesFound != stackValuesFound) {
			MemPoolFixed_Free(&this->findValuePool, valuesFound);
		}
        if(result == -1) {
			// TODO: If we don't memset the allocation we can save a lot of time. Also, using a stack allocated buffer will probably be a lot faster.
            valuesFound = MemPoolFixed_Allocate(&this->findValuePool, sizeof(FindResult) * valuesFoundLimit, hash);
            continue;
        }
        return result;
    }
}

// Might return 0 and the value still might not be contained. But when it turns 1, the table definitely
//  does not contain the hash.




int AtomicHashTable2_insertInner2(
    AtomicHashTable2* this,
    uint64_t hash,
    void* value,
    AtomicHashTableBase* table
) {
    int reserveResult = atomicHashTable2_updateReserved(this, table, &table->newAllocation, 1, 1);
    if(reserveResult > 0) {
        return reserveResult;
    }
    
    uint64_t index = getSlotBaseIndex(table, hash);
    uint64_t startIndex = index;
    uint64_t loopCount = 0;
    while(true) {
        #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
        InterlockedIncrement64((LONG64*)&this->searchLoops);
        #endif
        OutsideReference* pValue = &table->slots[index].value;
        OutsideReference valueRef = *pValue;

        if(IS_VALUE_MOVED(valueRef)) {
            //printf("Found move while inserting for %llu\n", hash);
            atomicHashTable2_updateReservedUndo(this, table, -1, -1);
            return 1;
        }

        if(IS_BLOCK_END(valueRef) || valueRef.valueForSet == BASE_NULL.valueForSet) {
            OutsideReference valueOutsideRef = { 0 };

            HashValue* hashValue = nullptr;
            // We use index here instead of hash, that way the allocator doesn't have it iterate over the filled slots like we did, to save some time...
            Reference_Allocate((MemPool*)table->valuePool, &valueOutsideRef, &hashValue, this->HASH_VALUE_SIZE, index << (64 - table->logSlotsCount));
            if(!hashValue) {
                // We might not be out of memory, we might just have tried to allocate right after we finished a move.
                if(table->valuePool->countForSet.destructed) {
					return 1;
                }
                atomicHashTable2_updateReservedUndo(this, table, -1, -1);
                return 3;
            }
            hashValue->hash = hash;
            memcpy(
                (byte*)hashValue + sizeof(HashValue),
                value,
                this->VALUE_SIZE
            );

            // Is safe, because we are replacing a non-reference value (and NOT something move is dealing with either, so
            //  it is really safe).
            if(InterlockedCompareExchange64(
                (LONG64*)pValue,
                valueOutsideRef.valueForSet,
                valueRef.valueForSet
            ) != valueRef.valueForSet) {
                DestroyUniqueOutsideRef(&valueOutsideRef);
                continue;
            }
			//printf("Inserted %llu (base %llu) for %llu\n", index, startIndex, hash);


            if(!IS_BLOCK_END(valueRef)) {
                // We reused a null entry, so we can release the reserved amount, as we didn't use up any more null entries
                //  (but we still used up a real reserved count)
                atomicHashTable2_updateReservedUndo(this, table, 0, -1);
            }

            return 0;
        }

        index = (index + 1) % table->slotsCount;
        if(index == 0) {
            loopCount++;
            if(loopCount > 10) {
                atomicHashTable2_updateReservedUndo(this, table, -1, -1);
                // Too many loops around the table...
                OnError(9);
                return 9;
            }
        }
    }
}

int AtomicHashTable2_insertInner(
    AtomicHashTable2* this,
    uint64_t hash,
    void* value,
    AtomicHashTableBase* table,
    AtomicHashTableBase* newTable
) {
    // See atomicHashTable2_removeInner
    //printf("First try insert for %llu\n", hash);
    int result = AtomicHashTable2_insertInner2(this, hash, value, table);
    if(result > 1) {
        return result;
    }
    if(newTable) {
        int moveResult = atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);
        if(moveResult > 1) {
            if(result == 0) {
                // Unfortunately, we have to swallow the error here, as the operation succeeded, it was inserted,
                //  so we need to return 0 to inform the caller it was inserted...
                return 0;
            }
            return moveResult;
        }
        if(result == 1) {
            return AtomicHashTable2_insertInner2(this, hash, value, newTable);
        }
    }
    return result;
}

int AtomicHashTable2_insert(AtomicHashTable2* this, uint64_t hash, void* value) {
    #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
    InterlockedIncrement64((LONG64*)&this->searchStarts);
    #endif

    if(!this->currentAllocation.valueForSet) {
        // Trigger initial resize, so currentAllocation always has a value.
        atomicHashTable2_updateReserved(this, nullptr, &this->currentAllocation, 0, 0);
    }
    while(true) {
        InsideReference* tableRef = Reference_Acquire(&this->currentAllocation);
        AtomicHashTableBase* table = Reference_GetValue(tableRef);
        if(!table) {
            return 0;
        }

        InsideReference* newTableRef = Reference_Acquire(&table->newAllocation);
        AtomicHashTableBase* newTable = Reference_GetValue(newTableRef);

        int result = AtomicHashTable2_insertInner(this, hash, value, table, newTable);

        Reference_Release(&table->newAllocation, newTableRef);
        Reference_Release(&this->currentAllocation, tableRef);

        if(result != 1) {
            return result;
        }
        #ifdef DEBUG
        if(IsSingleThreadedTest) {
            // We shouldn't have to retry while single threaded
            OnError(3);
        }
        #endif
    }
}







uint64_t DebugAtomicHashTable2_reservedSize(AtomicHashTable2* this) {
    InsideReference* tableRef = Reference_Acquire(&this->currentAllocation);
    if(!tableRef) return 0;
    AtomicHashTableBase* table = Reference_GetValue(tableRef);
    uint64_t slotsReserved = table->slotsReserved;
    Reference_Release(&this->currentAllocation, tableRef);
    return slotsReserved;
}
uint64_t DebugAtomicHashTable2_allocationSize(AtomicHashTable2* this) {
    InsideReference* tableRef = Reference_Acquire(&this->currentAllocation);
    if(!tableRef) return 0;
    AtomicHashTableBase* table = Reference_GetValue(tableRef);
    uint64_t slotsCount = table->slotsCount;
    Reference_Release(&this->currentAllocation, tableRef);
    return slotsCount;
}