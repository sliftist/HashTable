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
#define SIZE_PER_VALUE_ALLOC(table) (table->VALUE_SIZE + InsideReferenceSize + MemPoolHashed_VALUE_OVERHEAD)
#define SIZE_PER_COUNT(table) (SIZE_PER_VALUE_ALLOC(table) + sizeof(AtomicSlot))

uint64_t minSlotCount(AtomicHashTable2* this) {
    uint64_t sizePerCount = SIZE_PER_COUNT(this);
    
    #ifdef DEBUG_INSIDE_REFERENCES
    #define TARGET_PAGES EVENT_ID_COUNT
    #else
    #define TARGET_PAGES 1
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
    if(!Reference_HasBeenMoved(ref)) {
        // And if it hasn't, it never will be, as the free callback is only called when all outside and inside references are gone...
        void* userValue = (byte*)ref + InsideReferenceSize;
        this->deleteValue(userValue);
    }
}

//todonext
// Wait... when we attempt to get a reference, we might temporarily turn a tombstone into having a count, which will make it look like it is moved?
//  Crap...

// Has to be a value copied from shared storage, or else this will race...
//  (which is why we don't put parentheses around value, as if using it raw fails to compile, then it probably isn't a locally copied variable...)


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
    if(this->destructed) {
        // Destructed, no more resizing is allowed
        return 10;
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

        if(slotsReservedNoNulls > curSlotCount / 10 * 9) {
            // We are within 90% of being filled. This is dangerously close, this shouldn't happen, we should move enough entries
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

    
    memset(newTable->slots, 0x00, newSlotCount * sizeof(uint64_t));
    if(pNewAlloc == &this->currentAllocation) {
        // The first allocation started moved into (because there are no values)
        newTable->finishedMovingInto = true;
        printf("Is first allocation\n");
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

    // This could hang and create a race. Worst case it doubles the maximum fill rate by going in the wrong direction, which would create an
    //  80% fill... which is fine, we can move into a smaller allocation, and then out in the time it would take to use that 20%
    
    if(InterlockedCompareExchange64(
        (LONG64*)pNewAlloc,
        newAllocation.valueForSet,
        0
    ) != 0) {
        //printf("failed to add %p\n", newTableRef);
        MemPoolHashed_Destruct(newTable->valuePool);
        DestroyUniqueOutsideRef(&newAllocation);
        return 1;
    }
    //printf("starting new move size %llu to %llu, using %llu, saw using %llu\n", curAlloc ? curAlloc->slotsCount : 0, newTable->slotsCount, curAlloc ? curAlloc->slotsReserved : 0, slotsReservedNoNulls);
    return 0;
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

    MoveStateInner moveState = { 0 };
    while(true) {
        loops++;
        if(loops > curAlloc->slotsCount * 10) {
            OnError(9);
            return 9;
        }

        // Move state is small enough that access are implicitly atomic (as it is 64 bits, and 64 bit aligned)
        moveState = *pMoveState;

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

        OutsideReference* pSource = &curAlloc->slots[moveState.sourceIndex].value;
        // Make sure curAlloc->slots[sourceIndex] is frozen, and marked as moved

        InsideReference* newRef = nullptr;
        int result = Reference_AcquireStartMove(pSource, (MemPool*)newAlloc->valuePool, this->VALUE_SIZE, &newRef);
        
        if(result > 0) {
            if(result == 3) {
                if(newAlloc->valuePool->countForSet.destructed) {
                    return 1;
                }            
            }
            return result;
        }

        if(newRef) {
            if(!MemPoolHashed_IsInPool(newAlloc->valuePool, newRef)) {
                // What? Did start move not work?
                breakpoint();
            }
        }

        // Rule for finding where to put it:
        // Either it is in newAlloc, sourceIndex has been updated, or it hasn't been inserted every yet?
        //  - Because once we insert it, to remove it applyMove has to be run, which updates sourceIndex
        // This means, that once we find a destIndex, if we try to insert it and find the dest slot is already being used,
        //  and not by our value, if we find sourceIndex is the same, we can atomically rollback destIndex.

        // Find a destination for the source, reserving it (now that the source is frozen, the hash won't change)
        if(newRef && moveState.destIndex == UINT32_MAX) {
            
            uint64_t hash = Reference_GetHash(newRef);

            uint64_t baseIndex = getSlotBaseIndex(newAlloc, hash);
            uint64_t index = baseIndex;
            bool couldNotFindSpot = false;
            while(true) {
                OutsideReference* pDest = &newAlloc->slots[index].value;
                OutsideReference dest = *pDest;
                if(FAST_CHECK_POINTER(dest, newRef)) {
                    DebugLog2("found at index", newRef, __FILE__, index);
                    moveState.destIndex = (uint32_t)index;
                    Reference_Release(&emptyReference, newRef);
                    newRef = nullptr;
                    break;
                }
                if(dest.valueForSet == 0) {
                    MoveStateInner newMoveState = moveState;
                    newMoveState.destIndex = (uint32_t)index;
                    if(InterlockedCompareExchange64(
                        (LONG64*)pMoveState,
                        newMoveState.valueForSet,
                        moveState.valueForSet
                    ) != moveState.valueForSet) {
						couldNotFindSpot = true;
						break;
                    }
					moveState.destIndex = (uint32_t)index;
                    break;
                }
                index = (index + 1) % newAlloc->slotsCount;
                if(index == baseIndex) {
                    couldNotFindSpot = true;
                    // We ran out of space while moving. This must be because everything has already finished moving, right?
                    
                    MoveStateInner moveState = *pMoveState;
                    if(moveState.sourceIndex < curAlloc->slotsCount) {
                        // Ran out of space... the 0s do get used up by new inserts, but those should help us
                        //  move a lot, and so we shouldn't be running our of 0s to insert into...

                        OnError(9);
                        return 9;
                    }
                    break;
                }
            }
            if(couldNotFindSpot) {
                Reference_Release(&emptyReference, newRef);
                continue;
            }
        }

        if(newRef) {
            // Now moveState.destIndex will have a value
            OutsideReference* pDest = &newAlloc->slots[moveState.destIndex].value;
            
            OutsideReference newSource = *pSource;
            // Already wiped out source, so sourceIndex must have been incremented too
            if(!newSource.pointerClipped) {
                Reference_Release(&emptyReference, newRef);
                continue;
            }

            OutsideReference newDest = Reference_CreateOutsideReference(newRef);

            InterlockedIncrement64(&newAlloc->slotsReserved);
            InterlockedIncrement64(&newAlloc->slotsReservedWithNulls);
            OutsideReference prevDest;
            prevDest.valueForSet = InterlockedCompareExchange64(
                (LONG64*)pDest,
                newDest.valueForSet,
                0
            );
            // If it isn't newRef, either someone took or spot, or we inserted, increased sourceIndex, and removed the original source
            //  (in which case the rollback code will just fail and we will continue).
            if(prevDest.valueForSet != 0) {
                if(!Reference_DestroyOutside(&newDest, newRef)) {
                    OnError(3);
                }
                IsInsideRefCorrupt(newRef);
                if(FAST_CHECK_POINTER(prevDest, newRef)) {
                    // Just retry, so we can catch it in the destIndex search loop
                    Reference_Release(&emptyReference, newRef);
                    continue;
                }
                else {

                    // The value must have been used up by an insert

                    InterlockedDecrement64(&newAlloc->slotsReserved);
                    InterlockedDecrement64(&newAlloc->slotsReservedWithNulls);

                    // Rollback destIndex, and try again to find a new one (if this hangs, it is fine. Worst case we rollback
                    //  when destIndex has already been set and inserted... which is fine, because the next loop will see that
                    //  it has been inserted (and it can't get deleted, as before deletions happen in newAlloc atomicHashTable2_applyMoveTableOperationInner
                    //  has to be called, which will run this code again, making the move finalize first).
                    // AND when operation see we are moving, they help with the move, so nothing will ever be hung up in a move.

                    MoveStateInner curMoveState = *pMoveState;
                    if(curMoveState.sourceIndex == moveState.sourceIndex) {
                        MoveStateInner newMoveState = moveState;
                        newMoveState.destIndex = UINT32_MAX;
                        InterlockedCompareExchange64(
                            (LONG64*)pMoveState,
                            newMoveState.valueForSet,
                            moveState.valueForSet
                        );
                    }

                    Reference_Release(&emptyReference, newRef);

                    continue;
                }
            }
            Reference_Release(&emptyReference, newRef);
        }


        MoveStateInner newMoveState = moveState;
        newMoveState.sourceIndex++;
        newMoveState.destIndex = UINT32_MAX;
        if(InterlockedCompareExchange64(
            (LONG64*)pMoveState,
            newMoveState.valueForSet,
            moveState.valueForSet
        ) == moveState.valueForSet) {
            if(Reference_FinishMove(pSource)) {   
                InterlockedDecrement64((LONG64*)&curAlloc->slotsReserved);
            }
            countApplied++;
            if(countApplied > minApplyCount) {
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
    AtomicHashTableBase* table,
    AtomicHashTableBase* newTable
) {
    uint64_t index = getSlotBaseIndex(table, hash);
    uint64_t loopCount = 0;
    while(true) {
        #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
        InterlockedIncrement64((LONG64*)&this->searchLoops);
        #endif
        OutsideReference* pValue = &table->slots[index].value;
        OutsideReference valueRef = *pValue;

        if (IS_BLOCK_END(valueRef)) {
			return 0;
		}

        bool moved = false;
        bool isFrozen = false;
        InsideReference* ref = Reference_AcquireCheckIsMoved(pValue, &moved, &isFrozen);
        if(!ref) {
            if(moved) {
                if(!newTable) {
                    // We thought we were moving to newTable, and now newTable is moving? We must be way behind
                    return 1;
                }
                if(isFrozen) {
                    // Make sure the value is moved, and then we will read it in the newTable when we check that.
                    RETURN_ON_ERROR(atomicHashTable2_applyMoveTableOperationInner(this, table, newTable));
                }
            }
        } else {
            uint64_t refHash = Reference_GetHash(ref);
            if(refHash == hash) {
                void* valueVoid = Reference_GetValue(ref);
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
    if(newTable) {
        RETURN_ON_ERROR(atomicHashTable2_applyMoveTableOperationInner(this, table, newTable));
    }
    int result = atomicHashTable2_removeInner2(this, hash, callbackContext, callback, table, newTable);
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
        // The previous removeInner only returns 1 if !newTable... so... we will never be clobbering result == 1 here.
        result = atomicHashTable2_removeInner2(this, hash, callbackContext, callback, newTable, nullptr);
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
    void* value;
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
        OutsideReference valueRef = *pValue;

        if(IS_BLOCK_END(valueRef)) {
            //printf("Finished found %llu proof from %llu to %llu of %llu\n", *pValuesFoundCur, baseIndex, index, hash);
            return 0;
        }

        bool moved = false;
        bool isFrozen = false;
        InsideReference* ref = Reference_AcquireCheckIsMoved(pValue, &moved, &isFrozen);
        if(!ref) {
            if(moved) {
                if(!newTable) {
                    // We thought we were moving to newTable, and now newTable is moving? We must be way behind
                    return 1;
                }
                // Make sure the value is moved, and then we will read it in the newTable when we check that.
                RETURN_ON_ERROR(atomicHashTable2_applyMoveTableOperationInner(this, table, newTable));
            }
        } else {
            uint64_t refHash = Reference_GetHash(ref);
            if (refHash != hash) {
                Reference_Release(pValue, ref);
            }
            else {
                FindResult result;
                result.refSource = pValue;
                result.ref = ref;
                result.value = Reference_GetValueFast(ref);

                uint64_t valuesFoundCur = *pValuesFoundCur;
                uint64_t valuesFoundLimit = *pValuesFoundLimit;
                valuesFoundCur++;
                if (valuesFoundCur > valuesFoundLimit) {
                    Reference_Release(pValue, ref);
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
    // And also, change atomicHashTable2_applyMoveTableOperationInner to happen after we run for find/remove, and swallow
    //  the error from it... giving us a chance to keep working when we run out of memory (also... maybe set an out of memory
    //  flag which causes us to not call apply in find?)

    if(newTable) {
        RETURN_ON_ERROR(atomicHashTable2_applyMoveTableOperationInner(this, table, newTable));
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
                if (Reference_HasBeenMoved(valuesFound[i].ref)) {
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
            callback(callbackContext, valuesFound[i].value);
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
        atomicHashTable2_updateReservedUndo(this, table, -1, -1);
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

        if(valueRef.valueForSet == 0 || valueRef.valueForSet == BASE_NULL.valueForSet) {
            OutsideReference valueOutsideRef = { 0 };

            void* newValue;
            // TODO: Pass 2 values where we pass hash, 1 which is based on index, so allocate doesn't have to do the same iteration we just did to
            //  find an open spot (most of the time, although if slots and our mempool get out of sync it will have to).
            Reference_Allocate((MemPool*)table->valuePool, &valueOutsideRef, &newValue, this->VALUE_SIZE, hash);
            if(!newValue) {
                // We might not be out of memory, we might just have tried to allocate right after we finished a move.
                if(table->valuePool->countForSet.destructed) {
					return 1;
                }
                atomicHashTable2_updateReservedUndo(this, table, -1, -1);
                breakpoint();
                return 3;
            }
            memcpy(
                (byte*)newValue,
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


            if(valueRef.valueForSet == BASE_NULL.valueForSet) {
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
    if(newTable) {
        RETURN_ON_ERROR(atomicHashTable2_applyMoveTableOperationInner(this, table, newTable));
    }

    // The simplest, obviously always try to insert in newTable. Why would we try to insert in an old table?

    if(newTable) {
        return AtomicHashTable2_insertInner2(this, hash, value, newTable);
    } else {
        return AtomicHashTable2_insertInner2(this, hash, value, table);
    }
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
            if(this->destructed && result == 0) {
                AtomicHashTable2_dtor(this);
            }
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

void AtomicHashTable2_dtor(
    AtomicHashTable2* this
) {
    this->destructed = 1;
    while(true) {
        InsideReference* tableRef = Reference_Acquire(&this->currentAllocation);
        AtomicHashTableBase* table = Reference_GetValue(tableRef);
        
        {
            InsideReference* newTableRef = Reference_Acquire(&table->newAllocation);
            AtomicHashTableBase* newTable = Reference_GetValue(newTableRef);
            if(newTable) {
                atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);

                Reference_Release(&table->newAllocation, newTableRef);
                Reference_Release(&this->currentAllocation, tableRef);
                continue;
            }
        }

        Reference_DestroyOutsideMakeNull1(&this->currentAllocation, tableRef);
        MemPoolHashed_Destruct(table->valuePool);

        // Make the whole table BASE_NULL1. Because we set destructed no more new tables can be created,
        //  so if the whole table is BASE_NULL1 (which nothing can insert on, as that is the signal
        //  to look in newTable), then nothing can be inserted, and so once we empty the table,
        //  eventually all table references will be released, and the table will free.

        for(uint64_t i = 0; i < table->slotsCount; i++) {
            OutsideReference* pValue = &table->slots[i].value;
            OutsideReference value = *pValue;
            InsideReference* valueRef = Reference_Acquire(&value);
            if(valueRef) {
                if(!Reference_DestroyOutsideMakeNull1(pValue, valueRef)) {
                    Reference_Release(pValue, valueRef);
                    continue;
                }
            } else {
                if(InterlockedCompareExchange64(
                    (LONG64*)pValue,
                    BASE_NULL1.valueForSet,
                    value.valueForSet
                ) != value.valueForSet) {
                    Reference_Release(pValue, valueRef);
                    continue;
                }
            }
            Reference_Release(pValue, valueRef);
        }

        Reference_Release(&this->currentAllocation, tableRef);
        break;
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