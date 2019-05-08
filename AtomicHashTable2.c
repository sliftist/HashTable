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
    
    uint64_t minCount = (PAGE_SIZE - sizeof(AtomicHashTableBase) - InsideReferenceSize - sizeof(MemPoolHashed)) / sizePerCount;

    // Must be a power of 2, for getSlotBaseIndex
    minCount = 1ll << log2RoundDown(minCount);

    return minCount;
}
uint64_t getTableSize(AtomicHashTable2* this, uint64_t slotCount) {
    uint64_t tableSize = SIZE_PER_COUNT(this) * slotCount + sizeof(AtomicHashTable2);
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

// Has to be a value copied from shared storage, or else this will race...
//  (which is why we don't put parentheses around value, as if using it raw fails to compile, then it probably isn't a locally copied variable...)
#define IS_VALUE_MOVED(value) (value.isNull && value.valueForSet != BASE_NULL.valueForSet)
#define IS_BLOCK_END(value) (value.valueForSet == 0 || value.valueForSet == BASE_NULL_MAX.valueForSet)


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
    newSlotCount = newShrinkSize(this, curSlotCount, slotsReservedNoNulls);
    newSlotCount = newSlotCount ? newSlotCount : newGrowSize(this, curSlotCount, slotsReservedNulls);
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
        // We shouldn't be trying to grow though when moving... We should be able to move the whole table in well
        //  under the number of operations it takes to doulbe our size again. This likely means our fixed
        //  move count is too low, or our grow/shrink ranges overlap, or are too close, or something.
        OnError(9);
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
    newTable->moveState.nextSourceSlot = UINT32_MAX;
    newTable->moveState.sourceBlockStart = UINT32_MAX;
    newTable->moveState.sourceValue.valueForSet = 0;
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

    InsideReference* newTableRef = Reference_Acquire(&newAllocation);
    OutsideReference memPoolTableRef = { 0 };
    Reference_SetOutside(&memPoolTableRef, newTableRef);
    Reference_Release(&newAllocation, newTableRef);

    MemPoolHashed pool = MemPoolHashedDefault(SIZE_PER_VALUE_ALLOC(this), newSlotCount, logSlotsCount, memPoolTableRef, this, atomicHashTable2_memPoolFreeCallback);
    *newTable->valuePool = pool;
    MemPoolHashed_Initialize(newTable->valuePool);

    if(InterlockedCompareExchange64(
        (LONG64*)pNewAlloc,
        newAllocation.valueForSet,
        0
    ) != 0) {
        return 1;
    }
    printf("starting new move size %llu to %llu\n", curAlloc ? curAlloc->slotsCount : 0, newTable->slotsCount);
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



// Returns 0 if moves were applied,
// Returns > 1 on error
int atomicHashTable2_applyMoveTableOperationInner(
    AtomicHashTable2* this,
    AtomicHashTableBase* curAlloc,
    AtomicHashTableBase* newAlloc
) {
    //todonext
    // Alright... how could moving, skip over a value that definitely exists? Because that is what happening, a value not deleted,
    //  doesn't get moved, it just completely gets skipped over. Maybe... print all cases we skip over, and why?

    // Hash block checking doesn't work because of wrap around (at least, it can't be efficient because of wrap around).
    //  So instead we just move A block, and if we ran into a moved element in a block, then either
    //  that block was already moved, or we just moved it... so this works...

	// This has to be a certain size above our grow/shrink thresholds or else we will try to grow before we finish moving!
    uint64_t minApplyCount = 64;

    uint64_t HASH_VALUE_SIZE = this->HASH_VALUE_SIZE;
    uint64_t VALUE_SIZE = this->VALUE_SIZE;

    uint64_t countApplied = 0;

    MoveStateInner* pMoveState = &curAlloc->moveState;

    uint64_t loops = 0;
    while(true) {
        loops++;
        if(loops > curAlloc->slotsCount * 10) {
            OnError(9);
            return 9;
        }

        // Make sure we start off at the start of a block (so a 0).
        if(pMoveState->nextSourceSlot == UINT32_MAX) {
            bool shrinking = curAlloc->slotsCount < newAlloc->slotsCount;
            for(uint32_t index = 0; index < curAlloc->slotsCount; index++) {
                if(shrinking && index % 2 == 1) {
                    // If we start moving on an odd entry, it means the dest index will be in a transition state (all of the source values
                    //  that could fill it haven't been moved to it, but some might have), which will make our access algorithms
                    //  way too complicated, as right now they rely on a location either being moved into (so it isn't in the source),
                    //  or not moved into (so it is in the source).
                    // So, we just skip odd entries when shrinking.
                    continue;
                }
                if(curAlloc->slots[index].value.valueForSet == 0) {
                    if(InterlockedCompareExchange64(
                        (LONG64*)&curAlloc->slots[index].value,
                        BASE_NULL2.valueForSet,
                        0
                    ) != 0) {
                        break;
                    }
                    // If this fails, leaving BASE_NULL2s around is fine... we have to handle them anyway, and so leaving them around
                    //  just lets us test that code more often.
                    InterlockedCompareExchange(
                        (LONG32*)&pMoveState->nextSourceSlot,
                        index,
                        UINT32_MAX
                    );
                    break;
                }
            }
            if(pMoveState->nextSourceSlot == UINT32_MAX) {
                continue;
            }
        }

        // Make sure pMoveState->sourceValue is populated
        {
            // Move the value from the source into the move state, atomically, making it look like null in the source,
            //  and not like null in the destination.

            // We map to BASE_NULL1, and not 0, that way 0 can stay as a sentinel
            if(pMoveState->sourceValue.valueForSet == 0) {
                uint32_t nextSourceSlot = pMoveState->nextSourceSlot;
                OutsideReference* pSource = &curAlloc->slots[nextSourceSlot % curAlloc->slotsCount].value;
                OutsideReference sourceValue = *pSource;
                if (sourceValue.valueForSet == 0) {
                    if (InterlockedCompareExchange64(
                        (LONG64*)pSource,
                        (LONG64)BASE_NULL2.valueForSet,
                        0
                    ) != 0) {
                        continue;
                    }
                    sourceValue = BASE_NULL2;
                }
                else if(sourceValue.valueForSet == BASE_NULL.valueForSet) {
                    if(InterlockedCompareExchange64(
                        (LONG64*)pSource,
                        (LONG64)BASE_NULL1.valueForSet,
                        (LONG64)BASE_NULL.valueForSet
                    ) != (LONG64)BASE_NULL.valueForSet) {
                        continue;
                    }
                    sourceValue = BASE_NULL1;
                } else if(sourceValue.valueForSet == BASE_NULL1.valueForSet || sourceValue.valueForSet == BASE_NULL2.valueForSet) {
                    // Already set, nothing to do
                }
                else {
                    if(!sourceValue.isNull) {
                        InsideReference* sourceRef = Reference_Acquire(pSource);
                        // If it does acquire a value, it must been the source isn't null, so we can use whatever value, even if it doesn't
                        //  equal sourceValue, because we wipe out sourceValue anyway...
                        if(!sourceRef) {
                            continue;
                        }

                        OutsideReference setSourceValue = { 0 };
                        setSourceValue.isNull = 1;
                        setSourceValue.pointerClipped = (uint64_t)sourceRef;

                        // This... freezes pSource, no one will ever change it after this. AND, it reduces the outside ref count to 0
                        //  first, so we don't break any outstanding references. HOWEVER! It doesn't break the pointer, and it doesn't
                        //  technically destroy the outside reference (as in, the outside reference's reference to the inner reference
                        //  is kept alive). So... it means even if this thread fails, the next thread can pick up after it, trivially.
                        
                        if(!Reference_ReplaceOutsideStealOutside(
                            pSource,
                            sourceRef,
                            setSourceValue
                        )) {
                            Reference_Release(pSource, sourceRef);
                            continue;
                        }
                        // Replace will move outside references to the inside, so we free from the empty reference.
                        Reference_Release(&emptyReference, sourceRef);

                        sourceValue = setSourceValue;
                    } // else, it is a null we set it to, so we can just pick up where we left off
                    sourceValue.isNull = 0;
                }

                // So at this point the source value is immutable. No user will replace an isNull value
                //  (except for BASE_NULL1, but setting isNull on a valid value will never create that, as that value
                //  would have to have a pointer value of 1, which I am assuming can't happen...)
                // So... that is very very nice

                MoveStateInner prevMoveState;
                prevMoveState.nextSourceSlot = nextSourceSlot;
                prevMoveState.sourceValue.valueForSet = 0;
                prevMoveState.sourceBlockStart = pMoveState->sourceBlockStart;

                MoveStateInner newMoveState;
                newMoveState.nextSourceSlot = nextSourceSlot;
                newMoveState.sourceValue = sourceValue;
                newMoveState.sourceBlockStart = prevMoveState.sourceBlockStart;
                
                if(!InterlockedCompareExchangeStruct128(pMoveState, &prevMoveState, &newMoveState)) {
                    continue;
                }
                
                if(sourceValue.pointerClipped > 10) {
                    printf("Populated value from source index %llu\n", nextSourceSlot % curAlloc->slotsCount);
                } else {
                    printf("Populated empty value from source index %llu\n", nextSourceSlot % curAlloc->slotsCount);
                }
            }
        }

        #ifdef DEBUG
        if(IsSingleThreadedTest) {
            if(pMoveState->sourceValue.valueForSet == 0) {
                // Can never be 0, otherwise we can't tell if we read a null from source, or didn't read anything yet.
                OnError(3);
            }
        }
        #endif

        // Get a value, and source index (atomically) so when we are done we can advance the source index
        InsideReference* valueRef;
        uint32_t sourceIndex;
        uint32_t sourceBlockStart;
        bool isBlockEnd = false;
        {
            MoveStateInner moveState = *pMoveState;
            if(!EqualsStruct128(&moveState, pMoveState)) {
                continue;
            }
            if(moveState.sourceValue.valueForSet == 0) {
                continue;
            }
            valueRef = Reference_Acquire(&pMoveState->sourceValue);
            if(!FAST_CHECK_POINTER(moveState.sourceValue, valueRef)) {
                Reference_Release(&pMoveState->sourceValue, valueRef);
                continue;
            }
            if(moveState.sourceValue.valueForSet == 0) {
                continue;
            }
            sourceIndex = moveState.nextSourceSlot;
            sourceBlockStart = moveState.sourceBlockStart;
            if(moveState.sourceValue.valueForSet == BASE_NULL2.valueForSet) {
                isBlockEnd = true;
            }
        }

        // Now make sure valueRef is in the newAlloc pool (so redirected)
        if(valueRef && !MemPoolHashed_IsInPool(newAlloc->valuePool, valueRef)) {
            HashValue* value = Reference_GetValue(valueRef);
            OutsideReference newValueRef = { 0 };
            HashValue* newValue = nullptr;
            Reference_Allocate((MemPool*)newAlloc->valuePool, &newValueRef, &newValue, this->HASH_VALUE_SIZE, value->hash);
            if(!newValue) {
                Reference_Release(&pMoveState->sourceValue, valueRef);
                return 3;
            }

            newValue->hash = value->hash;
            memcpy(
                (byte*)newValue + sizeof(HashValue),
                (byte*)value + sizeof(HashValue),
                VALUE_SIZE
            );

            InsideReference* newValueRefInside = Reference_Acquire(&newValueRef);
            Reference_RedirectReference(
                valueRef,
                newValueRefInside
            );

            Reference_DestroyOutside(&newValueRef, newValueRefInside);
            Reference_Release(&emptyReference, newValueRefInside);


            // Release and reacquire it to make sure we get the most up to date value
            Reference_Release(&pMoveState->sourceValue, valueRef);
            valueRef = Reference_Acquire(&pMoveState->sourceValue);
            if(!MemPoolHashed_IsInPool(newAlloc->valuePool, valueRef)) {
                Reference_Release(&pMoveState->sourceValue, valueRef);
                continue;
            }
        }

        #ifdef DEBUG
        if(IsSingleThreadedTest) {
            if(pMoveState->sourceValue.valueForSet == 0) {
                // Can never be 0, otherwise we can't tell if we read a null from source, or didn't read anything yet.
                OnError(3);
            }
        }
        #endif

        // Now make sure valueRef has been put in the destination
        //  (this is slightly dangerous, but because of our delete calls us an because it sees one entry after the value it deletes,
        //  this should only put a value in the destination multiple times in the case of a delete, and then only once per delete thread,
        //  and only before the delete call exits).
        if(valueRef) {
            HashValue* value = Reference_GetValue(valueRef);
            uint64_t sourceBaseIndex = getSlotBaseIndex(curAlloc, value->hash);

            // TODO: We really can't reuse BASE_NULL spots when moving, as this would create a hanging insert, but not in the original
            //  insert function, which allow values to spontaneously reappear. This means that if deletes happen when moving we
            //  could keep half moving, having the value deleted, and then inserting to another place, until we use up the whole
            //  table. However... this should be unlikely, as each delete should see a move is happening, and then start helping,
            //  which means this should only be possible if there are a lot of threads (equal to the number of free slots?).
            
            bool alreadyInserted = false;
			uint64_t baseDestIndex = getSlotBaseIndex(newAlloc, value->hash);
            uint64_t curDestIndex = baseDestIndex;
            uint64_t loops = 0;
            while(true) {
                OutsideReference value = newAlloc->slots[curDestIndex].value;
                if(FAST_CHECK_POINTER(value, valueRef)) {
                    printf("double insert proof, value is already at %llu\n", curDestIndex);
                    alreadyInserted = true;
                    break;
                }
                if(value.valueForSet == 0) {
                    //todonext;
                    // Yeah... how would this work? If we fill every dest slot except 1, and then find that the corresponding source slot
                    //  to that has 2 values... Well, that clearly wouldn't work...
                    // But, the whole thing we are doing is to only insert into BASE_NULL_MAX slots, AND, we are also trying to clean up BASE_NULL_MAX
                    //  slots while moving, for two reasons
                    //  1) To prevent iterating over all the values when we finish
                    //  2) To allow adding 0s before we finish moving everything
                    //      - But maybe... maybe just letting BASE_NULL_MAX not count as moved, and let access algorithms treat it as 0... maybe that would work?
                    //  Hmm... is there any way to recover shrinking to not force a full iteration to replace all BASE_NULL_MAX values when we finish? But also
                    //      to not run out of spaces?

                    // So... when we insert, we don't know if a block before us will start to spill into us...
                    //  Hmm... it would take... a lot of searching to make sure.

                    // BUT, we do need to do something so when searching we can know the dest block is ended...

                    //  Hmm... maybe, when we would end the block, we see if we can set a special null at the end of the dest block. We might not
                    //      be able to, if we shrunk and increased in relative size, but we should be able to usually (and if we can't we can
                    //      just apply more moves, and it will be fine), which access functions can use to 

                    // Oh... 

                    printf("double insert proof, dest %llu is 0, base %llu\n", curDestIndex, baseDestIndex);
                    // This means that a thread finished the move, and is currently going through the block changing BASE_NULL_MAX to 0.
                    alreadyInserted = true;
                    break;
                }
                
                if(value.valueForSet == BASE_NULL_MAX.valueForSet) {
                    break;
                }
                // value.valueForSet == 0 means we moved to here previous, but that value got deleted, we can't reuse the slot...
                curDestIndex = (curDestIndex + 1) % newAlloc->slotsCount;
                if(curDestIndex == baseDestIndex) {
                    printf("double insert proof, looped around\n");
                    // The entire move must have finished already, which is why we can't find any free place to put it.
                    alreadyInserted = true;
                    break;
                }
            }
            if(alreadyInserted) {
                // But... just because it has been inserted, doesn't mean the insert finished, so still try to finish
                //  the insert by changing moveState.
                printf("Ignoring double insert %llu to %llu for %llu\n", sourceIndex % curAlloc->slotsCount, curDestIndex, value->hash);
            } else {
                OutsideReference destOutsideRef = { 0 };
                Reference_SetOutside(&destOutsideRef, valueRef);
                

                //todonext
                // Hmm... there is racing to move different values to the same slot. This should be impossible, we test pMoveState before we decide
                //  upon the curDestIndex...

                printf("Thinking of moving %llu to %llu for %llu\n", sourceIndex % curAlloc->slotsCount, curDestIndex, value->hash);

                InterlockedIncrement64((LONG64*)&newAlloc->slotsReserved);
                InterlockedIncrement64((LONG64*)&newAlloc->slotsReservedWithNulls);
                if(InterlockedCompareExchange64(
                    (LONG64*)&newAlloc->slots[curDestIndex].value,
                    destOutsideRef.valueForSet,
                    BASE_NULL_MAX.valueForSet
                ) != BASE_NULL_MAX.valueForSet) {
                    InterlockedDecrement64((LONG64*)&newAlloc->slotsReserved);
                    InterlockedDecrement64((LONG64*)&newAlloc->slotsReservedWithNulls);
                    DestroyUniqueOutsideRef(&destOutsideRef);
                    continue;
                }
                printf("Moved %llu to %llu for %llu\n", sourceIndex % curAlloc->slotsCount, curDestIndex, value->hash);
            }
		} else {
            printf("Nothing to move from %llu\n", sourceIndex % curAlloc->slotsCount);
        }

        // The destination has definitely been populated here

        // Wipe out any BASE_NULL_MAXs with 0 in our block, and set nextSourceBlockStart
        uint32_t nextSourceBlockStart = sourceBlockStart;
        if(isBlockEnd) {
            // If shrinking it is impossible to identify dest slots as being accounted for, as shrinking could cause massive cascading overlap.
            //  Therefore, when shrinking, we have to leave nulls around as space to handle cascading overlap.
            //  However when growing or staying the same size we don't need to, so we replace nulls early, that way we don't
            //      need a final iteration to get rid of the remaining nulls...
            /*
            if(!(curAlloc->slotsCount < newAlloc->slotsCount)) {
                atomicHashTable2_replaceNullsInBlock(sourceBlockStart, sourceIndex, curAlloc, newAlloc);
            }
            */

            nextSourceBlockStart = UINT32_MAX;
        } else {
            if(nextSourceBlockStart == UINT32_MAX) {
                nextSourceBlockStart = sourceIndex % curAlloc->slotsCount;
            }
        }

        // Check if this is the last move
        {
            OutsideReference nextSourceValue = curAlloc->slots[(sourceIndex + 1) % curAlloc->slotsCount].value;
            if(IS_VALUE_MOVED(nextSourceValue)) {
                Reference_Release(&pMoveState->sourceValue, valueRef);

                MoveStateInner prevMoveState = *pMoveState;
                if(!EqualsStruct128(pMoveState, &prevMoveState)) {
                    continue;
                }
                // If we aren't the current move this just means we are behind (but if we are the current move, this means we wrapped around)
				if(pMoveState->nextSourceSlot != sourceIndex) {
                    continue;
                }
                if(prevMoveState.sourceValue.valueForSet == 0) {
                    // Impossible, sourceValue MUST be left non-0 when we finish moving
                    OnError(9);
                }


                printf("finished move\n");
                if(!prevMoveState.sourceValue.isNull) {
                    MoveStateInner newMoveState = prevMoveState;
                    newMoveState.sourceValue = BASE_NULL;
                    if(InterlockedCompareExchangeStruct128(pMoveState, &prevMoveState, &newMoveState)) {
                        DestroyUniqueOutsideRef(&prevMoveState.sourceValue);
                    }
                }


                /*
                if(!(curAlloc->slotsCount < newAlloc->slotsCount)) {
                    // End the current block, because no one else will...
                    atomicHashTable2_replaceNullsInBlock(sourceBlockStart, sourceIndex, curAlloc, newAlloc);
                } else {
                    atomicHashTable2_replaceNullsInBlock(0, curAlloc->slotsCount - 1, curAlloc, newAlloc);
                }
                */

                //todonext
                // Wait... if we don't replace nulls while iterating, won't that leave us open to hanging inserts? Ugh... maybe we need to
                //  just map nulls to new values, so if we wrap around we can still reuse them. Ugh... Okay, so... we could use nulls to store a value,
                //  and then... when inserting only insert if that value is < the current value?
                //  - Okay, so... values could be related to the sourceBlockStart?
                // BUT WAIT! We can't have there isNulls overlap with real pointers, or else a hanging cleanup can break the final move.
                todonext
                // Okay well... a hanging insert has to start with a search...
                // Uh... the place that does the insert, will have to...
                // So, a hanging insert will have to iterate over values...

                // Oh, maybe when inserting we need to set another bit (in addition to isNull), to say the values are still moving? Fuck...
                //  we might need more state, and then proper transactions here...

                todonext
                // Okay... we can reserve a small amount of nulls to be used... fuck, idk, to prepare items? And then hanging threads
                //  leak those, but only so many threads can hang at once, so it is fine...

                fuck
                // Okay, well... maybe we don't do moving by parts, maybe we just move everything, every time, except for finds,
                //  which never do moving.


                //todonext
                // Wait... we can totally break this iteration up into groups. So... we should do that...
                atomicHashTable2_replaceNullsInBlock(0, curAlloc->slotsCount - 1, curAlloc, newAlloc);


                /*
                #ifdef DEBUG
				for (uint64_t i = 0; i < curAlloc->slotsCount; i++) {
					OutsideReference value = curAlloc->slots[i].value;
					if (!IS_VALUE_MOVED(value)) {
						// We shouldn't finish the move until everything has been moved...
						breakpoint();
					}
				}
                // We shouldn't have any BASE_NULL_MAXs, as they should be removed during moves (and if they aren't, it will mean that
                //	calls us might cause an operation to have to keep looping until all moves are done, which shouldn't be required,
                //	as that means the best worse cast scenario for operations is O(N))
                for(uint64_t i = 0; i < newAlloc->slotsCount; i++) {
                    OutsideReference value = newAlloc->slots[i].value;
                    if(value.valueForSet == BASE_NULL_MAX.valueForSet) {
                        breakpoint();
                    }
                }
                #endif
                */

                // Hmm... this really puts a lot on Reference_RedirectReference, but... it should be fine... as long as the caller
                //  makes sure newAlloc is inside of curAlloc, then curAlloc can only redirect once,
                //  AND this actually makes it so this->currentAllocation gets automatically updated when Reference_Acquire is called.
                
                InsideReference* newAllocRef = (void*)((byte*)newAlloc - InsideReferenceSize);

                newAlloc->finishedMovingInto = true;

                MemPoolHashed_Destruct(curAlloc->valuePool);

                Reference_RedirectReference((void*)((byte*)curAlloc - InsideReferenceSize), newAllocRef);
                Reference_DestroyOutsideMakeNull(&curAlloc->newAllocation, newAllocRef);

                return 0;
            }
        }

        // Update nextSourceSlot and destOffset
        {
            while(true) {
                if(valueRef) {
                    // Must make the outside ref have 0 references, or else we will leave outside references in limbo when we swap,
                    //  possibly resulting in a premature free.
                    if (!Reference_ReduceToZeroOutsideRefs(&pMoveState->sourceValue)) {
                        break;
                    }
                }
                MoveStateInner prevMoveState = *pMoveState;
                if(!EqualsStruct128(pMoveState, &prevMoveState)) {
                    continue;
                }
                if(prevMoveState.sourceValue.count != 0) {
                    break;
                }
                if(prevMoveState.nextSourceSlot != sourceIndex) {
                    break;
                }
                if(prevMoveState.sourceBlockStart != sourceBlockStart) {
                    break;
                }
                if(!FAST_CHECK_POINTER(prevMoveState.sourceValue, valueRef)) {
                    break;
                }

                MoveStateInner newMoveState;
                newMoveState.nextSourceSlot = sourceIndex + 1;
                newMoveState.sourceBlockStart = nextSourceBlockStart;
                newMoveState.sourceValue.valueForSet = 0;

                if(InterlockedCompareExchangeStruct128(
                    pMoveState,
                    &prevMoveState,
                    &newMoveState
                )) {
                    countApplied++;
                    if(valueRef) {
                        DestroyUniqueOutsideRef(&prevMoveState.sourceValue);
                    }
                }
                break;
            }
            if(valueRef) {
                Reference_Release(&pMoveState->sourceValue, valueRef);
            }
        }

        // !valueRef means we finished a block, which is important
        if(countApplied >= minApplyCount && !valueRef) {
            // Done...
            return 0;
        }
    }
}



int atomicHashTable2_removeInner2(
    AtomicHashTable2* this,
    uint64_t hash,
	void* callbackContext,
	bool(*callback)(void* callbackContext, void* value),
    AtomicHashTableBase* table,
    uint64_t* pIndex
) {
    //uint64_t index = *pIndex;
    uint64_t index = getSlotBaseIndex(table, hash);
    uint64_t loopCount = 0;
    while(true) {
        #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
        InterlockedIncrement64((LONG64*)&this->searchLoops);
        #endif
        OutsideReference* pValue = &table->slots[index].value;
        OutsideReference valueRef = *pValue;

        if(IS_VALUE_MOVED(valueRef)) {
            *pIndex = index;
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
                        printf("Removed %llu for %llu\n", index, hash);
                    }
                }
                Reference_Release(pValue, ref);
            }
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
    uint64_t index = getSlotBaseIndex(table, hash);
    int result = atomicHashTable2_removeInner2(this, hash, callbackContext, callback, table, &index);
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
            if(result == 0) {
                // Unfortunately, we have to swallow the error here, as the operation succeeded, so we are forced to return 0
                return 0;
            }
            return moveResult;
        }
        if(result == 1) {
            if(newTable->slotsCount > table->slotsCount) {
                index = index * 2;
            } else if(newTable->slotsCount < table->slotsCount) {
                // Hmm... I am assuming rounding will be fine here...
                index = index / 2;
            }
            return atomicHashTable2_removeInner2(this, hash, callbackContext, callback, newTable, &index);
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
    uint64_t* pIndex,
    uint64_t baseIndex,
    uint64_t hash
) {
    //uint64_t index = *pIndex;
    uint64_t index = getSlotBaseIndex(table, hash);
    uint64_t startIndex = index;
    uint64_t loopCount = 0;
    while(true) {
        #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
        InterlockedIncrement64((LONG64*)&this->searchLoops);
        #endif
        OutsideReference* pValue = &table->slots[index].value;
        OutsideReference value = *pValue;
        if(IS_VALUE_MOVED(value)) {
            printf("moved retry\n");
            *pIndex = index;
            return 1;
        }

        if(IS_BLOCK_END(value)) {
            printf("Finished found %llu proof from %llu to %llu of %llu\n", *pValuesFoundCur, startIndex, index, hash);
            *pIndex = index;
            return 0;
        }
        if(value.valueForSet != BASE_NULL.valueForSet) {
            InsideReference* valueRef = Reference_Acquire(pValue);
            if (!valueRef) {
                // Might be because of move, but is more likely just because the value was set to the tombstone value
                //  (BASE_NULL), so we just just try rereading it
                continue;
            }
            HashValue* value = Reference_GetValue(valueRef);
            if (value->hash != hash) {
                Reference_Release(pValue, valueRef);
            }
            else {
                FindResult result;
                result.refSource = pValue;
                result.ref = valueRef;
                result.value = value;

                uint64_t valuesFoundCur = *pValuesFoundCur;
                uint64_t valuesFoundLimit = *pValuesFoundLimit;
                valuesFoundCur++;
                if (valuesFoundCur > valuesFoundLimit) {
                    Reference_Release(pValue, valueRef);
                    // Eh... *10, because we don't know how many entries there might be. We could loop and count how many values
                    //  there might be, and allocate less... but I don't think we need to optimize the case of many results right now...
                    *pValuesFoundLimit = valuesFoundLimit * 10 + 1;
                    printf("found count retry\n");
                    return -1;
                }

                printf("found at %llu for %llu\n", index, hash);
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
            printf("full loop retry\n");
            return -1;
        }
    }
}
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
    // See atomicHashTable2_removeInner, although this is also modified, as finds need to save results across both
    //  the table and newTable searches

    uint64_t baseIndex = getSlotBaseIndex(table, hash);
    uint64_t index = baseIndex;
    int result = atomicHashTable2_findInner4(this, table, valuesFound, pValuesFoundCur, pValuesFoundLimit, &index, baseIndex, hash);
    if(result > 1) {
        return result;
    }
    todonext
    // Okay, so... if we search table, then search newTable, and then find the values we found in table
    //  are still there, then the find is good. Otherwise, we don't know if the values we found in table
    //  have been moved to newTable, causing us to read the same value twice.
    todonext
    // Or... maybe using redirects... we can just see that the values have been redirected?
    //  Although... redirect happens before we move, so just because something has been redirected doesn't mean
    //  it is in the dest... so maybe redirects won't be useful after all..
    if(newTable) {
        int moveResult = atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);
        if(moveResult > 1) {
            if(result == 0) return 0;
            return moveResult;
        }
        if(result == 1) {
            if(newTable->slotsCount > table->slotsCount) {
                index = index * 2;
            } else if(newTable->slotsCount < table->slotsCount) {
                // Hmm... I am assuming rounding will be fine here...
                index = index / 2;
            }
            for(uint64_t i = 0; i < *pValuesFoundCur; i++) {
                Reference_Release(valuesFound[i].refSource, valuesFound[i].ref);
            }
            *pValuesFoundCur = 0;
            return atomicHashTable2_findInner4(this, newTable, valuesFound, pValuesFoundCur, pValuesFoundLimit, &index, baseIndex, hash);
        }
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
    AtomicHashTableBase* table,
    uint64_t* pIndex
) {
    int reserveResult = atomicHashTable2_updateReserved(this, table, &table->newAllocation, 1, 1);
    if(reserveResult > 0) {
        return reserveResult;
    }
    
    //uint64_t index = *pIndex;
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
            printf("Found move while inserting for %llu\n", hash);
            atomicHashTable2_updateReservedUndo(this, table, -1, -1);
            *pIndex = index;
            return 1;
        }

        if(IS_BLOCK_END(valueRef) || valueRef.valueForSet == BASE_NULL.valueForSet) {
            OutsideReference valueOutsideRef = { 0 };

            HashValue* hashValue = nullptr;
            // We use index here instead of hash, that way the allocator doesn't have it iterate over the filled slots like we did, to save some time...
            Reference_Allocate((MemPool*)table->valuePool, &valueOutsideRef, &hashValue, this->HASH_VALUE_SIZE, index << (64 - table->logSlotsCount));
            if(!hashValue) {
                // We might not be out of memory, we might just have tried to allocate right after we finished a move.
                if(table->valuePool->destructed) {
                    // If the table is destructed it means all the values will be marked as moved, so the next loop will trigger the IS_VALUE_MOVED code.
                    continue;
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
			printf("Inserted %llu (base %llu) for %llu\n", index, startIndex, hash);

            if(valueRef.valueForSet != 0) {
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
    uint64_t index = getSlotBaseIndex(table, hash);
    printf("First try insert for %llu\n", hash);
    int result = AtomicHashTable2_insertInner2(this, hash, value, table, &index);
    if(result > 1) {
        return result;
    }
    if(newTable) {
        int moveResult = atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);
        if(moveResult > 1) {
            if(result == 0) return 0;
            return moveResult;
        }
        if(result == 1) {
            if(newTable->slotsCount > table->slotsCount) {
                index = index * 2;
            } else if(newTable->slotsCount < table->slotsCount) {
                // Hmm... I am assuming rounding will be fine here...
                index = index / 2;
            }
            //todonext
            // Oh wait... insert... can't use the same index we left off at... some of the nulls we saw might have become
            //  0s... And for finds... values can be moved backwards, same for deletions. Hmm... why did I think this would work?
            // Okay, so... this is required, IF we cross the move start boundary?
            //  Oh... so after we call move... yeah, it moves enough entries, so find will be fine... I think. And deletes too?
            //printf("Second try insert for %llu\n", hash);
            return AtomicHashTable2_insertInner2(this, hash, value, newTable, &index);
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