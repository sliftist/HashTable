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
#define IS_VALUE_MOVED(value) (value.isNull && value.valueForSet != BASE_NULL.valueForSet)



// Doesn't do any moves, just makes undoing reserve updates easier, because undoing
//  a reserve update shouldn't really trigger a resize anyway... (and it definitely
//  won't be relied upon to).
void atomicHashTable2_updateReservedUndo(
    AtomicHashTable2* this,
    AtomicHashTableBase* curAlloc,
    int64_t slotsReservedDelta,
    int64_t slotsReservedWithNullsDelta
) {
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
    uint64_t slotsReserved = curAlloc ? curAlloc->slotsReservedWithNulls : 1;
    newSlotCount = newShrinkSize(this, curSlotCount, slotsReserved);
    newSlotCount = newSlotCount ? newSlotCount : newGrowSize(this, curSlotCount, slotsReserved);
    if(!newSlotCount || newSlotCount == curSlotCount) {
        return 0;
    }

    // Again with slotsReserved, which will be the value of slotsReservedWithNulls after we move
    slotsReserved = curAlloc ? curAlloc->slotsReserved : 1;
    newSlotCount = newShrinkSize(this, curSlotCount, slotsReserved);
    newSlotCount = newSlotCount ? newSlotCount : newGrowSize(this, curSlotCount, slotsReserved);
    if(!newSlotCount) {
        newSlotCount = curAlloc->slotsReserved;
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
    newTable->moveState.sourceBlockStart = UINT32_MAX;

    if(pNewAlloc != &this->currentAllocation) {
        for(uint64_t i = 0; i < newSlotCount; i++) {
            newTable->slots[i].value = BASE_NULL4;
        }
    } else {
        newTable->finishedMovingInto = true;
    }

    InsideReference* newTableRef = Reference_Acquire(&newAllocation);
    OutsideReference memPoolTableRef = { 0 };
    Reference_SetOutside(&memPoolTableRef, newTableRef);
    Reference_Release(&newAllocation, newTableRef);

    MemPoolHashed pool = MemPoolHashedDefault(SIZE_PER_VALUE_ALLOC(this), newSlotCount, logSlotsCount, memPoolTableRef, this, atomicHashTable2_memPoolFreeCallback);
    *newTable->valuePool = pool;

    if(InterlockedCompareExchange64(
        (LONG64*)pNewAlloc,
        newAllocation.valueForSet,
        0
    ) != 0) {
        return 1;
    }
    return 0;
}


// Returns 0 if moves were applied, but curAlloc and newAlloc are still relevant,
//  and returns 1 if the move has finished
//  (and of course, returns > 1 on error...)
int atomicHashTable2_applyMoveTableOperationInner(
    AtomicHashTable2* this,
    AtomicHashTableBase* curAlloc,
    AtomicHashTableBase* newAlloc
) {
    // Hash block checking doesn't work because of wrap around (at least, it can't be efficient because of wrap around).
    //  So instead we just move A block, and if we ran into a moved element in a block, then either
    //  that block was already moved, or we just moved it... so this works...

	// This has to be a certain size above our grow/shrink thresholds or else we will try to grow before we finish moving!
    uint64_t minApplyCount = 64;

    uint64_t HASH_VALUE_SIZE = this->HASH_VALUE_SIZE;
    uint64_t VALUE_SIZE = this->VALUE_SIZE;

    // Oops, this is tri-state now. Growing, shrinking, and just staying the same. So we need to fix our code to handle the 3 states.
    bool growing = newAlloc->slotsCount > curAlloc->slotsCount;
    bool shrinking = newAlloc->slotsCount < curAlloc->slotsCount;
    // And of course, slotsCount could be the same, so we aren't growing or shrinking, we are just moving to reduce the tombstone count.

    uint64_t countApplied = 0;
    bool satisifedHash = false;

    MoveStateInner* pMoveState = &curAlloc->moveState;

    while(true) {
        MoveStateInner moveState = *pMoveState;
        // Ensure we atomically read 128 bits.
        if(!EqualsStruct128(&moveState, pMoveState)) {
            continue;
        }

        // Finish the move
        // (we only stop once we are past the end, and not in a block)
        if(!curAlloc || moveState.nextSourceSlot >= curAlloc->slotsCount && moveState.sourceBlockStart == UINT32_MAX) {
            // Remove all BASE_NULL4 values (they are just intermediate values)
			#ifdef DEBUG
			// We shouldn't have any BASE_NULL4s, as they should be removed during moves (and if they aren't, it will mean that
			//	calls us might cause an operation to have to keep looping until all moves are done, which shouldn't be required,
			//	as that means the best worse cast scenario for operations is O(N))
            for(uint64_t i = 0; i < newAlloc->slotsCount; i++) {
                OutsideReference value = newAlloc->slots[i].value;
                if(value.valueForSet == BASE_NULL4.valueForSet) {
					breakpoint();
                }
            }
			#endif

            // Hmm... this really puts a lot on Reference_RedirectReference, but... it should be fine... as long as the caller
            //  makes sure newAlloc is inside of curAlloc, then curAlloc can only redirect once,
            //  AND this actually makes it so this->currentAllocation gets automatically updated when Reference_Acquire is called.
            
			InsideReference* newAllocRef = (void*)((byte*)newAlloc - InsideReferenceSize);

            newAlloc->finishedMovingInto = true;

			MemPoolHashed_Destruct(curAlloc->valuePool);

            Reference_RedirectReference((void*)((byte*)curAlloc - InsideReferenceSize), newAllocRef);
            Reference_DestroyOutsideMakeNull(&curAlloc->newAllocation, newAllocRef);

            return 1;
        }

        uint64_t curSourceIndex = moveState.nextSourceSlot % curAlloc->slotsCount;
        // Move the value from the source into the move state, atomically, making it look like null in the source,
        //  and not like null in the destination.
        if(moveState.sourceValue.valueForSet == 0) {
            OutsideReference* pSource = &curAlloc->slots[curSourceIndex].value;
            OutsideReference sourceValue = *pSource;
			if (sourceValue.valueForSet == 0) {
				if (InterlockedCompareExchange64(
					(LONG64*)pSource,
					(LONG64)BASE_NULL1.valueForSet,
					0
				) != 0) {
					continue;
				}
			}
            else if(sourceValue.valueForSet == BASE_NULL.valueForSet) {
                if(InterlockedCompareExchange64(
                    (LONG64*)pSource,
                    (LONG64)BASE_NULL1.valueForSet,
					(LONG64)BASE_NULL.valueForSet
                ) != (LONG64)BASE_NULL.valueForSet) {
                    continue;
                }
            }
            else if(!sourceValue.isNull) {

                InsideReference* sourceRef = Reference_Acquire(pSource);
                if(!sourceRef) {
                    continue;
                }

				// Use whatever value we acquired, even if it isn't the original sourceValue

				OutsideReference setSourceValue = { 0 };
				setSourceValue.isNull = 1;
				setSourceValue.pointerClipped = (uint64_t)sourceRef;

                // This... freezes pSource, no one will ever change it after this. AND, it reduces the outside ref count to 0
                //  first, so we don't break any outstanding references. HOWEVER! It doesn't break the pointer, and it doesn't
                //  technically destroy the outside reference (as in, the outside reference's reference to the inner reference
                //  is kept alive). So... it means even if this thread fails, the next thread can pick up after it, trivially.

                setSourceValue.count = 0;
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
            }
            
            // If it is BASE_NULL1, it is because it was BASE_NULL and we changed it, so leave that null
            if(sourceValue.valueForSet != BASE_NULL1.valueForSet) {
                sourceValue.isNull = 0;
            }

            // So at this point the source value is immutable. No user will replace an isNull value
            //  (except for BASE_NULL1, but setting isNull on a valid value will never create that, as that value
            //  would have to have a pointer value of 1, which I am assuming can't happen...)
            // So... that is very very nice
            
            MoveStateInner newMoveState = moveState;
            newMoveState.sourceValue = sourceValue;
            if(!InterlockedCompareExchangeStruct128(pMoveState, &moveState, &newMoveState)) {
                continue;
            }
			moveState = newMoveState;
        }

        bool hasValue = moveState.sourceValue.valueForSet != 0 && moveState.sourceValue.valueForSet != BASE_NULL1.valueForSet;

        if(!hasValue) {
            satisifedHash = true;
        }

        uint64_t hash = 0;

        // Now redirect the sourceValue
        if(hasValue) {
            InsideReference* valueRef = Reference_Acquire(&pMoveState->sourceValue);
            if(!valueRef) {
                // Someone else must have destroyed it
                continue;
            }
            
            // If it isn't redirect, make it redirected, updating valueRef, and moveState.sourceValue
            if(!Reference_HasBeenRedirected(valueRef)) {
                HashValue* value = Reference_GetValue(valueRef);
                OutsideReference newValueRef = { 0 };
                hash = value->hash;
                HashValue* newValue = nullptr;
                Reference_Allocate((MemPool*)newAlloc->valuePool, &newValueRef, &newValue, this->HASH_VALUE_SIZE, hash);
                if(!newValue) {
                    Reference_Release(&pMoveState->sourceValue, valueRef);
                    return 3;
                }
                newValue->hash = hash;
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
                moveState.sourceValue.pointerClipped = (uint64_t)newValueRefInside;

                Reference_Release(&pMoveState->sourceValue, valueRef);

                // Reference_Acquire will update sourceValue to the redirected reference,
                //  which allows us to destroy our previous outside reference.
                valueRef = Reference_Acquire(&pMoveState->sourceValue);

                Reference_DestroyOutside(&newValueRef, newValueRefInside);
                Reference_Release(&emptyReference, newValueRefInside);
            } else {
                HashValue* value = Reference_GetValue(valueRef);
                hash = value->hash;
            }

            if(!MemPoolHashed_IsInPool(newAlloc->valuePool, valueRef)) {
                Reference_Release(&pMoveState->sourceValue, valueRef);
                continue;
            }

            Reference_Release(&pMoveState->sourceValue, valueRef);
        }

        // Now moveState.sourceValue is redirected (or 0) (and hash is set)

        uint64_t sourceBaseIndex = getSlotBaseIndex(curAlloc, hash);

        if(moveState.nextSourceSlot < moveState.sourceBlockStart && moveState.sourceBlockStart != UINT32_MAX) {
            if(sourceBaseIndex < moveState.sourceBlockStart) {
                // We wrapped around, and found a non-wrapped around value. So we are done moving it.
                hasValue = false;
            }
        }

        if(hasValue) {
            bool wrappedAroundStop = false;
            if(sourceBaseIndex > moveState.nextSourceSlot) {
                // This means it is a wrap around value. The redirecting is fine, but now we have to just skip past it,
                //  handling it when we do wrap around...
                MoveStateInner newMoveState = moveState;
                newMoveState.nextSourceSlot++;
                newMoveState.sourceValue.valueForSet = 0;

                uint64_t nextSourceSlot = moveState.nextSourceSlot;
                // Update moveState, because it might trivially change due to ref count changes
                //  (which shouldn't block updating the index)
                moveState = *pMoveState;
                if(!EqualsStruct128(&moveState, pMoveState)) {
                    continue;
                }
                if(moveState.nextSourceSlot != nextSourceSlot) {
                    continue;
                }

                InterlockedCompareExchangeStruct128(pMoveState, &moveState, &newMoveState);
                continue;
            }

			uint64_t curDestIndex = getSlotBaseIndex(newAlloc, hash);
            while(true) {
                OutsideReference value = newAlloc->slots[curDestIndex].value;
                if(value.valueForSet == BASE_NULL4.valueForSet) {
                    break;
                }
                if(value.valueForSet == 0) {
                    break;
                }
                curDestIndex = (curDestIndex + 1) % newAlloc->slotsCount;
                if(curDestIndex == 0) {
                    // Impossible, the table is too full?
                    OnError(9);
                    return 9;
                }
            }

            // Clone moveState.sourceValue, and then atomically move that unique value into the destination
            InsideReference* destRef = Reference_Acquire(&pMoveState->sourceValue);
            // If we can't get a reference to it, it must be because nextSourceSlot was updated
            if(!destRef) continue;
            // Also, it could be for a different move
            if(!FAST_CHECK_POINTER(moveState.sourceValue, destRef)) {
                Reference_Release(&pMoveState->sourceValue, destRef);
                continue;
            }

            OutsideReference destOutsideRef = { 0 };
            Reference_SetOutside(&destOutsideRef, destRef);
			Reference_Release(&pMoveState->sourceValue, destRef);

            InterlockedIncrement64((LONG64*)&newAlloc->slotsReserved);
            InterlockedIncrement64((LONG64*)&newAlloc->slotsReservedWithNulls);
            if(InterlockedCompareExchange64(
                (LONG64*)&newAlloc->slots[curDestIndex].value,
                destOutsideRef.valueForSet,
                BASE_NULL4.valueForSet
            ) != BASE_NULL4.valueForSet) {
                InterlockedDecrement64((LONG64*)&newAlloc->slotsReserved);
                InterlockedDecrement64((LONG64*)&newAlloc->slotsReservedWithNulls);
                DestroyUniqueOutsideRef(&destOutsideRef);
                continue;
            }
		}

        // the destination has definitely been populated here


        // Update nextSourceSlot and destOffset
        {
            MoveStateInner newMoveState = moveState;
            newMoveState.nextSourceSlot++;
            newMoveState.sourceValue.valueForSet = 0;
            if(!hasValue) {
                // Source block finished, so we know that the corresponding dest block is also finished.
                //  This means we need to set all BASE_NULL4s to 0, and reset 
                    
                // We know that the source block won't be extended, as we set BASE_NULL in this source slot, and so anything trying to fill in that slot will
                //  instead help apply this move.

                // Hmm... I think the rounding that shrinking experiences should work itself out... Not 100% sure though...
                // TODO: We should verify the rounding is okay...
                uint64_t destBlockEnd = curSourceIndex + 1;
                if(growing) {
                    destBlockEnd = destBlockEnd * 2;
                } else if(shrinking) {
                    breakpoint();
                    destBlockEnd = destBlockEnd / 2;
                }
                uint64_t sourceBlockStart = moveState.sourceBlockStart != UINT32_MAX ? moveState.sourceBlockStart : curSourceIndex;
                uint64_t destBlockCur = sourceBlockStart;
                if(growing) {
                    destBlockCur = destBlockCur * 2;
                } else if(shrinking) {
                    destBlockCur = destBlockCur / 2;
                }

                while(destBlockCur != destBlockEnd) {
                    OutsideReference value = newAlloc->slots[destBlockCur].value;
                    if(value.valueForSet == BASE_NULL4.valueForSet) {
                        InterlockedCompareExchange64(
                            (LONG64*)&newAlloc->slots[destBlockCur].value,
                            0,
                            BASE_NULL4.valueForSet
                        );
                    }
                    destBlockCur++;
                }
                
                newMoveState.sourceBlockStart = UINT32_MAX;
            } else {
                if(newMoveState.sourceBlockStart == UINT32_MAX) {
                    newMoveState.sourceBlockStart = moveState.nextSourceSlot;
                }
            }

			if (hasValue) {
				if (!Reference_ReduceToZeroOutsideRefs(&pMoveState->sourceValue)) {
					continue;
				}
			}


            uint64_t nextSourceSlot = moveState.nextSourceSlot;
            // Update moveState, because it might trivially change due to ref count changes
            //  (which shouldn't block updating the index)
            moveState = *pMoveState;
            if(!EqualsStruct128(&moveState, pMoveState)) {
                continue;
            }
            if(moveState.sourceValue.count != 0) {
                continue;
            }
            if(moveState.nextSourceSlot != nextSourceSlot) {
                continue;
            }			


            if(!InterlockedCompareExchangeStruct128(pMoveState, &moveState, &newMoveState)) {
                continue;
            }
            countApplied++;

            // We atomically wiped this out, so now we get to destroy it
            if(hasValue) {
				DestroyUniqueOutsideRef(&moveState.sourceValue);
            }
        }

        if(countApplied >= minApplyCount && satisifedHash) {
            // Done...
            return 0;
        }
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
    uint64_t hash,
    FindResult* valuesFound,
    uint64_t* pValuesFoundCur,
    uint64_t* pValuesFoundLimit
) {
    *pValuesFoundCur = 0;
    uint64_t index = getSlotBaseIndex(table, hash);
    uint64_t loopCount = 0;
    while(true) {
        OutsideReference* pValue = &table->slots[index].value;
        OutsideReference value = *pValue;
        if(IS_VALUE_MOVED(value)) {
            return 1;
        }

        if(value.valueForSet == 0) {
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
                    return -1;
                }

                *pValuesFoundCur = valuesFoundCur;
                valuesFound[valuesFoundCur - 1] = result;
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
}
int atomicHashTable2_findInner3(
    AtomicHashTable2* this,
    uint64_t hash,
    FindResult* valuesFound,
    uint64_t* pValuesFoundLimit,
    void* callbackContext,
    void(*callback)(void* callbackContext, void* value),
    AtomicHashTableBase* table
) {
    uint64_t valuesFoundCur;
    int result = atomicHashTable2_findInner4(this, table, hash, valuesFound, &valuesFoundCur, pValuesFoundLimit);
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
    // See atomicHashTable2_removeInner
    int result = atomicHashTable2_findInner3(this, hash, valuesFound, pValuesFoundLimit, callbackContext, callback, table);
    if(newTable) {
        int moveResult = atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);
        if(result == 1 && moveResult == 1) {
        } else {
            if(result == 0 && moveResult == 1) {
                return 0;
            }
            RETURN_ON_ERROR(moveResult);
        }
    }
    if(result == 1) {
        return atomicHashTable2_findInner3(this, hash, valuesFound, pValuesFoundLimit, callbackContext, callback, newTable);
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
    while(true) {
        InsideReference* tableRef = Reference_Acquire(&this->currentAllocation);
        AtomicHashTableBase* table = Reference_GetValue(tableRef);
        if(!table) {
            return 0;
        }

        InsideReference* newTableRef = table->newAllocation.valueForSet ? Reference_Acquire(&table->newAllocation) : 0;
        AtomicHashTableBase* newTable = Reference_GetValue(newTableRef);

        int result = atomicHashTable2_findInner2(this, hash, valuesFound, pValuesFoundLimit, callbackContext, callback, table, newTable);

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
    uint64_t loopCount = 0;
    while(true) {
        OutsideReference* pValue = &table->slots[index].value;
        OutsideReference valueRef = *pValue;

        if(IS_VALUE_MOVED(valueRef)) {
            atomicHashTable2_updateReservedUndo(this, table, -1, -1);
            return 1;
        }

        if(valueRef.valueForSet == 0 || valueRef.valueForSet == BASE_NULL.valueForSet) {
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
            memcpy(
                (byte*)hashValue + sizeof(HashValue),
                value,
                this->VALUE_SIZE
            );
            hashValue->hash = hash;

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

            if(valueRef.valueForSet != 0) {
                // We reused a null entry, so we can release the reserved amount, as we didn't use up any new entries
                atomicHashTable2_updateReservedUndo(this, table, -1, -1);
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
    // This function is basically atomicHashTable2_removeInner
    int result = AtomicHashTable2_insertInner2(this, hash, value, table);
    if(newTable) {
        int moveResult = atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);
        if(result == 1 && moveResult == 1) {
        } else {
            if(result == 0 && moveResult == 1) {
                return 0;
            }
            RETURN_ON_ERROR(moveResult);
        }
    }
    if(result == 1) {
        return AtomicHashTable2_insertInner2(this, hash, value, newTable);
    }
    return result;
}

int AtomicHashTable2_insert(AtomicHashTable2* this, uint64_t hash, void* value) {
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
        OutsideReference* pValue = &table->slots[index].value;
        OutsideReference valueRef = *pValue;

        if(IS_VALUE_MOVED(valueRef)) {
            return 1;            
        }
		if (valueRef.valueForSet == 0) {
			return 0;
		}
        if(valueRef.valueForSet != BASE_NULL.valueForSet) {
            InsideReference* ref = Reference_Acquire(pValue);
            if(!ref) {
                continue;
            }
            HashValue* value = Reference_GetValue(ref);
            void* valueVoid = (byte*)value + sizeof(HashValue);
            bool shouldRemove = callback(callbackContext, valueVoid);
            if(shouldRemove) {
                bool destroyedRef = Reference_DestroyOutsideMakeNull(pValue, ref);
                Reference_Release(pValue, ref);
                if(!destroyedRef) {
                    continue;
				}
				else {
					atomicHashTable2_updateReserved(this, table, &table->newAllocation, -1, 0);
				}
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
    int result = atomicHashTable2_removeInner2(this, hash, callbackContext, callback, table);
    // It is important to move after checking table, that way either the move is on the cusp of what we read
    //  (and so this move makes the newTable have the info we need), newTable already had the data we needed,
    //  OR table had the data we needed. If we do it at the beginning we could easily leave the move in the middle
    //  of where we check, requiring an extra iteration.
    if(newTable) {
        int moveResult = atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);
        if(result == 1 && moveResult == 1) {
            // If result wants to move, and move finished, then newTable will have the result, so we should try it, and if it succeeds,
            //  then we don't need to retry.
        } else {
            // Don't retry if we succeeded, even if move finished the move.
            if(result == 0 && moveResult == 1) {
                return 0;
            }
            RETURN_ON_ERROR(moveResult);
        }
    }
    if(result == 1) {
        return atomicHashTable2_removeInner2(this, hash, callbackContext, callback, newTable);
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