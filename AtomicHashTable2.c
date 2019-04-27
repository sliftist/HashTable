#include "AtomicHashTable2.h"

#include "bittricks.h"
#include "Timing.h"

#define RETURN_ON_ERROR(x) { int returnOnErrorResult = x; if(returnOnErrorResult != 0) return returnOnErrorResult; }

// (sizeof(InsideReference) because our tracked allocations are this much smaller)

// Means when we add more bits, overlaps stay as neighbors (which is a useful property when resizing)
#define getSlotBaseIndex(alloc, hash) ((hash) >> (64 - (alloc)->logSlotsCount))


uint64_t log2RoundDown(uint64_t v) {
	return log2(v * 2) - 1;
}

#define SIZE_PER_COUNT(table) (table->HASH_VALUE_SIZE + InsideReferenceSize + MemPoolHashed_VALUE_OVERHEAD + sizeof(AtomicSlot))

uint64_t minSlotCount(AtomicHashTable2* this) {
    uint64_t sizePerCount = SIZE_PER_COUNT(this);
    
    uint64_t minCount = (PAGE_SIZE - sizeof(AtomicHashTableBase) - InsideReferenceSize - sizeof(MemPoolHashed)) / sizePerCount;

    // Must be a power of 2, for getSlotBaseIndex
    minCount = 1ll << log2RoundDown(minCount);

    return minCount;
}
uint64_t getTableSize(AtomicHashTable2* this, uint64_t slotCount) {
    uint64_t tableSize = SIZE_PER_COUNT(this) * slotCount;
}


uint64_t newShrinkSize(AtomicHashTable2* this, uint64_t slotCount, uint64_t fillCount) {
    uint64_t minCount = minSlotCount(this);
    if((int64_t)fillCount < ((int64_t)slotCount - (int64_t)minCount) / 10) {
        return max(minCount, slotCount / 2);
    }
    // (so implicitly, never shrink to 0. Because then... why?)
    return 0;
}
uint64_t newGrowSize(AtomicHashTable2* this, uint64_t slotCount, uint64_t fillCount) {
    // Grow threshold and shrink threshold can't be within a factor of the grow/shrink factor, or else
    //  a grow will trigger a shrink...
    uint64_t minCount = minSlotCount(this);
    if(fillCount > (slotCount / 10 * 4)) {
        return max(minCount, slotCount * 2);
    }
    return 0;
}


void destroyUniqueOutsideRef(OutsideReference* ref)  {
    void* temp = Reference_Acquire(&ref);
    if(!temp) return;
    Reference_DestroyOutside(&ref, temp);
    Reference_Release(&ref, temp);
    ref->valueForSet = 0;
}

void atomicHashTable2_memPoolFreeCallback(AtomicHashTable2* this, InsideReference* ref) {
    if(!Reference_HasBeenRedirected(ref)) {
        // And if it hasn't, it never will be, as the free callback is only called when all outside and inside references are gone...
        void* userValue = (byte*)ref + sizeof(InsideReference) + sizeof(HashValue);
        this->deleteValue(userValue);
    }
}

// Has to be a value copied from shared storage, or else this will race...
#define IS_VALUE_MOVED(value) (value.isNull && value.valueForSet != BASE_NULL.valueForSet)

// Called when we want to reserve entries, and after we find we didn't use all of the entries we reserved,
//  because we reused some null entries instead.
//  (should not be called in a retry loop, should only be called after or before?)
int atomicHashTable2_mutateReserved(
    AtomicHashTable2* this,
    OutsideReference** curAllocRefOutsideOut,
    InsideReference** curAllocRefOut,
    AtomicHashTableBase** curAllocOut,
    int64_t reservedDelta,
    int64_t reservedWithNullsDelta
) {
    InsideReference* curAllocRef = *curAllocRefOut;
    if(!curAllocRef) {
        InsideReference* curAllocRef = Reference_Acquire(&this->currentAllocation);
        AtomicHashTableBase* curAlloc = Reference_GetValue(curAllocRef);
    }


    
    if(curAlloc->newAllocation.valueForSet) {
        // Then there is no point is changing the reserved, 
        return;
    }

    if(!this->currentAllocation.valueForSet) {
        *curAllocRefOutsideOut = nullptr;
        *curAllocRefOut = nullptr;
        *curAllocOut = nullptr;
        return 0;
    }
}

int atomicHashTable2_moveSome(
    AtomicHashTable2* this,
    OutsideReference** curAllocRefOutsideOut,
    InsideReference** curAllocRefOut,
    AtomicHashTableBase** curAllocOut,
    uint64_t countToMove
) {

}



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
    uint64_t slotsReserved = curAlloc ? curAlloc->slotsReservedWithNulls : 0;
    newSlotCount = newShrinkSize(this, curSlotCount, slotsReserved);
    newSlotCount = newSlotCount ? newSlotCount : newGrowSize(this, curSlotCount, slotsReserved);
    if(!newSlotCount) {
        return 0;
    }

    // Again with slotsReserved, which will be the value of slotsReservedWithNulls after we move
    slotsReserved = curAlloc ? curAlloc->slotsReserved : 0;
    newSlotCount = newShrinkSize(this, curSlotCount, slotsReserved);
    newSlotCount = newSlotCount ? newSlotCount : newGrowSize(this, curSlotCount, slotsReserved);
    if(!newSlotCount) {
        newSlotCount = curAlloc->slotsReserved;
    }

    // Create and prepare the new allocation
    OutsideReference newAllocation;
    AtomicHashTableBase* newTable;
    uint64_t tableSize = getTableSize(this, newSlotCount);
    Reference_Allocate(&memPoolSystem, &newAllocation, newTable, tableSize, 0);
    if(!newTable) {
        return 3;
    }

    newTable->slotsCount = newSlotCount;
    newTable->logSlotsCount = log2(newSlotCount);
    newTable->slots = (void*)((byte*)newTable + sizeof(AtomicHashTableBase));
    newTable->valuePool = (void*)((byte*)newTable->slots + sizeof(AtomicSlot) * newSlotCount);

    for(uint64_t i = 0; i < newSlotCount; i++) {
        newTable->slots[i].value = BASE_NULL4;
    }

    MemPoolHashed pool = MemPoolHashedDefault(this->VALUE_SIZE, newSlotCount, newAllocation, this, atomicHashTable2_memPoolFreeCallback);
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

    uint64_t minApplyCount = 10;

    uint64_t HASH_VALUE_SIZE = this->HASH_VALUE_SIZE;
    uint64_t VALUE_SIZE = this->VALUE_SIZE;

    bool growing = newAlloc->slotsCount > curAlloc->slotsCount;

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
        if(!curAlloc || moveState.nextSourceSlot == curAlloc->slotsCount) {
            // Remove all BASE_NULL4 values (they are just intermediate values)
            for(uint64_t i = 0; i < newAlloc->slotsCount; i++) {
                OutsideReference value = newAlloc->slots[i].value;
                if(value.valueForSet == BASE_NULL4.valueForSet) {
                    InterlockedCompareExchange64(
                        (LONG64*)&newAlloc->slots[i].value,
                        0,
                        BASE_NULL4.valueForSet
                    );
                }
            }

            // Hmm... this really puts a lot on Reference_RedirectReference, but... it should be fine... as long as the caller
            //  makes sure newAlloc is inside of curAlloc, then curAlloc can only redirect once,
            //  AND this actually makes it so this->currentAllocation gets automatically updated when Reference_Acquire is called.
            Reference_RedirectReference(curAlloc, newAlloc);

            Reference_DestroyOutsideMakeNull(&curAlloc->newAllocation, newAlloc);

            return 1;
        }

        uint32_t sourceSlot = moveState.nextSourceSlot;

        // Move the value from the source into the move state, atomically, making it look like null in the source,
        //  and not like null in the destination.
        if(moveState.sourceValue.valueForSet == 0) {
            OutsideReference* pSource = &curAlloc->slots[moveState.nextSourceSlot].value;
            OutsideReference sourceValue = *pSource;
            if(sourceValue.valueForSet == BASE_NULL.valueForSet) {
                if(InterlockedCompareExchange64(
                    (LONG64*)pSource,
                    &BASE_NULL1,
                    &BASE_NULL
                ) != BASE_NULL.valueForSet) {
                    continue;
                }
            }
            else if(!sourceValue.isNull) {
                OutsideReference setSourceValue = sourceValue;
                setSourceValue.isNull = 1;


                InsideReference* sourceRef = Reference_Acquire(pSource);
                if(!FAST_CHECK_POINTER(sourceValue, sourceRef)) {
                    continue;
                }

                //todonext
                // Crap... this actually doesn't work. We have to atomically move it,
                //  which requires moving the ref count into the underlying inside reference.
                //  Which we have code to do... I think...
                // And actually! This whole thing of setting isNull doesn't work... We need to... both set isNull,
                //  AND reduce the ref count to 0. Hmm... I think we should just expose a function which
                //  lets us try to reduce to ref count to 0, and then only swap with values that have a ref count
                //  of 0, making the swap perfectly safe...

                //todonext
                // Or maybe... try to ReplaceOutside with a version of the outside reference that has ref count set to 0,
                //  and isNull set.

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
                Reference_Release(pSource, sourceRef);

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
                uint64_t hash = value->hash;
                HashValue* newValue = nullptr;
                Reference_Allocate(&newAlloc->valuePool, &newValueRef, &newValue, this->HASH_VALUE_SIZE, hash);
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

                Reference_Release(&pMoveState->sourceValue, valueRef);

                // Reference_Acquire will update sourceValue to the redirected reference,
                //  which allows us to destroy our previous outside reference.
                valueRef = Reference_Acquire(&pMoveState->sourceValue);

                Reference_DestroyOutside(&newValueRef, newValueRefInside);
                Reference_Release(&newValueRef, newValueRefInside);
            }

            // Ensure we atomically read 128 bits.
            moveState = *pMoveState;
            if(!EqualsStruct128(&moveState, pMoveState)) {
                Reference_Release(&pMoveState->sourceValue, valueRef);
                continue;
            }

            if(!FAST_CHECK_POINTER(moveState.sourceValue, valueRef)) {
                Reference_Release(&pMoveState->sourceValue, valueRef);
                continue;
            }

            if(!MemPoolHashed_IsInPool(&newAlloc->valuePool, valueRef)) {
                Reference_Release(&pMoveState->sourceValue, valueRef);
                continue;
            }

            HashValue* value = Reference_GetValue(valueRef);
            hash = value->hash;

            Reference_Release(&pMoveState->sourceValue, valueRef);
        }

        // Now moveState.sourceValue is redirected (or 0) (and hash is set)

        int32_t destOffset = moveState.destOffset;

        if(hasValue) {
            uint64_t destIndexBase = getSlotBaseIndex(newAlloc, hash) + destOffset;
            if(moveState.nextSourceSlot > 0) {
                uint64_t lastDestIndex = (growing ? (moveState.nextSourceSlot - 1) * 2 : (moveState.nextSourceSlot - 1) / 2) + destOffset;
                if(lastDestIndex >= destIndexBase) {
                    destOffset++;
                    destIndexBase++;
                }
            }

            uint64_t destIndex = destIndexBase % newAlloc->slotsCount;

            // Clone moveState.sourceValue, and then atomically move that unique value into the destination
            InsideReference* destRef = Reference_Acquire(&moveState.sourceValue);
            // If we can't get a reference to it, it must be because nextSourceSlot was updated
            if(!destRef) continue;

            OutsideReference destOutsideRef = { 0 };
            Reference_SetOutside(&destOutsideRef, destRef);

            InterlockedIncrement64((LONG64*)&newAlloc->slotsReserved);
            InterlockedIncrement64((LONG64*)&newAlloc->slotsReservedWithNulls);
            if(InterlockedCompareExchange64(
                (LONG64*)&newAlloc->slots[destIndex].value,
                destOutsideRef.valueForSet,
                BASE_NULL4.valueForSet
            ) != BASE_NULL4.valueForSet) {
                InterlockedDecrement64((LONG64*)&newAlloc->slotsReserved);
                InterlockedDecrement64((LONG64*)&newAlloc->slotsReservedWithNulls);
                destroyUniqueOutsideRef(&destOutsideRef);
                continue;
            }
        }

        // the destination has definitely been populated here
        
        if(growing) {
            destOffset = max(0, destOffset - 2);
        } else if(sourceSlot % 2) {
            destOffset = max(0, destOffset - 1);
        }

        // Update nextSourceSlot and destOffset
        {
            MoveStateInner newMoveState = moveState;
            newMoveState.nextSourceSlot++;
            newMoveState.destOffset = destOffset;
            newMoveState.sourceValue.valueForSet = 0;

            if(!InterlockedCompareExchangeStruct128(pMoveState, &moveState, &newMoveState)) {
                continue;
            }
            countApplied++;

            // We atomically wiped this out, so now we get to destroy it
            if(hasValue) {
                destroyUniqueOutsideRef(&moveState.sourceValue);
            }
        }

        if(countApplied >= minApplyCount && satisifedHash) {
            // Done...
            return 0;
        }
    }
}


int atomicHashTable2_applyMoveAndFree(
    AtomicHashTable2* this,
    AtomicHashTableBase* curAlloc,
    InsideReference* curAllocRef
) {
    InsideReference* newAllocRef = Reference_Acquire(&curAlloc->newAllocation);
    if(!newAllocRef) {
        // curAlloc is now 2 behind. Just restart everything.
        Reference_Release(&this->currentAllocation, curAllocRef);
        return 1;
    }

    AtomicHashTableBase* newAlloc = Reference_GetValue(newAllocRef);
    int result = atomicHashTable2_applyMoveTableOperationInner(this, curAlloc, newAlloc);
    // We can optimize result <= 1 case... but it doesn't happen often, so whatever...
    //  The difficulty is that even though we know curAlloc should be freed and then become
    //  newAlloc, it causes problems when we want to free our newAllocRef, as it's old outside
    //  reference has been destroyed, and the memory freed. So we would need to remember
    //  to release it from BASE_NULL, at which point... why even bother with the optimization...
    Reference_Release(&curAlloc->newAllocation, newAllocRef);
    Reference_Release(&this->currentAllocation, curAllocRef);
    if(result > 1) {
        return result;
    }
    return 1;
}


#pragma pack(push, 1)
typedef struct {
    OutsideReference* refSource;
    InsideReference* ref;
    HashValue* value;
} FindResult;
#pragma pack(pop)

// May return 1 just because of a move, and not because of a pValuesFoundLimit changed
int AtomicHashTable2_findInner2(
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
            Reference_Release(&this->currentAllocation, table);
            return 0;
        }
        if(value.valueForSet != BASE_NULL.valueForSet) {
            InsideReference* valueRef = Reference_Acquire(pValue);
            if(!valueRef) {
                // Might be because of move, but is more likely just because the value was set to the tombstone value
                //  (BASE_NULL), so we just just try rereading it
                continue;
            }
            HashValue* value = Reference_GetValue(valueRef);
            
            FindResult result;
            result.refSource = pValue;
            result.ref = valueRef;
            result.value = value;

            uint64_t valuesFoundCur = *pValuesFoundCur;
            uint64_t valuesFoundLimit = *pValuesFoundLimit;
            valuesFoundCur++;
            if(valuesFoundCur > valuesFoundLimit) {
                // Eh... *10, because we don't know how many entries there might be. We could loop and count how many values
                //  there might be, and allocate less... but I don't think we need to optimize the case of many results right now...
                *pValuesFoundLimit = valuesFoundLimit * 10;
                return 1;
            }

            *pValuesFoundCur = valuesFoundCur;
            valuesFound[valuesFoundCur - 1] = result;
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

// Shouldn't be called if tableRefSource doesn't have a value (you need to check that first, or else we will always return 1).
int AtomicHashTable2_findInner(
    AtomicHashTable2* this,
    uint64_t hash,
    FindResult* valuesFound,
    uint64_t* pValuesFoundLimit,
    OutsideReference* tableRefSource,
    void* callbackContext,
    void(*callback)(void* callbackContext, void* value)
) {
    InsideReference* curAllocRef = Reference_Acquire(tableRefSource);
    if(!curAllocRef) {
        return 1;
    }
    AtomicHashTableBase* curAlloc = Reference_GetValue(curAllocRef);

    uint64_t valuesFoundLimitBefore = *pValuesFoundLimit;
    
    uint64_t valuesFoundCur = 0;
    int result = AtomicHashTable2_findInner2(this, curAlloc, hash, valuesFound, &valuesFoundCur, pValuesFoundLimit);
        
    if(result > 0) {
        for(uint64_t i = 0; i < valuesFoundCur; i++) {
            Reference_Release(valuesFound[i].refSource, valuesFound[i].ref);
        }

        if(result > 1 || result == 1 && valuesFoundLimitBefore != *pValuesFoundLimit) {
            Reference_Release(&this->currentAllocation, curAllocRef);
            return result;
        }

        result = AtomicHashTable2_findInner(this, hash, valuesFound, pValuesFoundLimit, &curAlloc->newAllocation, callbackContext, callback);

        Reference_Release(&this->currentAllocation, curAllocRef);

        return result;
    }

    for(uint64_t i = 0; i < valuesFoundCur; i++) {
        void* value = (byte*)valuesFound[i].value + sizeof(HashValue);
        callback(callbackContext, value);
    }
    for(uint64_t i = 0; i < valuesFoundCur; i++) {
        Reference_Release(valuesFound[i].refSource, valuesFound[i].ref);
    }

    Reference_Release(&this->currentAllocation, curAllocRef);
    return 0;
}


todonext;
// Write the outside find wrapper, probably with a pool for valuesFound, or maybe stack allocated first?
//  (and also check the ref source for 0 really quick before we even call findInner)
int AtomicHashTable2_find(
    AtomicHashTable2* this,
    uint64_t hash,
    void* callbackContext,
    // May return results that don't equal the given hash, so a deep comparison should be done on the value
	//	to determine if it is the one you want.
	// (and obviously, may call this callback multiple times for one call, but not more than one time per call
    //  for the same value, assuming no value is added to the table more than once).
    void(*callback)(void* callbackContext, void* value)
) {
    uint64_t valuesFoundLimit = 10;
    while(true) {
        FindResult* valuesFound = MemPoolFixed_Allocate(&this->findValuePool, sizeof(FindResult) * valuesFoundLimit, hash);
        if(!valuesFound) {
            OnError(3);
            return 3;
        }
        uint64_t valuesFoundLimitLast = valuesFoundLimit;
        int result = AtomicHashTable2_findInner(this, hash, valuesFound, &valuesFoundLimit, &this->currentAllocation, callbackContext, callback);
        if(result != 1 || valuesFoundLimit != valuesFoundLimitLast) {
            MemPoolFixed_Free(&this->findValuePool, valuesFound);
            if(result != 1) {
                return result;
            }
            valuesFound = MemPoolFixed_Allocate(&this->findValuePool, sizeof(FindResult) * valuesFoundLimit, hash);
        }
    }
}




int AtomicHashTable2_insertInner(
    AtomicHashTable2* this,
    uint64_t hash,
    void* value,
    InsideReference* tableRef
) {
    if(!tableRef) {
        return 1;
    }
    AtomicHashTableBase* table = Reference_GetValue(tableRef);

    int reserveResult = atomicHashTable2_updateReserved(this, table, &table->newAllocation, 1, 1);
    if(reserveResult > 0) {
        return reserveResult;
    }

    InsideReference* newTableRef = Reference_Acquire(&table->newAllocation);
    AtomicHashTableBase* newTable = nullptr;
    if(newTableRef) {
        newTable = Reference_GetValue(newTableRef);
        int moveResult = atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);
        if(moveResult > 0) {
            Reference_Release(&table->newAllocation, newTable);
            atomicHashTable2_updateReservedUndo(this, table, -1, -1);
            return moveResult;
        }
    }

    OutsideReference valueOutsideRef = { 0 };
    HashValue* hashValue = nullptr;
    Reference_Allocate(&table->valuePool, &valueOutsideRef, &hashValue, this->HASH_VALUE_SIZE, hash);
    if(!hashValue) {
        atomicHashTable2_updateReservedUndo(this, table, -1, -1);
        Reference_Release(&table->newAllocation, newTableRef);
        return 3;
    }
    memcpy(
        (byte*)hashValue + sizeof(HashValue),
        value,
        this->VALUE_SIZE
    );
    hashValue->hash = hash;
    
    uint64_t index = getSlotBaseIndex(table, hash);
    uint64_t loopCount = 0;
    while(true) {
        OutsideReference* pValue = &table->slots[index].value;
        OutsideReference valueRef = *pValue;

        if(IS_VALUE_MOVED(valueRef)) {
            atomicHashTable2_updateReservedUndo(this, table, -1, -1);
            destroyUniqueOutsideRef(&valueOutsideRef);
            if(!newTableRef) {
                return 1;
            }
            int moveResult = atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);
            if(moveResult > 0) {
                Reference_Release(&table->newAllocation, newTableRef);
                return moveResult;
            }

            int result = AtomicHashTable2_insertInner(this, hash, value, newTableRef);
            Reference_Release(&table->newAllocation, newTableRef);
            return result;
        }

        if(valueRef.valueForSet == 0 || valueRef.valueForSet == BASE_NULL.valueForSet) {
            // Is safe, because we are replacing a non-reference value (and NOT something move is dealing with either, so
            //  it is really safe).
            if(InterlockedCompareExchange64(
                (LONG64*)pValue,
                valueOutsideRef.valueForSet,
                valueRef.valueForSet
            ) != valueRef.valueForSet) {
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
                destroyUniqueOutsideRef(&valueOutsideRef);
                Reference_Release(&table->newAllocation, newTableRef);
                // Too many loops around the table...
                OnError(9);
                return 9;
            }
        }
    }
}

int AtomicHashTable2_insert(AtomicHashTable2* this, uint64_t hash, void* value) {
    if(!this->currentAllocation.valueForSet) {
        atomicHashTable2_updateReserved(this, nullptr, &this->currentAllocation, 0, 0);
    }
    while(true) {
        InsideReference* tableRef = Reference_Acquire(&this->currentAllocation);
        int result = AtomicHashTable2_insertInner(this, hash, value, tableRef);
        Reference_Release(&this->currentAllocation, tableRef);
        if(result == 1) continue;
        return result;
    }
}



int AtomicHashTable2_removeInner(
    AtomicHashTable2* this,
    uint64_t hash,
	void* callbackContext,
	bool(*callback)(void* callbackContext, void* value),
    InsideReference* tableRef
) {
    if(!tableRef) {
        return 1;
    }
    AtomicHashTableBase* table = Reference_GetValue(tableRef);

    // atomicHashTable2_updateReserved(this, table, &table->newAllocation, 1, 1)

    InsideReference* newTableRef = Reference_Acquire(&table->newAllocation);
    AtomicHashTableBase* newTable = nullptr;
    if(newTableRef) {
        newTable = Reference_GetValue(newTableRef);
        int moveResult = atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);
        if(moveResult > 0) {
            Reference_Release(&table->newAllocation, newTable);
            return moveResult;
        }
    }
    
    uint64_t index = getSlotBaseIndex(table, hash);
    uint64_t loopCount = 0;
    while(true) {
        OutsideReference* pValue = &table->slots[index].value;
        OutsideReference valueRef = *pValue;

        if(IS_VALUE_MOVED(valueRef)) {
            if(!newTableRef) {
                return 1;
            }
            int moveResult = atomicHashTable2_applyMoveTableOperationInner(this, table, newTable);
            if(moveResult > 0) {
                Reference_Release(&table->newAllocation, newTableRef);
                return moveResult;
            }

            int result = AtomicHashTable2_removeInner(this, hash, callbackContext, callback, newTableRef);
            Reference_Release(&table->newAllocation, newTableRef);
            return result;
        }
        if(valueRef.valueForSet == 0) break;
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
            }
        }

        index = (index + 1) % table->slotsCount;
        if(index == 0) {
            loopCount++;
            if(loopCount > 10) {
                atomicHashTable2_updateReservedUndo(this, table, -1, -1);
                Reference_Release(&table->newAllocation, newTableRef);
                // Too many loops around the table...
                OnError(9);
                return 9;
            }
        }
    }
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
        if(!tableRef) {
            return 0;
        }
        int result = AtomicHashTable2_removeInner(this, hash, callbackContext, callback, tableRef);
        Reference_Release(&this->currentAllocation, tableRef);
        if(result == 1) continue;
        return result;
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