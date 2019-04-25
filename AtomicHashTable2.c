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

int atomicHashTable2_getVersion(AtomicHashTable2* this, uint64_t* versionOut);

todonext
// Destroys newOperation if it can't add it (because of an error, or because the version changed)
//  (and if it adds it takes ownership of newOperation).
int atomicHashTable2_runOperation(AtomicHashTable2* this, uint64_t* versionOut, OutsideReference newOperation);


void destroyUniqueOutsideRef(OutsideReference* ref)  {
    void* temp = Reference_Acquire(&ref);
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


// Not to be called in a retry loop, as it runs its own (but it still may return errors)
//  (also pVersion is an out, so don't get a version before you call this)
// The value it returns is a value with all moves applied, so if the version doesn't change, no moves have happened.
//  (does not return 1)
int atomicHashTable2_updateReservedCount(AtomicHashTable2* this, uint64_t* pVersion, uint64_t delta) {
    InterlockedAdd64((LONG64*)&this->slotsReserved, (LONG64)delta);

    while(true) {
        RETURN_ON_ERROR(atomicHashTable2_getVersion(this, pVersion));

        InsideReference* curAllocRef = Reference_Acquire(&this->state.currentAllocation);
        AtomicHashTableBase* curTable = curAllocRef ? Reference_GetValue(curAllocRef) : nullptr;

        uint64_t slotsReserved = this->slotsReserved;

        uint64_t newSlotCount = 0;
        uint64_t curSlotCount = curTable ? curTable->slotsCount : 0;
        newSlotCount = newShrinkSize(this, curSlotCount, slotsReserved);
        newSlotCount = newSlotCount ? newSlotCount : newGrowSize(this, curSlotCount, slotsReserved);

        if(!newSlotCount) {
            return 0;
        }

        // Create and prepare the new allocation
        OutsideReference newAllocation;
        AtomicHashTableBase* newTable;
        uint64_t tableSize = getTableSize(this, newSlotCount);
        Reference_Allocate(&memPoolSystem, &newAllocation, newTable, tableSize);
        if(!newTable) {
            Reference_Release(&this->state.currentAllocation, curAllocRef);
            return 3;
        }

        newTable->slotsCount = newSlotCount;
        newTable->logSlotsCount = log2(newSlotCount);
        newTable->slots = (void*)((byte*)newTable + sizeof(AtomicHashTableBase));
        newTable->valuePool = (void*)((byte*)newTable->slots + sizeof(AtomicSlot) * newSlotCount);

        for(uint64_t i = 0; i < newSlotCount; i++) {
            newTable->slots[i].value = GetNextNull();
        }

        MemPoolHashed pool = MemPoolHashedDefault(this->VALUE_SIZE, newSlotCount, newAllocation, this, atomicHashTable2_memPoolFreeCallback);
        *newTable->valuePool = pool;

        OutsideReference newOperation = { 0 };
        Operation* operation = nullptr;

        Reference_Allocate(&this->operationPool, &newOperation, &operation, sizeof(Operation), 0);
        if(!operation) {
            Reference_Release(&this->state.currentAllocation, curAllocRef);
            destroyUniqueOutsideRef(&newAllocation);
            return 3;
        }

        operation->type = 1;
        operation->newAllocation = newAllocation;
        operation->moveState.destSlot = GetNextNull();

        //int atomicHashTable2_runOperation(AtomicHashTable2* this, uint64_t* versionOut, OutsideReference newOperation);
        int result = atomicHashTable2_runOperation(this, pVersion, newOperation);

        Reference_Release(&this->state.currentAllocation, curAllocRef);

        if(result != 0) {
            destroyUniqueOutsideRef(&newAllocation);
        }
        if(result == 1) {
            continue;
        }
        return result;
    }
}


int atomicHashTable2_applyMoveTableOperationInner(
    AtomicHashTable2* this,
    OperationRef* operationRef,
    Operation* operation,
    AtomicHashTableBase* curAlloc,
    AtomicHashTableBase* newAlloc
) {
    uint64_t HASH_VALUE_SIZE = this->HASH_VALUE_SIZE;
    uint64_t VALUE_SIZE = this->VALUE_SIZE;

    bool growing = newAlloc->slotsCount > curAlloc->slotsCount;

    MoveStateInner moveState = operation->moveState;
    // Ensure we atomically read 128 bits.
    if(!EqualsStruct128(&moveState, &operation->moveState)) {
        return 1;
    }

    while(true) {
        // Finish the move
        if(!curAlloc || moveState.nextSourceSlot == curAlloc->slotsCount) {
            TransactionState transaction = this->state;
            if(
                !EqualsStruct128(&transaction, &this->state)
                ||
                !FAST_CHECK_POINTER(transaction.currentOperation, operationRef)
            ) {
                return 1;
            }
            // Atomically moves the outside reference from newAllocation to curAlloc,
            //  and finishes the operation.
            TransactionState newTransaction = { operation->newAllocation, GetNextNULL() };
            if(InterlockedCompareExchangeStruct128(&this->state, &transaction, &newTransaction)) {
                // curAlloc has been atomically removed from the hash table, so we inform the real owner of that
                //  reference that at least we no longer need it (but if it has outstanding allocations it will
                //  hang onto the reference until those are freed).
                MemPoolHashed_Destruct(&curAlloc->valuePool);
            }
            return 1;
        }

        AtomicSlot* sourceSlot = &curAlloc->slots[moveState.nextSourceSlot];
        uint32_t sourceSlot = moveState.nextSourceSlot;
        int32_t destOffset = moveState.destOffset;
        
        uint64_t destIndex = ((growing ? moveState.nextSourceSlot * 2 : moveState.nextSourceSlot / 2) + destOffset) % newAlloc->slotsCount;

        OutsideReference newValueRef = { 0 };
        InsideReference* newValueRefInside = Reference_Acquire(&sourceSlot->value);
        
        if(newValueRef) {
            
            if(!MemPoolHashed_IsInPool(&curAlloc->valuePool, newValueRefInside)) {
                // If it ISN'T in the pool, then it must have been already moved, so... there's no point in calling redirect
                Reference_SetOutside(&newValueRef, newValueRefInside);
            } else {
                HashValue* sourceValue = Reference_GetValue(newValueRefInside);
                if(sourceValue->deleted) {
                    Reference_Release(&sourceSlot->value, newValueRefInside);
                }
                else {
                    uint64_t hash = sourceValue->hash;

                    HashValue* newValue = nullptr;
                    Reference_Allocate(newAlloc->valuePool, &newValueRef, &newValue, HASH_VALUE_SIZE, hash);
                    if(!newValue) {
                        Reference_Release(&sourceSlot->value, valueRef);
                        return 3;
                    }

                    destIndex = ((int32_t)getSlotBaseIndex(newAlloc, hash) + destOffset) % newAlloc->slotsCount;
                    newValue->hash = hash;
                    // The delete flag is set in the transaction, so we don't need to worry about this
                    //  copy being atomic. If the delete flag changes our transaction version will change.
                    memcpy(
                        (byte*)newValue + sizeof(HashValue),
                        (byte*)sourceValue + sizeof(HashValue),
                        VALUE_SIZE
                    );

                    InsideReference* newValueInsideRef = Reference_Acquire(&newValueRef);

                    bool redirected = Reference_RedirectReference(
                        newValueRefInside,
                        newValueInsideRef
                    );
                    if(!redirected) {
                        Reference_DestroyOutside(&newValueRef, newValueInsideRef);
                        Reference_Release(&newValueRef, newValueInsideRef);
                        continue;
                    }

                    newValueRefInside = newValueInsideRef;
                }
            }
        }

        

        AtomicSlot* destSlot = &newAllocation->slots[destIndex];

        OutsideReference destValueRef = moveState.destSlot;
        MoveStateInner newMoveState;
        if(destValueRef.valueForSet == 0) {
            destValueRef = destSlot->value;
            newMoveState = moveState;
            newMoveState.destSlot = sourceValueRef;
            if(!InterlockedCompareExchangeStruct128(&operation->moveState, &moveState, &newMoveState)) {
                Reference_DestroyOutside(&newValueRef, newValueRefInside);
                Reference_Release(&newValueRef, newValueRefInside);
                return 1;
            }
            moveState = newMoveState;
        }

        // After this swap works our outside reference is now in the destination.
        int swapFailed = InterlockedCompareExchange64(
            (LONG64*)&newAllocation->slots[destIndex].value
            newValueRef.valueForSet,
            destValueRef.valueForSet
        ) != destValueRef.valueForSet;

        if(swapFailed) {
            Reference_DestroyOutside(&newValueRef, newValueRefInside);
            Reference_Release(&newValueRef, newValueRefInside);
            return 1;
        }

        if(growing) {
            destOffset = max(0, destOffset - 2);
        } else if(sourceSlot % 2) {
            destOffset = max(0, destOffset - 1);
        }

        newMoveState = moveState;
        newMoveState.nextSourceSlot++;
        newMoveState.destOffset = destOffset;
        newMoveState.destSlot = 0;

        if(!InterlockedCompareExchangeStruct128(&operation->moveState, &moveState, &newMoveState)) {
            Reference_Release(&newValueRef, newValueRefInside);
            return 1;
        }


        // Destroy the source reference, so the mem pool can reclaim it (and because we redirected to the new pool,
        //  all the memory stays intact, meaning we have atomically moved it to the new pool, so the destruct callback
        //  won't be called multiple time, even if there are outstanding references to it in hanging finds, etc - 
        //  although, hanging finds WILL prevent the destruct callback from being called in the first place).
        {
            InsideReference* sourceSlotRef = Reference_Acquire(&sourceSlot->value);
            Reference_DestroyOutside(&sourceSlot->value, sourceSlotRef);
            Reference_Release(&sourceSlot->value, sourceSlotRef);
        }

        Reference_Release(&newValueRef, valueRef);
        moveState = newMoveState;
    }

    
    
}
int atomicHashTable2_applyMoveTableOperation(AtomicHashTable2* this, OperationRef* operationRef, Operation* operation, AtomicHashTableBase* currentAllocation) {
    InsideReference* newAllocationRef = Reference_Acquire(&operation->newAllocation);
    if(!newAllocationRef) {
        return 1;
    }
    AtomicHashTableBase* newAllocation = Reference_GetValue(newAllocationRef);
    
    int result = atomicHashTable2_applyMoveTableOperationInner(this, operationRef, operation, currentAllocation, newAllocation);
    Reference_Release(&operation->newAllocation, newAllocationRef);
    return result;
}
int atomicHashTable2_applyRemoveEntryOperation(Operation* operation) {
    todonext
    // This 
}

// TransApply_FastGetVersion
// Returns the version, or 0 if atomicHashTable2_getVersion must be called instead
uint64_t atomicHashTable2_fastGetVersion(AtomicHashTable2* this) {
    OutsideReference curOperation = this->state.currentOperation;
    if(curOperation.isNull) {
        return curOperation.valueForSet;
    }
    return 0;
}

int atomicHashTable2_getVersion(AtomicHashTable2* this, uint64_t* versionOut) {
    uint64_t version;
    while(true) {
        OutsideReference curOperation = this->state.currentOperation;
        if(curOperation.isNull) {
            *versionOut = curOperation.valueForSet;
            return 0;
        }

        InsideReference* curOperationRef = Reference_Acquire(&this->state.currentOperation);
        if(!curOperationRef) {
            continue;
        }
        Operation* operation = Reference_GetValue(curOperationRef);
        InsideReference* currentAllocationRef = Reference_Acquire(&this->state.currentAllocation);

        AtomicHashTableBase* currentAllocation = Reference_GetValue(currentAllocationRef);

        int result;
        if(operation->type == 1) {
            result = atomicHashTable2_applyMoveTableOperation(this, curOperationRef, operation, currentAllocation);
        } else if(operation->type == 2) {
            result = atomicHashTable2_applyRemoveEntryOperation(this, operation, currentAllocation);
        }
        Reference_Release(&this->state.currentAllocation, currentAllocationRef);
        Reference_Release(&this->state.currentOperation, curOperationRef);
        RETURN_ON_ERROR(result);
        // continue
    }
}



int atomicHashTable2_insertInner(AtomicHashTable2* this, uint64_t hash, void* valueVoid, OutsideReference* slotRef, AtomicSlot* slot) {
    
    void* didAllocate = 0;
    Reference_Allocate(0, &slot->valueRef.ref, &didAllocate);
    if(!didAllocate) {
        // Out of memory...
        return 2;
    }
    slot->hash.value = hash;
    slot->valueUniqueId.value = InterlockedIncrement64((LONG64*)&this->nextValueUniqueIdMinus2) + 2;

    byte* value = valueVoid;

    for(uint64_t i = 0; i < this->VALUE_SIZE / 8; i++) {
        (&slot->valueUnits)[i].value = ((uint64_t*)value)[i];
    }
	if (this->VALUE_SIZE % 8 != 0) {
		// Non 8 byte aligned values aren't supported yet.
		OnError(3);
		return 3;
	}


    uint64_t version = 0;
    RETURN_ON_ERROR(atomicHashTable2_updateReservedCount(this, &version, 1));

    uint64_t slotSize = getSlotSize(this);
    while(true) {
        RETURN_ON_ERROR(atomicHashTable2_getVersion(this, &version));

        // Find the destination slot, reserving currentAllocation
        InsideReference* currentRef = Reference_Acquire(&this->currentAllocation.ref);
        if(!currentRef) {
            continue;
        }
        AtomicHashTableBase* table = currentRef->pointer;
        uint64_t indexOffset = 0;
        uint64_t indexStart = getSlotBaseIndex(table, hash);
        while(true) {
            uint64_t searchIndex = (indexStart + indexOffset) % table->slotsCount;
            AtomicSlot* destSlot = (AtomicSlot*)&(&table->slots)[slotSize * searchIndex];
            if(destSlot->valueUniqueId.value <= 1) {
                break;
            }
            
            indexOffset++;
            if(indexOffset == table->slotsCount * 10) {
                // Okay, so... this is possible. The table may only have 1 entry in it, which keeps moving in front of us.
                //  But... it is more likely this is indicative of some error in the internal hash table code.
                return 13;
            }
        }

        uint64_t targetIndex = (indexStart + indexOffset) % table->slotsCount;
        AtomicSlot* slotDest = (AtomicSlot*)&(&table->slots)[slotSize * targetIndex];
        

        Trans transaction = { 0 };
        TransChange emptyChange = { 0 };
        TransChange change = { 0 };

        Reference_SetOutside(&change.sourceRef, currentRef);

        // Can't be null, because... we haven't put it in the hash table yet.
        InsideReference* slotInsideRef = Reference_Acquire(slotRef);
        // TransApply will destroy destRef whether it succeeds or fails
		//	(destRef vs sourceRef doesn't matter, so this might as well be dest, it doesn't have to have anything to do with sourceRefToMove, or sourceUnits)
        Reference_SetOutside(&change.destRef, slotInsideRef);
        // No MemoryPool handling here, as we can't destroy the reference anyway, as change.destRef has a reference to it
        Reference_Release(slotRef, slotInsideRef, false);
        change.destRefMemPool = &this->valueInsertPool;

        change.sourceRefToMove = &slot->valueRef;
        change.destRefToMove = &slotDest->valueRef;

        change.moveCount = slotSize / sizeof(AtomicUnit2) - 1;
        change.sourceUnits = &slot->valueUniqueId;
        change.destUnits = &slotDest->valueUniqueId;

        transaction.changes[transaction.changeCount++] = change;

		Reference_Release(&this->currentAllocation.ref, currentRef, false);

        int result = TransApply_Run(&this->transaction, version, transaction);
        if(result > 1) return result;
        if(result == 1) continue;
        break;
    }

    return 0;
}
int AtomicHashTable2_insert(AtomicHashTable2* this, uint64_t hash, void* valueVoid) {
    uint64_t version;
    RETURN_ON_ERROR(atomicHashTable2_updateReservedCount(this, &version, 1));

    while(true) {
        InsideReference* curAllocRef = Reference_Acquire(&this->currentAllocation);
        AtomicHashTableBase* curAlloc = Reference_GetValue(curAllocRef);

        OutsideReference newValueRef = { 0 };
        HashValue* value = nullptr;
        Reference_Allocate(curAlloc->valuePool, &newValueRef, &value, this->HASH_VALUE_SIZE, hash);
        if(!value) {
            Reference_Release(&this->currentAllocation, curAllocRef);
            return 3;
        }
        memcpy(
            (byte*)value + sizeof(HashValue),
            valueVoid,
            this->VALUE_SIZE
        );

        uint64_t baseInsertIndex = getSlotBaseIndex(curAlloc, hash);
        AtomicSlot* slots = curAlloc->slots;
        OutsideReference prevValue;
        uint64_t cycles = 0;
        while(true) {
            prevValue = slots[baseInsertIndex].value;
            if(!prevValue.pointerClipped) {
                break;
            }
            // TODO: After a certain amount of iterations, or under certain conditions, start looking into values
            //  and seeing if they are deleted, and then replacing them if they are? But then again... who deletes
            //  a value without wiping it out in the table? That should be rare...
            baseInsertIndex = baseInsertIndex + 1;
            if(baseInsertIndex >= curAlloc->slotsCount) {
                baseInsertIndex = 0;
                cycles++;
                if(cycles > 10) {
                    // Somehow after 10 cycles through the table we haven't found a free spot. This must be an error.
                    OnError(9);
                    return 9;
                }
            }
        }

        if(InterlockedCompareExchange64(
            &slots[baseInsertIndex]->value,
            newValueRef,
            prevValue,
        ) != prevValue) {
            Reference_Release(&this->currentAllocation, curAllocRef);
            void* temp = Reference_Acquire(&newValueRef);
            Reference_DestroyOutside(&newValueRef, temp);
            Reference_Release(&newValueRef, temp);
            continue;
        }
        Reference_Release(&this->currentAllocation, curAllocRef);

        todonext
        // Oh wait... if we insert, while resizing... Hmm... maybe inserting does require a transaction? And maybe the move loop does have to be
        //  in the transaction queue?
        if(version == atomicHashTable2_fastGetVersion(this)) {

        }

        
        break;
    }



    //RETURN_ON_ERROR(atomicHashTable2_getVersion(this, &version));

    void* slotMemory = MemoryPool_Allocate(&this->valueInsertPool);
    if(!slotMemory) {
        // Out of memory
        return 2;
    }

    OutsideReference slotRef = { 0 };
    AtomicSlot* slot = nullptr;
    Reference_RecycleAllocate(slotMemory, getSlotSize(this), &slotRef, &slot);

    int result = atomicHashTable2_insertInner(this, hash, valueVoid, &slotRef, slot);

    InsideReference* slotInsideRef = Reference_Acquire(&slotRef);
    Reference_DestroyOutside(&slotRef, slotInsideRef);

    return 0;
}


int atomicHashTable2_removeInner(
	AtomicHashTable2* this,
    InsideReference* tableRef,
    AtomicHashTableBase* table,
    uint64_t index,
    uint64_t* version,
    // If -1, then there is nothing else to remove
    int64_t* nextIndexToRemove
) {
    uint64_t slotSize = getSlotSize(this);

    AtomicSlot* destSlot = (AtomicSlot*)&(&table->slots)[slotSize * index];
    
	// Find an index to swap with
    uint64_t indexStart = index;
    uint64_t swapIndex = indexStart;
    uint64_t indexOffset = 1;
    while(true) {
        uint64_t searchIndex = (indexStart + indexOffset) % table->slotsCount;
        AtomicSlot* afterSlot = (AtomicSlot*)&(&table->slots)[slotSize * searchIndex];
        if(afterSlot->valueUniqueId.value == 0) {
            break;
        }
        // if == 1, then continue, but no need to check for swapping
        if(afterSlot->valueUniqueId.value > 1) {
            uint64_t afterGoalIndex = getSlotBaseIndex(table, afterSlot->hash.value);

            // See if it is a candidate for swapping down.
            if(afterGoalIndex <= indexStart && searchIndex != indexStart) {
                //breakpoint();
                if(searchIndex < indexStart) {
                    breakpoint();
                }
				swapIndex = searchIndex;
                break;
            }
        }
        indexOffset++;
        if(indexOffset == table->slotsCount * 10) {
            // Okay, so... this is possible. The table may only have 1 entry in it, which keeps moving in front of us.
            //  But... it is more likely this is indicative of some error in the internal hash table code.
			OnError(13);
            return 13;
        }
    }


    Trans transaction = { 0 };
    TransChange emptyChange = { 0 };
    TransChange change = { 0 };



	uint64_t hash = 0;
    
    if(swapIndex == indexStart) {

        // Just wipe out indexStart, no need to swap
        change = emptyChange;
        Reference_SetOutside(&change.sourceRef, tableRef);
        change.moveCount = 1;
        change.sourceConstValue = 0;
        change.destUnits = &destSlot->valueUniqueId;

        change.sourceRefToMove = nullptr;
        change.destRefToMove = &destSlot->valueRef;

        /*
        if(destSlot->valueUniqueId.value > 1) {
            // (we could be wiping out a skip entry, and in fact, probably are, as we have to wipe out skip entries somehow!)
            
            byte* valueToDelete = MemoryPool_Allocate(&this->valueFindPool);

            for(uint64_t i = 0; i < this->VALUE_SIZE / 8; i++) {
                (&destSlot->valueUnits)[i].value = ((uint64_t*)value)[i];
            }
        }
        */

        transaction.changes[transaction.changeCount++] = change;

        *nextIndexToRemove = -1;

		hash = destSlot->hash.value;
    } else {
        // Swap swapIndex down to indexStart, marking the original swapIndex as valueUniqueId.value == 1
        //  (and freeing the dest if it had a value > 1)

        AtomicSlot* sourceSlot = (AtomicSlot*)&(&table->slots)[slotSize * swapIndex];

        // Move the value (TransApply duplicates the ref properly)
        change = emptyChange;
        Reference_SetOutside(&change.sourceRef, tableRef);
        change.sourceRefToMove = &sourceSlot->valueRef;
        change.destRefToMove = &destSlot->valueRef;
        change.moveCount = slotSize / sizeof(AtomicUnit2) - 1;
        change.sourceUnits = &sourceSlot->valueUniqueId;
        change.destUnits = &destSlot->valueUniqueId;
        transaction.changes[transaction.changeCount++] = change;

        // Mark the old value as not being used
        change = emptyChange;
        Reference_SetOutside(&change.sourceRef, tableRef);

        // Wipe out the source ref
        change.sourceRefToMove = nullptr;
        change.destRefToMove = &sourceSlot->valueRef;

        change.moveCount = 1;
        change.sourceConstValue = 1;
        change.destUnits = &sourceSlot->valueUniqueId;
        transaction.changes[transaction.changeCount++] = change;

        *nextIndexToRemove = swapIndex;
    }

	if (swapIndex == indexStart) {
		for (uint16_t testIndex = 0; testIndex < table->slotsCount; testIndex++) {
			AtomicSlot* testSlot = (AtomicSlot*)&(&table->slots)[slotSize * testIndex];
			if (testSlot->valueUniqueId.value <= 1) continue;
			uint64_t hash = testSlot->hash.value;
			uint64_t baseIndex = getSlotBaseIndex(table, hash);
			if (testIndex == baseIndex) continue;

			int64_t offset = -1;
			while (true) {
				uint64_t index = ((uint64_t)((int64_t)testIndex + offset + (int64_t)table->slotsCount)) % table->slotsCount;

				AtomicSlot* beforeSlot = (AtomicSlot*)&(&table->slots)[slotSize * index];
				if (beforeSlot->valueUniqueId.value == 0) {

					// Invalid, hole before or at base index
					breakpoint();
				}

				if (index == baseIndex) break;
				offset--;
			}
		}
	}

    int result = TransApply_Run(&this->transaction, *version, transaction);
    if(result == 0) {
		if (swapIndex == indexStart) {
			//printf("Wiped out at %llu, hash %p, searched %llu\n", swapIndex, hash, indexOffset);
		}
        for(uint16_t testIndex = 0; testIndex < table->slotsCount; testIndex++) {
            AtomicSlot* testSlot = (AtomicSlot*)&(&table->slots)[slotSize * testIndex];
            if(testSlot->valueUniqueId.value <= 1) continue;
			uint64_t hash = testSlot->hash.value;
            uint64_t baseIndex = getSlotBaseIndex(table, hash);
			if (testIndex == baseIndex) continue;

            int64_t offset = -1;
            while(true) {
                uint64_t index = ((uint64_t)((int64_t)testIndex + offset + (int64_t)table->slotsCount)) % table->slotsCount;

                AtomicSlot* beforeSlot = (AtomicSlot*)&(&table->slots)[slotSize * index];
                if(beforeSlot->valueUniqueId.value == 0) {
					
                    // Invalid, hole before or at base index
                    breakpoint();
                }

                if(index == baseIndex) break;
                offset--;
            }
        }

		// Eh... not great, but saves some time, as if there is no contention, and it has been applied, the version will be exactly one more,
		//	which allows the outer loop to finish without retrying (when there is no contention).
		//*version = *version + 1;
    }
    return result;
}


int atomicHashTable2_removeLoop(
    AtomicHashTable2* this,
	uint64_t hash,
	void* callbackContext,
	// On true, removes the value from the table
	bool(*callback)(void* callbackContext, void* value),
    AtomicHashTableBase* table,
    InsideReference* tableRef,
    uint64_t version,
    byte* value
) {
    uint64_t slotSize = getSlotSize(this);

    uint64_t indexStart = getSlotBaseIndex(table, hash);
    uint64_t indexOffset = 0;
    while(version == this->transaction.transactionToApplyUnit.version) {
        uint64_t index = (indexStart + indexOffset++) % table->slotsCount;
        AtomicSlot* slot = (AtomicSlot*)&(&table->slots)[slotSize * index];
        if(slot->valueUniqueId.value == 0) break;
        if(slot->valueUniqueId.value == 1) continue;
        if(slot->hash.value != hash) continue;
        
        InsideReference* valueRef = Reference_Acquire(&slot->valueRef.ref);
        if(!valueRef) {
            // Eh... must be a contention  we will surely retry later
            continue;
        }

        for(uint64_t i = 0; i < this->VALUE_SIZE / 8; i++) {
            ((uint64_t*)value)[i] = (&slot->valueUnits)[i].value;
        }
		if (this->VALUE_SIZE % 8 != 0) {
			// Non 8 byte aligned values aren't supported yet.
			OnError(3);
			return 3;
		}
		

        if(version != this->transaction.transactionToApplyUnit.version) {
            // Writes have occured since we started reading, so our read is not consistent. Retry
            Reference_Release(&slot->valueRef.ref, valueRef, false);
            break;
        }

        bool shouldRemove = callback(callbackContext, value);

        //todonext
        // Oh... when the last reference to slot->ref.ref is released we need to call this->deleteValue on the full value...

        // TODO: Wait, we need to copy the value out, and call this->deleteValue on it.
        //values = MemoryPool_Allocate(&this->valueFindPool);

        Reference_Release(&slot->valueRef.ref, valueRef, false);

        if(shouldRemove) {
            int64_t nextIndexToRemove = index;
            while(nextIndexToRemove != -1) {
                int result = atomicHashTable2_removeInner(this, tableRef, table, nextIndexToRemove, &version, &nextIndexToRemove);
                if(result > 1) return result;
                if(result == 1) {
                    // Retrying in the inner loop here is way too complicated, and requires rechecking a lot of stuff.
                    //  Breaking lets us maybe leave hanging valueUniqueId == 1 values, which should be fine...
                    break;
                }
            }

			// We might have moved something into the slot, and we might need to remove that, so... recheck the index,
			//	IF we moved something into it
			if (slot->valueUniqueId.value != 0) {
				indexOffset--;
			}
        }
    }

    return 0;
}



// REMEMBER! When swapping to accomplish a move, if a location we marked as skipped becomes something else,
//  then we are done! Because it means something has been inserted there, or someone else has swapped or whatever.
//  Also, if a data move happens (so the location we marked as skipped is no longer valid), then we are done too.
// REMEMBER! When setting a value to be skipped, or just wiping it out, we have to free its AtomicSlot.ref
//  (after we marked it as wiped out)
//  - I guess we should just atomically swap it with nullptr
int AtomicHashTable2_remove(
	AtomicHashTable2* this,
	uint64_t hash,
	void* callbackContext,
	// On true, removes the value from the table
	bool(*callback)(void* callbackContext, void* value)
) {   
    // Iterate, using callback, and... we have to restart our iteration if version changes, as values can swap down
    //  (because we swap values down). Which should be fine, we should only have to iterate over a few values.
    // Also, we can iterate and check with the find logic, allowing us to get references easily.

    void* valueTempMemory = MemoryPool_Allocate(&this->valueFindPool);
    if(!valueTempMemory) {
        // Out of memory...
        return 2;
    }
    byte* value = valueTempMemory;

    int result = 0;

	uint64_t version = 0;
	RETURN_ON_ERROR(atomicHashTable2_updateReservedCount(this, &version, -1));

    while(true) {
        RETURN_ON_ERROR(atomicHashTable2_getVersion(this, &version));

        InsideReference* tableRef = Reference_Acquire(&this->currentAllocation.ref);
        if(!tableRef) {
            continue;
        }
        AtomicHashTableBase* table = tableRef->pointer;

        result = atomicHashTable2_removeLoop(
            this,
            hash,
            callbackContext,
            callback,
            table,
            tableRef,
            version,
            value
        );

        Reference_Release(&this->currentAllocation.ref, tableRef, false);

        if(result > 1) {
            break;
        }
        if(version != this->transaction.transactionToApplyUnit.version) {
            continue;
        }
        break;
    }

    MemoryPool_Free(&this->valueFindPool, valueTempMemory);

    return result;
}

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
    uint64_t slotSize = getSlotSize(this);

    uint64_t valueCount = 0;
    uint64_t lastValueCount = valueCount;
    uint64_t valuesUsed = 0;
	OutsideReference* valueReferences = nullptr;
	byte* values = nullptr;

	OutsideReference valueReferencesOne[1];

    while(true) {

		uint64_t version = atomicHashTable2_fastGetVersion(this);
		if (!version) {
			RETURN_ON_ERROR(atomicHashTable2_getVersion(this, &version));
		}

        InsideReference* tableRef = Reference_Acquire(&this->currentAllocation.ref);
        if(!tableRef) {
            continue;
        }
        AtomicHashTableBase* table = tableRef->pointer;

        {
            uint64_t indexStart = getSlotBaseIndex(table, hash);
            uint64_t indexOffset = 0;
            while(version == this->transaction.transactionToApplyUnit.version) {
                uint64_t index = (indexStart + indexOffset++) % table->slotsCount;
                AtomicSlot* slot = (AtomicSlot*)&(&table->slots)[slotSize * index];
                if(slot->valueUniqueId.value == 0) break;
                if(slot->valueUniqueId.value == 1) continue;
                if(slot->hash.value != hash) continue;
                
                if(valuesUsed + 1 > valueCount) {
                    if (valueCount == 0) {
                        valueCount = 1;
                    }
                    else {
                        // Need more values. No point in adding extra iterator code, just multiple by 10. If they are
                        //  expecting find to return many elements... then they probably shouldn't be using a hash table anyway...
                        valueCount = valueCount * 10;
                    }
                    break;
                }

                {
                    InsideReference* valueRef = Reference_Acquire(&slot->valueRef.ref);
                    if(!valueRef) {
                        // Eh... must be a contention we will surely retry later
                        continue;
                    }

                    Reference_SetOutside(&valueReferences[valuesUsed], valueRef);
                    Reference_Release(&slot->valueRef.ref, valueRef, false);
                }

                byte* value = &values[this->VALUE_SIZE * (valuesUsed)];

                for(uint64_t i = 0; i < this->VALUE_SIZE / 8; i++) {
                    ((uint64_t*)value)[i] = (&slot->valueUnits)[i].value;
                }
				if (this->VALUE_SIZE % 8 != 0) {
					// Non 8 byte aligned values aren't supported yet.
					OnError(3);
					return 3;
				}

                if(version != this->transaction.transactionToApplyUnit.version) {
                    // Writes have occured since we started reading, so our read is not consistent. Retry
                    break;
                }

                valuesUsed++;
            }
        }

        Reference_Release(&this->currentAllocation.ref, tableRef, false);

        if(valueCount != lastValueCount) {
			if (valueCount == 1) {
                // Nothing to free in here, as if valueCount is 1 now, then previous it must have been zero.
				OutsideReference zeroRef = { 0 };
				valueReferencesOne[0] = zeroRef;

				valueReferences = valueReferencesOne;
				values = MemoryPool_Allocate(&this->valueFindPool);
				if (!values) {
					// Out of memory...
					return 2;
				}
			}
			else {
				for (uint64_t i = 0; i < valuesUsed; i++) {
					InsideReference* valRef = Reference_Acquire(&valueReferences[i]);
					Reference_DestroyOutside(&valueReferences[i], valRef);
					Reference_Release(nullptr, valRef, false);
				}

				if (lastValueCount == 1) {
					MemoryPool_Free(&this->valueFindPool, values);
				}
				else {
					free(values);
					free(valueReferences);
				}

				values = malloc(this->VALUE_SIZE * valueCount);
				valueReferences = malloc(sizeof(OutsideReference) * valueCount);

                if(!values || !valueReferences) {
                    if(values) {
                        values = nullptr;
                        free(values);
                    }
                    if(valueReferences) {
                        valueReferences = nullptr;
                        free(valueReferences);
                    }

                    return 2;
                }
			}
            lastValueCount = valueCount;
            memset(values, 0, (int)(this->VALUE_SIZE * valueCount));
			memset(valueReferences, 0, (int)(sizeof(OutsideReference) * valueCount));
            valuesUsed = 0;
            continue;
        }

        // This version check should catch all result == 1 cases anyway, so we don't need to check for it.
        if(version != this->transaction.transactionToApplyUnit.version) {
            memset(values, 0, (int)(this->VALUE_SIZE * valueCount));
			memset(valueReferences, 0, (int)(sizeof(OutsideReference) * valueCount));
            valuesUsed = 0;
            continue;
        }

        break;
    }

    
    for(uint64_t i = 0; i < valuesUsed; i++) {
        callback(callbackContext, (void*)&values[this->VALUE_SIZE * i]);
    }

    for(uint64_t i = 0; i < valuesUsed; i++) {
		if (valueReferences[i].value) {
			InsideReference* valRef = Reference_Acquire(&valueReferences[i]);
			Reference_DestroyOutside(&valueReferences[i], valRef);
			Reference_Release(nullptr, valRef, false);
		}
    }

	if (lastValueCount == 0) {

	}
    else if(lastValueCount == 1) {
        MemoryPool_Free(&this->valueFindPool, values);
    } else {
        free(values);
        free(valueReferences);
    }

    return 0;
}


uint64_t DebugAtomicHashTable2_reservedSize(AtomicHashTable2* self) {
    return self->slotsReserved;
}
uint64_t DebugAtomicHashTable2_allocationSize(AtomicHashTable2* this) {
    InsideReference* curRef = Reference_Acquire(&this->currentAllocation.ref);
    if(!curRef) {
        return 0;
    }
    AtomicHashTableBase* table = curRef->pointer;
    return table->slotsCount;
}