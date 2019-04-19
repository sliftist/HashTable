#include "AtomicHashTable2.h"

#include "bittricks.h"
#include "Timing.h"

#define RETURN_ON_ERROR(x) { int returnOnErrorResult = x; if(returnOnErrorResult != 0) return returnOnErrorResult; }

// (sizeof(InsideReference) because our tracked allocations are this much smaller)

#define getSlotSize(hashTable) AtomicHashTableSlotSize(hashTable->VALUE_SIZE)

// Means when we add more bits, overlaps stay as neighbors (which is a useful property when resizing)
#define getSlotBaseIndex(alloc, hash) (hash >> (64 - alloc->logSlotsCount))


uint64_t log2RoundDown(uint64_t v) {
	return log2(v * 2) - 1;
}

uint64_t minSlotCount(AtomicHashTable2* this) {
    uint64_t slotSize = getSlotSize(this);
    uint64_t minCount = (PAGE_SIZE - (sizeof(AtomicHashTableBase) - sizeof(byte*)) - sizeof(InsideReference)) / slotSize;

    // Must be a power of 2, for getSlotBaseIndex
    minCount = 1ll << log2RoundDown(minCount);

    return minCount;
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

// Not to be called in a retry loop, as it runs its own (but it still may return errors)
//  (also pVersion is an out, so don't get a version before you call this)
int atomicHashTable2_updateReservedCount(AtomicHashTable2* this, uint64_t* pVersion, uint64_t delta) {
    uint64_t slotsReserved = InterlockedAdd64((LONG64*)&this->slotsReserved, (LONG64)delta);

    while(true) {
        RETURN_ON_ERROR(atomicHashTable2_getVersion(this, pVersion));
        InsideReference* curAllocRef = Reference_Acquire(&this->currentAllocation.ref);
        AtomicHashTableBase* curTable = curAllocRef ? curAllocRef->pointer : nullptr;

        uint64_t newSlotCount = 0;
        uint64_t curSlotCount = curTable ? curTable->slotsCount : 0;
        newSlotCount = newShrinkSize(this, curSlotCount, slotsReserved);
        newSlotCount = newSlotCount ? newSlotCount : newGrowSize(this, curSlotCount, slotsReserved);

        Reference_Release(&this->currentAllocation.ref, curAllocRef, false);

		if (!newSlotCount) {
			return 0;
		}

        Trans transaction = { 0 };
        TransChange emptyChange = { 0 };
        TransChange change = { 0 };

        change.moveCount = 1;
        change.sourceUnits = 0;
        change.destUnits = &this->nextMoveSlot;
        transaction.changes[transaction.changeCount++] = change;

        change = emptyChange;
        change.moveCount = 1;
        change.sourceUnits = 0;
        change.destUnits = &this->nextDestSlot;
        transaction.changes[transaction.changeCount++] = change;

        
        AtomicHashTableBase* newTable = nullptr;
        // Reference_Allocate memsets allocations to 0
        Reference_Allocate(
            newSlotCount * getSlotSize(this) + sizeof(AtomicHashTableBase) - sizeof(byte*),
            &change.sourceRef,
            &newTable
        );

        OutsideReference newTableRef = { 0 };
        InsideReference* newTableInsideRef = Reference_Acquire(&change.sourceRef);
        Reference_SetOutside(&newTableRef, newTableInsideRef);
        Reference_Release(&change.sourceRef, newTableInsideRef, false);

        if(!newTable) {
            // Allocation failed
            OnError(2);
            return 2;
        }
        newTable->slotsCount = newSlotCount;
        newTable->logSlotsCount = log2(newSlotCount);

        change = emptyChange;
        change.moveCount = 1;
        change.sourceConstValue = newTableRef.value;
        change.destUnits = (AtomicUnit2*)&this->newAllocation;
        transaction.changes[transaction.changeCount++] = change;

        
        int result = TransApply_Run(&this->transaction, *pVersion, transaction);
        if(result == 1) continue;
        return result;
    }
}

int atomicHashTable2_applyMoveInner(
    AtomicHashTable2* this,
    uint64_t version,
    InsideReference* newAllocRef,
    AtomicHashTableBase* newAlloc,
    InsideReference* curAllocRef,
    AtomicHashTableBase* curAlloc
) {

    uint64_t nextMoveSlotIndex = this->nextMoveSlot.value;

    uint64_t slotSize = getSlotSize(this);
    while(curAlloc && nextMoveSlotIndex < curAlloc->slotsCount) {
        
        Trans transaction = { 0 };
        TransChange emptyChange = { 0 };
        TransChange change = { 0 };

        AtomicSlot* slotSource = (AtomicSlot*)&(&curAlloc->slots)[nextMoveSlotIndex * slotSize];
        if(slotSource->valueUniqueId.value > 1) {
            uint64_t destSlotIndex = getSlotBaseIndex(newAlloc, slotSource->hash.value);
            if(destSlotIndex < this->nextDestSlot.value) {
                destSlotIndex = this->nextDestSlot.value;
            }
            uint64_t realDestSlotIndex = destSlotIndex;
            // Logic to deal with shrinking tables having collisions at the end, which force them to wrap around.
            if(destSlotIndex >= newAlloc->slotsCount) {
                realDestSlotIndex = realDestSlotIndex % newAlloc->slotsCount;
                while(((AtomicSlot*)&(&newAlloc->slots)[realDestSlotIndex * slotSize])->valueUniqueId.value > 1) {
                    realDestSlotIndex++;
					nextMoveSlotIndex++;
                }
            }
            AtomicSlot* slotDest = (AtomicSlot*)&(&newAlloc->slots)[realDestSlotIndex * slotSize];

            //todonext
            // First of all, only move values that have values.
            // Second of all, don't cause overlaps when moving.
            //	 Hmm... this is why I didn't want to do mod. If we add most significant bit, it means that overlaps either continue to overlap, OR become really far apart...

            Reference_SetOutside(&change.sourceRef, curAllocRef);
            Reference_SetOutside(&change.destRef, newAllocRef);

            change.sourceRefToMove = &slotSource->ref;
            change.destRefToMove = &slotDest->ref;

            // Move all the data in one big move (not really faster, we still have to iterate through it in TransApply, but it is easier)
            //  (except sourceRefToMove, which we move independently to allow references to be added without transactions)
            change.moveCount = slotSize / sizeof(AtomicUnit2) - 1;
            change.sourceUnits = &slotSource->valueUniqueId;
            change.destUnits = &slotDest->valueUniqueId;

            transaction.changes[transaction.changeCount++] = change;

			if (destSlotIndex >= this->nextDestSlot.value) {
				change = emptyChange;
				change.moveCount = 1;
				change.sourceConstValue = destSlotIndex + 1;
				change.destUnits = &this->nextDestSlot;
				transaction.changes[transaction.changeCount++] = change;
			}
        }

        change = emptyChange;
        nextMoveSlotIndex++;
        change.moveCount = 1;
        change.sourceConstValue = nextMoveSlotIndex;
        change.destUnits = &this->nextMoveSlot;
        transaction.changes[transaction.changeCount++] = change;


        RETURN_ON_ERROR(TransApply_Run(&this->transaction, version, transaction));
		
		version = TransApply_GetVersion(&this->transaction);
		if (this->newAllocation.ref.pointerClipped != (uint64_t)newAllocRef) {
			return 1;
		}
		nextMoveSlotIndex = this->nextMoveSlot.value;
    }

    // Finish up the move
    {
        Trans transaction = { 0 };
        TransChange emptyChange = { 0 };
        TransChange change = { 0 };

        // Must do this seperately, as refs are moved last, so if we wipe out newAllocation first we won't be able to move it...
        change.moveCount = 0;
        change.sourceRefToMove = &this->newAllocation;
        change.destRefToMove = &this->currentAllocation;
        transaction.changes[transaction.changeCount++] = change;

        change = emptyChange;
        change.moveCount = 1;
        change.sourceUnits = nullptr;
        change.destUnits = (AtomicUnit2*)&this->newAllocation;
        transaction.changes[transaction.changeCount++] = change;

        return TransApply_Run(&this->transaction, version, transaction);
    }
}
int atomicHashTable2_applyMove(AtomicHashTable2* this, uint64_t version) {
    // This check is an optimization
    if(!this->newAllocation.ref.pointerClipped) {
        return 0;
    }
    
    InsideReference* newAllocRef = Reference_Acquire(&this->newAllocation.ref);
    AtomicHashTableBase* newAlloc = newAllocRef ? newAllocRef->pointer : nullptr;
    if(!newAllocRef) {
        return 1;
    }
    InsideReference* curAllocRef = Reference_Acquire(&this->currentAllocation.ref);
    AtomicHashTableBase* curAlloc = curAllocRef ? curAllocRef->pointer : nullptr;

    int result = atomicHashTable2_applyMoveInner(this, version, newAllocRef, newAlloc, curAllocRef, curAlloc);

    Reference_Release(&this->newAllocation.ref, newAllocRef, false);
    Reference_Release(&this->currentAllocation.ref, curAllocRef, false);

    return result;
}

// TransApply_FastGetVersion
// Returns the version, or 0 if atomicHashTable2_getVersion must be called instead
uint64_t atomicHashTable2_fastGetVersion(AtomicHashTable2* this) {
    if(this->newAllocation.ref.pointerClipped) {
        return 0;
    }
    return TransApply_FastGetVersion(&this->transaction);
}

int atomicHashTable2_getVersion(AtomicHashTable2* this, uint64_t* versionOut) {
    while(true) {
        // This also applies any pending transactions, which is nice...
        uint64_t version = TransApply_GetVersion(&this->transaction);
        *versionOut = version;
        int result = atomicHashTable2_applyMove(this, version);
        if(result == 0) {
            // Apply any last changes?
            *versionOut = TransApply_GetVersion(&this->transaction);
        }
        if(result != 1) {
            return result;
        }
    }
}



int atomicHashTable2_insertInner(AtomicHashTable2* this, uint64_t hash, void* valueVoid, OutsideReference* slotRef, AtomicSlot* slot) {
    
    void* didAllocate = 0;
    Reference_Allocate(0, &slot->ref.ref, &didAllocate);
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
    uint64_t alignedEnd = this->VALUE_SIZE / 8 * 8;
    for(uint64_t i = alignedEnd; i < this->VALUE_SIZE; i++) {
        ((byte*)(&slot->valueUnits)[i].value)[i - alignedEnd] = value[i];
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

        change.sourceRefToMove = &slot->ref;
        change.destRefToMove = &slotDest->ref;

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
    void* slotMemory = MemoryPool_Allocate(&this->valueInsertPool);
    if(!slotMemory) {
        // Out of memory
        return 2;
    }

    OutsideReference slotRef = { 0 };
    AtomicSlot* slot = nullptr;
    Reference_RecycleAllocate(slotMemory, getSlotSize(this), &slotRef, &slot);

    TimeBlock(insertInner,
    int result = atomicHashTable2_insertInner(this, hash, valueVoid, &slotRef, slot);
    );

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
            if(afterGoalIndex <= indexStart) {
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

    AtomicSlot slotRefRemoved = { 0 };
    
    // Hmm... I may have miscounted some references here. It gets a bit complicated...
    if(swapIndex == indexStart) {
        slotRefRemoved = *destSlot;

        // Just wipe out indexStart, no need to swap
        change = emptyChange;
        change.moveCount = 1;
        change.sourceConstValue = 0;
        change.destUnits = &destSlot->valueUniqueId;
        transaction.changes[transaction.changeCount++] = change;

        // Wipe out the dest ref (we destroy the ref properly if this succeeds)
        change = emptyChange;
        Reference_SetOutside(&change.sourceRef, tableRef);
        change.moveCount = 1;
        change.sourceConstValue = 0;
        change.destUnits = (AtomicUnit2*)&destSlot->ref;
        transaction.changes[transaction.changeCount++] = change;

        *nextIndexToRemove = -1;
    } else {
        // Swap swapIndex down to indexStart, marking the original swapIndex as valueUniqueId.value == 1
        //  (and freeing the dest if it had a value > 1)

        AtomicSlot* sourceSlot = (AtomicSlot*)&(&table->slots)[slotSize * swapIndex];

        // Move the value (TransApply duplicates the ref properly)
        change = emptyChange;
        change.sourceRefToMove = &sourceSlot->ref;
        change.destRefToMove = &destSlot->ref;
        change.moveCount = slotSize / sizeof(AtomicUnit2) - 1;
        change.sourceUnits = &sourceSlot->valueUniqueId;
        change.destUnits = &destSlot->valueUniqueId;
        transaction.changes[transaction.changeCount++] = change;

        // Mark the old value as not being used
        change = emptyChange;
        change.moveCount = 1;
        change.sourceConstValue = 1;
        change.destUnits = &sourceSlot->valueUniqueId;
        transaction.changes[transaction.changeCount++] = change;

        // Wipe out the source ref
        change = emptyChange;
        Reference_SetOutside(&change.sourceRef, tableRef);
        change.moveCount = 1;
        change.sourceConstValue = 0;
        change.destUnits = (AtomicUnit2*)&sourceSlot->ref;
        transaction.changes[transaction.changeCount++] = change;

        *nextIndexToRemove = swapIndex;
    }


    int result = TransApply_Run(&this->transaction, *version, transaction);
    if(result == 0) {
		// Eh... not great, but saves some time, as if there is no contention, and it has been applied, the version will be exactly one more,
		//	which allows the outer loop to finish without retrying (when there is no contention).
		*version = *version + 1;

        // If we wiped out a slot, (and it was a value with a valueId > 1), so we need to destroy the reference.
        if(slotRefRemoved.valueUniqueId.value > 1) {
            InsideReference* slotRefInner = Reference_Acquire(&slotRefRemoved.ref.ref);
            Reference_DestroyOutside(&slotRefRemoved.ref.ref, slotRefInner);
            Reference_Release(&slotRefRemoved.ref.ref, slotRefInner, false);
        }
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
        
        InsideReference* valueRef = Reference_Acquire(&slot->ref.ref);
        if(!valueRef) {
            // Eh... must be a contention  we will surely retry later
            continue;
        }

        for(uint64_t i = 0; i < this->VALUE_SIZE / 8; i++) {
            ((uint64_t*)value)[i] = (&slot->valueUnits)[i].value;
        }
        uint64_t alignedEnd = this->VALUE_SIZE / 8 * 8;
        for(uint64_t i = alignedEnd; i < this->VALUE_SIZE; i++) {
            value[i] = ((byte*)(&slot->valueUnits)[i].value)[i - alignedEnd];
        }

        if(version != this->transaction.transactionToApplyUnit.version) {
            // Writes have occured since we started reading, so our read is not consistent. Retry
            Reference_Release(&slot->ref.ref, valueRef, false);
            break;
        }

        bool shouldRemove = callback(callbackContext, value);
        Reference_Release(&slot->ref.ref, valueRef, false);

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

        if(result != 1) {
            break;
        }
        if(version != this->transaction.transactionToApplyUnit.version) {
            continue;
        }
    }

    MemoryPool_Free(&this->valueFindPool, valueTempMemory);

    return result;
}


// NOTE: we have to copy the values out, and store all of them independently, because once we start calling the callback
//  we can't go back and retry, unless we keep track of the values we passed and don't pass them again... but then that would
//  require a fast lookup structure, which is what we are building in the first place. Unless of course there are few values,
//  in which case copying all of them should be fine... unless they are large values. But if they are large values,
//  and there is enough contention, then the user shouldn't be storing them inline anyway! They should store a pointer
//  in our table, making inserts and removals faster, and finds faster!
int atomicHashTable2_findLoopInner(
    AtomicHashTable2* this,
	uint64_t hash,
    AtomicHashTableBase* table,
    InsideReference* tableRef,
    uint64_t version,
    // If increased, we get called again with a higher valueCount
    uint64_t* valueCount,
    uint64_t* valuesUsed,
    byte* values,
    // All of these are released on retries
    OutsideReference* valueReferences
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
        
        if(*valuesUsed + 1 > *valueCount) {
			if (*valueCount == 0) {
				*valueCount = 1;
			}
			else {
				// Need more values. No point in adding extra iterator code, just multiple by 10. If they are
				//  expecting find to return many elements... then they probably shouldn't be using a hash table anyway...
				*valueCount = *valueCount * 10;
			}
            return 1;
        }

        {
            InsideReference* valueRef = Reference_Acquire(&slot->ref.ref);
            if(!valueRef) {
                // Eh... must be a contention we will surely retry later
                continue;
            }

            Reference_SetOutside(&valueReferences[*valuesUsed], valueRef);
            Reference_Release(&slot->ref.ref, valueRef, false);
        }

        byte* value = &values[this->VALUE_SIZE * (*valuesUsed)];

        for(uint64_t i = 0; i < this->VALUE_SIZE / 8; i++) {
            ((uint64_t*)value)[i] = (&slot->valueUnits)[i].value;
        }
        uint64_t alignedEnd = this->VALUE_SIZE / 8 * 8;
        for(uint64_t i = alignedEnd; i < this->VALUE_SIZE; i++) {
            value[i] = ((byte*)(&slot->valueUnits)[i].value)[i - alignedEnd];
        }

        if(version != this->transaction.transactionToApplyUnit.version) {
            // Writes have occured since we started reading, so our read is not consistent. Retry
            return 1;
        }

        (*valuesUsed)++;
    }

    return 0;
}

int atomicHashTable2_findLoop(
    AtomicHashTable2* this,
	uint64_t hash,
    uint64_t version,
    // If increased, we get called again with a higher valueCount
    uint64_t* valueCount,
    uint64_t* valuesUsed,
    byte* values,
    // All of these are released on retries
    OutsideReference* valueReferences
) {
    InsideReference* tableRef = Reference_Acquire(&this->currentAllocation.ref);
    if(!tableRef) {
        return 1;
    }
    AtomicHashTableBase* table = tableRef->pointer;

    int result = atomicHashTable2_findLoopInner(
        this,
        hash,
        table,
        tableRef,
        version,
        valueCount,
        valuesUsed,
        values,
        valueReferences
    );

    Reference_Release(&this->currentAllocation.ref, tableRef, false);

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
    uint64_t valueCount = 0;
    uint64_t lastValueCount = valueCount;
    uint64_t valuesUsed = 0;
	OutsideReference* valueReferences = nullptr;
	byte* values = nullptr;

    OutsideReference valueReferencesOne[1] = { 0 };

    int result = 0;

    while(true) {

		uint64_t version = atomicHashTable2_fastGetVersion(this);
		if (!version) {
			RETURN_ON_ERROR(atomicHashTable2_getVersion(this, &version));
		}

        int result = atomicHashTable2_findLoop(
            this,
            hash,
            version,
            &valueCount,
            &valuesUsed,
            values,
            valueReferences
        );

        if(valueCount != lastValueCount) {
			if (valueCount == 1) {
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
			}
            lastValueCount = valueCount;
        }
		if (result != 0) {
			memset(values, 0, (int)(this->VALUE_SIZE * valueCount));
			memset(valueReferences, 0, (int)(sizeof(OutsideReference) * valueCount));
            valuesUsed = 0;
		}

        if(result != 1) break;
        // This version check should catch all result == 1 cases anyway, so we don't need to check for it.
        if(version != this->transaction.transactionToApplyUnit.version) {
            continue;
        }
    }

    if(result == 0) {
        for(uint64_t i = 0; i < valuesUsed; i++) {
            callback(callbackContext, (void*)&values[this->VALUE_SIZE * i]);
        }
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

    return result;
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