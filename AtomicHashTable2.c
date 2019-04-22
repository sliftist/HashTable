#include "AtomicHashTable2.h"

#include "bittricks.h"
#include "Timing.h"

#define RETURN_ON_ERROR(x) { int returnOnErrorResult = x; if(returnOnErrorResult != 0) return returnOnErrorResult; }

// (sizeof(InsideReference) because our tracked allocations are this much smaller)

#define getSlotSize(hashTable) (hashTable)->SLOT_SIZE

// Means when we add more bits, overlaps stay as neighbors (which is a useful property when resizing)
#define getSlotBaseIndex(alloc, hash) ((hash) >> (64 - (alloc)->logSlotsCount))


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
        if(!newTable) {
            // Allocation failed
            OnError(2);
            return 2;
        }

        OutsideReference newTableRef = { 0 };
        InsideReference* newTableInsideRef = Reference_Acquire(&change.sourceRef);
        Reference_SetOutside(&newTableRef, newTableInsideRef);
        Reference_Release(&change.sourceRef, newTableInsideRef, false);

        
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
    //printf("applying move %llu to %llu\n", curAlloc ? curAlloc->slotsCount : 0, newAlloc->slotsCount);
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

            Reference_SetOutside(&change.sourceRef, curAllocRef);
            Reference_SetOutside(&change.destRef, newAllocRef);

            change.sourceRefToMove = &slotSource->valueRef;
            change.destRefToMove = &slotDest->valueRef;

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

/*
int atomicHashTable2_copyAndAllocateValue(AtomicHashTable2* this, AtomicSlot* slot, void** value) {
    
}
int atomicHashTable2_freeValue(AtomicHashTable2* this, void** value) {
    
}
*/

//todonext
// Hmm... if we could embed memory pools in inside references... that would make both 10 times better...
// And maybe... if memory pools existed inside of slots... which would require the memory pool to reference
//  the table, which would mean moving the slots would require atomically moving the inside references... which
//  requires them to be interlaced... unless..
//  Maybe inside references could be chained in someway?
//      - Well.. that really just gets into how to move ref counted memory without interlacing? Which... I think is possible...

// Moving ref counted memory without interlacing
//  - So... the OutsideReference controls access to the InsideReference.
//  - Hmm... maybe, we have two ref counts inside the InsideReference? Or maybe, an InsideReference starts with an
//      OutsideReference? Which if exists, is accessed as opposed to the original pointer? And then, when the
//      first InsideReference is finally destroyed, instead of freeing its value, you just destroy the OutsideReference?
//      - So leaking references will cause increasingly long chains of OutsideReferences? And shrinking these chains...
//          is inherently difficult...
//          - We could have the chains two way link? So an InsideReference knows who moved to it?
//              - And then when the ref count becomes 1, if we know we point to another reference, we have to go to it
//                  and remove its reference to us, so we can free ourself.
//              - And then when the ref count becomes 2, we can see if we both point to another reference, and have
//                  a reference pointed to ourself. If so... then those are our references. However, we have to do it differently...
//                  We have to try to replace the reference that points to us to point to the reference after us.
//              - And of course, SetOutsideReference will follow to the deepest reference.
//              - So... we will have a doubled linked list, which we only add to the end of
//                  - But as everything is ref counted, removing is actually easy, having A -> B -> C, we can
//                      hold all references and atomically swap. Oh, but... what about back references? Hmm... upon seeing
//                      B only has 2 references, you could then get the back reference, and destroy it, maybe atomically
//                      swapping it, ensuring deletion of a node only happens once?
//  - So...
//      - Oh crap... but, actually, our whole linked list thing doesn't work IF you link to another InsideReference stored
//          in different underlying storage. Crap... okay, we do have to resolve that...
//          - Optional- or... required? self reference to storage holding InsideReference? Or, maybe they only require the reference
//              to their storage once they are moved WHILE they have an outstanding reference (as if they don't have an outstanding reference,
//              they can just be destroyed without worry).
//          - And then, that reference is destroyed last, after the value would be freed.

// So, an InsideReference is one of:
//      // (Count will overlap with insideReferenceHolderRef, and be incremented so it works as either, that way chaining
//      //  is simple)
//      uint64_t count;
//      MemoryPool* pool;
//      OutsideReference prevInsideReference;
//      // (if it has no pool, this can be a constant reference which the MemoryPool knows about, causing it to just call a static function, ex, the free fnc,
//      //    which we will just call the global pool, or system pool, or whatever...)
//      // (also, it is assumed the pool will exist in the same memory as the insideReference, so be valid, or if we are chained
//      //    use insideReferenceHolderRef to make sure it is alive)
//      // (for the InsideReference itself, and will be called with InsideReference*)
//      (not used)
//      OutsideReference nextInsideReference;
//  OR
//      OutsideReference insideReferenceHolderRef;
//      // (not used?)
//      MemoryPool* pool;
//      OutsideReference prevInsideReference;
//      OutsideReference nextInsideReference;

// Hmm... swapping stuff around gets really tricky... but I believe it is tractable... We can make everything sort of immutable,
//  and once the insideReferenceHolderRef.count becomes low enough, we have do a few checks, and once we know our type,
//  we can them be sure we are currently single threaded, and then go about removing the last references to a node,
//  (which can be done safely, because all the pointers will stay unique, as we will have references to them), and after which
//  we will be truly single threaded, and able to do a lot of stuff.

// Hmm... but if a reference finds nothing needs its inside reference to stay alive, except the prevInsideReference, it could just go
//  back to the prevInsideReference and change it to point to the new inside reference. The ref count on prevInsideReference.nextInsideReference
//  should be 0, and if it is 0 that is safe, so this should be usually possible. And actually... all swaps will really be like that,
//  so swaps should actually be really easy to do.

// Move
//  - count -> insideReferenceHolderRef
//  - pool -> null
//  - prevInsideReference stays whatever it is
//  - nextInsideReference from null -> new allocation reference

//      // We have no pool, as it is assumed we were moved to free the holder, and so it is assumed the real holder for us is inside
//      //    insideReferenceHolderRef, which probably holds multiple inside references.
//      // ALSO, of course, in our specific case our free will also pass the inside value to be freed by the user's allocator, which should only happen
//      //  once. Of course if this isn't wanted, the insideReferenceHolder could iterate over all of its references can simulate each of them
//      //  being freed independently, or something...

// Then we can put fixed sized pools for values in AtomicSlots
//  - This fixed sized memory pool will have a special handler to also pass the value back to us (or it might just be part of us), so we can call 
//      the user's free with the value.
//      - This only gets called once, as once we move a value it gains a new memory pool, and the old value when freed will only free the
//          inside reference holder (the main allocation).

// And then... the InsideReferences for values can actually just store the entire values, continously.

// So... when moving an InsideReference... maybe we have a bit on every field that we check when accessing? Definitely count and
//  insideReferenceHolderRef need to be close... or maybe THEY should be the same field? And so... when we decrement count, we can know
//  if in fact we just decremented the insideReferenceHolderRef? In which case... that could be okay... if we move all of our count
//  to that... And actually, we could always have an insideReferenceHolderRef, just sometimes have it be nullptr. And then if we go to 0
//  and it is nullptr, we will know to free via our pool? Yeah... and then no one will even try to move our memory, and that caller
//  will have to have basically an insideReferenceHolderRef anyway, so everything will be fine...

// And then... finally, the benefit of all is this, is that it means we can put fixed sized MemPools where the entries used to be,
//  allocate from them (oh, I guess we need to actually make our MemoryPools stack allocate a few objects, like we thought of before),
//  and then store an OutsideReference to that in the value slot. Which means the value doesn't have to be interlaced, as when
//  swapping we can just move the pointer, and allow the underlying values to be immutable. Which also means finds can be
//  faster, and everything can just be a lot faster...
// OH! And we can actually move the memory pools so they aren't beside the main values, and even do some linear probing looking between
//  memory pools (or at least add a todo for that), which will allow the main hash lookup search to use very very little memory,
//  and really just be a list of OutsideReferences and hashes (in atomic values, although... we should consider whether they need
//  to be atomic, or if we can just swap them together, as their own 128 bit value???)

// And actually, just make one memory pool per allocation, but start searching based on the hash.

//todonext
// Design MemoryPool, probably to just be basically polymorphic, having the free and allocate as the first members.

// TODO: Once we allocate InsideReferences from the main allocation, we could actually change the references to just be indexes
//  instead of pointers. And then if we run out of allocations we could resize (even if the reserved count is lower), so we only
//  ever have to use indexes. This would give us extra bits, meaning if a system used a full 64 bits in pointers, we could
//  create an index which uses however many bits we actually need.

/*
int atomicHashTable2_copyToValue(AtomicHashTable2* this, AtomicSlot* slot, OutsideReference* outRef, InsideReference* outInsideRef) {
    if(this->VALUE_SIZE % 8 != 0) {
        // VALUE_SIZE must be divisible by 8, or else this code is slower, and harder to write (and that's really the only reason)
        OnError(3);
        return 3;
    }
    for(uint64_t i = 0; i < this->VALUE_SIZE / 8; i++) {
        ((uint64_t*)value)[i] = (&slot->valueUnits)[i].value;
    }
    return 0;
}
*/


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

//todonext
// During contention holes appear in continous blocks in the hash table. So... clearly some of our code is wrong.
//  Maybe just print every time we completely remove an element?
//  The problem is probably our fast retry code failing to check for something, allowing it to continue even after we should
//  restart the outer loop...

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