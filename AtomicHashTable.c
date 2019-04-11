#include "AtomicHashTable.h"

#include "Timing.h"

#pragma pack(push, 1)
typedef union {
	uint64_t value;
	struct {
		uint64_t index : 32;
		uint64_t allocation : 8;
	};
} DataIndex;
#pragma pack(pop)

// If we make this smaller we have to change one of ours tests... Basically, our shrink factor also provides another
//	value, which we may be smaller than, but which we never shrink once we are below that value. Right now this value
//	is greater than that, but it doesn't really matter... (although, allocating more memory is better, as misses
//	are much faster the more we allocate).
uint64_t logMinEntryCount = 10;
// This can be close to 33, but... it takes so long to insert (> 1000ns), so I don't think it matters if this is less...
//	Also, even at 27 we use ~30GB, so we use ~450 bytes per insert (for a very small value struct), so we won't have
//	enough memory for 33 anyway...
uint64_t logMaxEntryCount = 27;

uint64_t allocLogToSize(uint64_t log) {
	return log == 0 ? 0 : (1 << (log - 1));
}
uint64_t hashToIndex(uint64_t hash, uint64_t log) {
	return hash >> (64 - log + 1);
}


int atomicHashTable_applyChange(
	AtomicHashTable* this, TransactionChange change
) {
	DataIndex index;
	index.value = TransactionChange_get_dataIndex(&change);
	
	AtomicUnit* allocation = nullptr;
	uint32_t allocRef = 0;
	if (index.allocation == 0) {
		allocation = this->zeroAllocation;
	}
	else {
		allocRef = Get_uint32_t(
			this->zeroAllocation,
			&this->base.allocations[index.allocation - 1]
		);
		// Reference it, in case it is removed from the allocation while we are running
		allocation = SafeReferenceSmallPointer32(&allocRef);
	}
	if (!allocation) {
		return 1;
	}

	AtomicUnit* unit = &allocation[index.index];

	int result = AtomicSet(unit, change);

	if (allocRef) {
		DereferenceSmallPointer(allocRef);
	}

	return result;
}



typedef struct {
	AtomicHashTable* this;
	uint32_t allocations[64];
} DtorTransactionState;

int atomicHashTable_dtorTransaction(
	DtorTransactionState* state,
	void* context,
	int(*insertWrite)(void* context, TransactionChange change),
	int(*finish)(void* context)
) {
	AtomicHashTable* this = state->this;

	// Go through all entries in all allocations, and free those, and then go free all allocations!
	uint32_t allocations[ALLOCATION_COUNT];

	Get_Bytes(this->zeroAllocation,
		&this->base.allocations,
		(unsigned char*)allocations,
		sizeof(allocations)
	);

	// TODO: Copy the allocation table out, try to free everything, and then retry the transaction loop,
	//	failing if there are any more allocations. Although... this means if the hash table has some sort
	//	of self consistency it could fail during destruction... but that's probably fine...
	//	(one failure would be if the user adds A, then B, being careful for the add of A to finish
	//	successfully first. Then during dtor we would allow them to query for B, find it, but they might
	//	not find A. Which is verifiably bad...)
	for (int i = 0; i < ALLOCATION_COUNT; i++) {
		uint32_t alloc = allocations[i];
		if (!alloc) {
			continue;
		}
		AtomicUnit* units = SafeReferenceSmallPointer32(&alloc);
		if (!units) {
			continue;
		} 
		uint64_t size = allocLogToSize(i + 1);
		for (int k = 0; k < size; k++) {
			uint32_t listHead = (uint32_t)units[k].value;
			while (listHead) {
				HashEntry* entry = SafeReferenceSmallPointer32(&listHead);
				if (!entry) break;
				uint32_t nextEntry = entry->nextEntry;
				DereferenceSmallPointer(entry->value);
				DereferenceSmallPointer(listHead);
				// This will free it from the list, orphaning the other entries, leaking them
				//	if we crash.
				DereferenceSmallPointer(listHead);
				listHead = nextEntry;
			}
		}
		DereferenceSmallPointer(alloc);
		DereferenceSmallPointer(alloc);
	}


	TransactionProperties props = { 0 };
	// Base sets use an allocation index of 0, and so can just be set raw
	int result = Set_Bytes(context, insertWrite, this->zeroAllocation,
		&this->base,
		(unsigned char*)&props,
		sizeof(props)
	);
	if (result != 0) return result;

	return finish(context);
}
int AtomicHashTable_dtor(AtomicHashTable* this) {
	DtorTransactionState state = { 0 };
	state.this = this;

	int result = TransactionQueue_ApplyWrite(
		&this->transactions,
		&state,
		atomicHashTable_dtorTransaction,
		this,
		atomicHashTable_applyChange
	);
	if (result != 0) {
		OnError(result);
		return result;
	}

	for (int i = 0; i < ALLOCATION_COUNT; i++) {
		uint32_t alloc = state.allocations[i];
		if (alloc) {
			// We atomically swapped it out of the table (via a transaction), so... it is safe to just free
			//	the table reference.
			DereferenceSmallPointer(alloc);
		}
	}
	return 0;
}


// TransactionChange_set_dataIndex

// (when we need to allocate we hard failure, and allocate outside the transaction loop, and then retry. In this way
//	we can reuse the allocation for multiple contention retries inside the transaction loop, instead of resetting everything
//	after a contention failure. Also, this makes loops more consistent)
//	(and we also do all free outside/after the loop too)

typedef struct {
	bool isAdd;

	// Add values
	uint64_t entrySmallPointer : BITS_IN_SMALL_POINTER;
	HashEntry* entry;

	// Remove values
	uint64_t hash;
	void* callbackContext;
	// On true, removes the value from the table
	bool(*callback)(void* callbackContext, void* value);

} StateChangeCore;

#define MOVE_FACTOR 64

typedef struct {
	AtomicHashTable* this;

	StateChangeCore change;	

	// (output)
	uint64_t needsAllocationOfSize;
	// (input, as a response to needsAllocationOfSize)
	//	(free if not nulled out)
	uint64_t allocationToAdd : BITS_IN_SMALL_POINTER;

	// (output)
	uint64_t allocationToFree : BITS_IN_SMALL_POINTER;

	// (output)
	uint64_t needsListEntries;

	// (input/output)
	// For use to create new lists when expanding the list.
	//	Everything in here should be dereferenced after we finish (if anything was used we would add a reference to it).
	uint32_t* entries;

	// (output)
	// May be sparsely populated, (so we do have to iterate all of them).
	uint32_t listsToFree[MOVE_FACTOR];

	// (output)
	uint64_t countRemoved;

} InsertState;


// Returns < 0 if an error occurs when reading
int64_t atomicHashTable_countListSize(uint32_t listHead) {
	int64_t count = 0;
	while (listHead) {
		HashEntry* entry = SafeReferenceSmallPointer32(&listHead);
		if (!entry) {
			return -1;
		}
		if (entry->value) {
			count++;
		}
		uint32_t nextListHead = entry->nextEntry;
		DereferenceSmallPointer(listHead);
		listHead = nextListHead;
	}
	return count;
}

typedef struct {
	AtomicHashTable* this;
	uint64_t count;
} DecrementState;
int atomicHashTable_decrementTransaction(
	DecrementState* state,
	void* context,
	int(*insert)(void*context, TransactionChange change),
	int(*finish)(void* context)
) {
	uint64_t currentFillCount;
	currentFillCount = Get_uint64_t(
		state->this->zeroAllocation,
		&state->this->base.currentFillCount
	);

	currentFillCount -= state->count;

	int result = Set_uint64_t(
		context,
		insert,
		state->this->zeroAllocation,
		&state->this->base.currentFillCount,
		currentFillCount
	);

	if (result != 0) {
		//OnError(result);
		return result;
	}

	return finish(context);
}

// Returns true if the head of the list has been deallocated
bool atomicHashTable_removeEmptyEntries(
	uint32_t listHead
) {
	uint32_t lastPointerWithNoValue = 0;
	bool lastPointerInvalid = false;

	// Eh... 10 seems good? This number is arbitrary though, and could be lower (or higher).
	#define lastEntriesSize 10
	uint32_t lastEntries[lastEntriesSize] = { 0 };
	uint32_t lastEntriesAddIndex = 0;
	{
		uint32_t listHeadTemp = listHead;
		while (listHeadTemp) {
			lastEntries[lastEntriesAddIndex] = listHeadTemp;
			lastEntriesAddIndex = (lastEntriesAddIndex + 1) % lastEntriesSize;
			HashEntry* entry = SafeReferenceSmallPointer32(&listHeadTemp);
			if (!entry) {
				// Treat like empty, but remember not to dereference it
				lastPointerWithNoValue = listHeadTemp;
				lastPointerInvalid = true;
				break;
			}

			if(!entry->value && !entry->nextEntry) {
				// Leave the reference to lastPointerWithNoValue, so it stays unique
				lastPointerWithNoValue = listHeadTemp;
				break;
			} else {
				uint64_t nextListHeadPointer = entry->nextEntry;
				DereferenceSmallPointer(listHeadTemp);
				listHeadTemp = (uint32_t)nextListHeadPointer;
			}
		}
	}

	// Now iterate over the last X backwards...
	for (uint64_t i = 1; i <= lastEntriesSize; i++) {
		// Go to the entry before the last value
		uint64_t ii = (lastEntriesAddIndex - i + lastEntriesSize * 2 - 1) % lastEntriesSize;
		uint32_t entryPointer = lastEntries[ii];
		if (!entryPointer) break;

		HashEntry* entry = SafeReferenceSmallPointer32(&entryPointer);
		if (!entry) {
			// Invalid entry, so the same as empty, but don't dereference it
			lastPointerWithNoValue = entryPointer;
			lastPointerInvalid = true;
			continue;
		}

		// Will be true, unless this is the last member of the list
		if (entry->nextEntry != lastPointerWithNoValue) {
			DereferenceSmallPointer(entryPointer);
			break;
		}

		// Wipe it out
		entry->nextEntry = 0;

		if(!lastPointerInvalid) {
			// So this is the second last entry
			DereferenceSmallPointer(lastPointerWithNoValue);
			// And the second deref should free the last entry
			DereferenceSmallPointer(lastPointerWithNoValue);
		}
		lastPointerWithNoValue = 0;

		lastEntries[(ii + 1) % lastEntriesSize] = 0;

		// If the last entry now has a value, stop
		if (entry->value) {
			DereferenceSmallPointer(entryPointer);
			break;
		}

		// Now try to free this entry,
		lastPointerWithNoValue = entryPointer;
		lastPointerInvalid = false;
	}

	if (lastPointerWithNoValue && !lastPointerInvalid) {
		// This removes the extra ref we added to last pointer
		DereferenceSmallPointer(lastPointerWithNoValue);
	}

	// We deallocated the head, now wipe it out (if this fails... it is okay. The list becomes slower, because it has
	//	to dereference before knowing the value is really empty, but that is fine...)
	if (lastPointerWithNoValue == listHead) {

		if(!lastPointerInvalid) {
			// Because our pop loop can't free the last value
			HashEntry* entry = SafeReferenceSmallPointer32(&lastPointerWithNoValue);
			if (entry) {
				DereferenceSmallPointer(lastPointerWithNoValue);
				DereferenceSmallPointer(lastPointerWithNoValue);
			}
		}

		return true;
	}
	return false;
}


int atomicHashTable_insertTransaction(
	InsertState* state,
	void* context,
	int(*insert)(void*context, TransactionChange change),
	int(*finish)(void* context)
) {
	AtomicHashTable* this = state->this;

	// Eh... we don't need to read so much. But... this makes things slightly easier, and
	//	reading should be orders of magnitude faster than writing, so this should be fine...
	TransactionProperties props;

	Get_Bytes(this->zeroAllocation,
		&this->base,
		(unsigned char*)&props,
		sizeof(props)
	);

	TransactionProperties originalProps = props;

	// We decrement currentFillCount outside of the main transaction, as we remove values outside of transactional memory.
	//	This insures anything we unallocate WILL actually be removed from the transaction count. The resize after removing
	//	is then delayed... but only by 1 call (or the number of threads), which is fine...
	if (state->change.isAdd) {
		props.currentFillCount++;
	}

	uint64_t currentEntrySize = props.moveAllocationLog ? allocLogToSize(props.moveAllocationLog) : allocLogToSize(props.currentAllocationLog);

	// Move factor calculations

	// We move with a factor of F of the larger number.

	// So, if we grow, we go from X fill capacity, to X/2. So
	//	NEW_CAP * X/2 = CURRENT_ELEMENTS
	//	NEW_CAP * Y = SHRINK_COUNT
	//	NEW_CAP / F = ELEMENTS_TO_MOVE_ALL
	//	CURRENT_ELEMENTS - SHRINK_COUNT > ELEMENTS_TO_MOVE_ALL
	//	NEW_CAP * X/2 - NEW_CAP * Y > NEW_CAP / F
	//	X/2 - Y > 1 / F

	// When we shrink we go from Y fill capacity to Y*2
	//	OLD_CAP * Y = CURRENT_ELEMENTS
	//	OLD_CAP * X = GROW_COUNT
	//	OLD_CAP / (F / 2) = ELEMENT_TO_MOVE_ALL
	//	GROW_COUNT - CURRENT_ELEMENTS > ELEMENT_TO_MOVE_ALL
	//	OLD_CAP * X - OLD_CAP * Y > OLD_CAP / (F / 2)
	//	X - Y > 1 / (F / 2)

	// And also, we need to be able to move everything before we shrink again!
	//	OLD_CAP * Y = CURRENT_ELEMENTS
	//	OLD_CAP * Y * Y = SHRINK_COUNT
	//	OLD_CAP / (F / 2) = ELEMENT_TO_MOVE_ALL
	//	CURRENT_ELEMENTS - SHRINK_COUNT > ELEMENT_TO_MOVE_ALL
	//	OLD_CAP * Y - OLD_CAP * Y * Y > OLD_CAP / (F / 2)
	//	Y - Y*Y > 1 / (F / 2)

	// And before we grow again
	//	NEW_CAP * X/2 = CURRENT_ELEMENTS
	//	NEW_CAP * X = GROW_COUNT
	//	NEW_CAP / F = ELEMENTS_TO_MOVE_ALL
	//	GROW_COUNT - CURRENT_ELEMENTS > ELEMENTS_TO_MOVE_ALL
	//	X/2 > 1 / F

	//	X/2 - Y > 1 / F
	//	X - Y > 1 / (F / 2)
	//	Y - Y*Y > 1 / (F / 2)
	//	X/2 > 1 / F

	// TODO: Once we know we aren't going to fall behind in our moves, add checks to prevent trying to move twice, to
	//	sort out the small number test cases, or at least to not hard fail if my calculations are wrong...

	bool resizeGrow = (
		props.currentFillCount >= currentEntrySize / 10 * 5
		// One exact chance, which will give us some time to resize uninterrupted, until we reach 70% at which point
		//	we will absolutely need to resize.
		|| props.currentFillCount == currentEntrySize / 10 * 4
	);
	// TODO: Also add, props.currentAllocationLog > logMinEntryCount? Maybe never allocating under logMinEntryCount
	//	once we use any entries?
	bool resizeShrink = (
		props.currentFillCount < currentEntrySize / 20 * 1
		|| props.currentFillCount == (currentEntrySize / 10 * 1 - 1)
	);
	if (resizeShrink && props.currentAllocationLog == logMinEntryCount) {
		resizeShrink = false;
	}
	if (resizeGrow && props.currentAllocationLog == logMaxEntryCount) {
		resizeGrow = false;
	}

	bool usedAllocationToAdd = false;

	if (resizeGrow || resizeShrink) {
		if (props.moveAllocationLog) {
			// Impossible. We haven't finished the previous move.
			OnError(2);
			return 2;
		}

		uint64_t newAllocationLog = props.currentAllocationLog + (resizeGrow ? 1 : -1);
		if (resizeGrow && newAllocationLog < logMinEntryCount) {
			// Skip the first few allocation sizes
			newAllocationLog = logMinEntryCount;
		}
		uint64_t newAllocationSize = allocLogToSize(newAllocationLog) * sizeof(AtomicUnit);
		uint64_t newAllocation = 0;
		if (state->needsAllocationOfSize >= newAllocationSize) {
			usedAllocationToAdd = true;
			newAllocation = state->allocationToAdd;
		}
		else {
			state->needsAllocationOfSize = newAllocationSize;
			return 2;
		}

		props.allocations[newAllocationLog - 1] = (uint32_t)newAllocation;
		props.moveAllocationLog = newAllocationLog;
		props.nextMoveSlotIndex = 0;
	}


	typedef struct {
		uint64_t allocLog;
		uint64_t allocIndex;
		HashSlot listHead;
	} EntrySetIntent;
	uint64_t setIntentCount = 0;
	// MOVE_FACTOR for resizing, and 1 for insert
	EntrySetIntent setIntents[MOVE_FACTOR + 1] = { 0 };

	// We only remove from the end of the list, and insert to the beginning, so we don't have to worry about our list
	//	being cut up (without the transaction memory from changing, which would cause us to rerun anyway)...

	uint32_t sourceLists[MOVE_FACTOR] = { 0 };
	uint32_t destListHeads[MOVE_FACTOR] = { 0 };
	uint64_t usedNewListEntries = 0;

	uint64_t prevAllocSmallPointer = 0;

	if (props.moveAllocationLog) {
		uint64_t nextMoveSlotIndex = props.nextMoveSlotIndex;
		uint64_t destMoveSize = allocLogToSize(props.moveAllocationLog);
		uint64_t sourceSize = allocLogToSize(props.currentAllocationLog);

		bool growing = props.moveAllocationLog > props.currentAllocationLog;

		uint64_t sourceCount = growing ? (MOVE_FACTOR / 2) : MOVE_FACTOR;
		uint64_t destCount = growing ? MOVE_FACTOR : (MOVE_FACTOR / 2);
		if (sourceCount > sourceSize - nextMoveSlotIndex) {
			sourceCount = sourceSize - nextMoveSlotIndex;
		}

		uint64_t destSlotIndex = growing ? nextMoveSlotIndex * 2 : nextMoveSlotIndex / 2;
		if (destCount > destMoveSize - destSlotIndex) {
			destCount = destMoveSize - destSlotIndex;
		}

		
		uint32_t sourceAlloc = props.allocations[props.currentAllocationLog - 1];
		if (sourceAlloc) {
			AtomicUnit* sourceUnits = SafeReferenceSmallPointer32(&sourceAlloc);
			if (!sourceUnits) {
				// Probably due to contention
				return 1;
			}
			for (uint64_t i = 0; i < sourceCount; i++) {
				uint64_t sourceSlotIndex = nextMoveSlotIndex + i;
				if (sourceSlotIndex >= sourceSize) break;
				HashSlot sourceSlot;
				Get_Bytes(
					sourceUnits,
					(void*)((uint64_t)sourceUnits + sourceSlotIndex * sizeof(sourceSlot)),
					(unsigned char*)&sourceSlot,
					sizeof(sourceSlot)
				);
				sourceLists[i] = sourceSlot.listHead;
			}
			DereferenceSmallPointer(sourceAlloc);
		}

		int64_t neededEntries = 0;
		for (uint64_t i = 0; i < sourceCount; i++) {
			uint32_t sourceList = sourceLists[i];
			int64_t neededEntriesCur = atomicHashTable_countListSize(sourceList);
			if (neededEntriesCur < 0) return 1;
			neededEntries += neededEntriesCur;
		}

		if ((uint64_t)neededEntries > state->needsListEntries) {
			// + 5 + 25%, so even if more entries are added, we will probably still have enough buffer to expand
			state->needsListEntries = (neededEntries + 5) * 5 / 4;
			return 2;
		}

		for (uint64_t i = 0; i < sourceCount; i++) {
			uint32_t sourceList = sourceLists[i];
			if (sourceList == 0) continue;

			while (sourceList) {
				HashEntry* entry = SafeReferenceSmallPointer32(&sourceList);
				if (!entry) {
					return 1;
				}

				// Only move entries with values
				if (entry->value) {
					if (usedNewListEntries >= state->needsListEntries) {
						// Surely we ran out just because entries were added while we were iterating
						return 1;
					}

					uint64_t destIndex = hashToIndex(entry->hash, props.moveAllocationLog) % destCount;
					uint32_t prevListHead = destListHeads[destIndex];

					uint32_t newListHead = state->entries[usedNewListEntries++];
					//state->listsToFree[destIndex] =
					destListHeads[destIndex] = newListHead;

					HashEntry* newListEntry = SafeReferenceSmallPointer32(&newListHead);
					if (!newListEntry) {
						// Should be impossible, how could this have been freed already, we haven't inserted it yet...
						// TODO: We should free anything we put in destListHeads already here... because right now we leak all of that...
						OnError(2);
						return 2;
					}
					// It already comes referenced, but when we dereference it here it crashes?
					DereferenceSmallPointer(newListHead);

					newListEntry->hash = entry->hash;
					// value can be safely moved, as anything that might remove it will have to operate at least with a get
					//	on the transaction loop, which means either we will get a retry upon calling insert, or they will...
					newListEntry->value = entry->value;
					newListEntry->nextEntry = prevListHead;
				}

				uint32_t prevSourceList = sourceList;
				sourceList = entry->nextEntry;
				DereferenceSmallPointer(prevSourceList);
			}
		}

		for (uint64_t i = 0; i < destCount; i++) {
			uint64_t destSlotIndexCur = destSlotIndex + i;

			// Okay, so... the indexes inside of destListHeads (i) doesn't linearily map as offsets to destSlotIndex.
			//	Those indexes are just the bits choosen, which may need to be mapped [3, 0, 1, 2], [2,3,0,1], [1,2,3,0] or [0,1,2,3]
			uint64_t destListHead = destListHeads[(destSlotIndexCur % destCount)];
			EntrySetIntent newState = { 0 };
			newState.allocLog = props.moveAllocationLog;
			newState.allocIndex = destSlotIndexCur;
			newState.listHead.listHead = (uint32_t)destListHead;
			setIntents[setIntentCount++] = newState;
		}

		props.nextMoveSlotIndex += sourceCount;
		// Finish the move, if its done
		if (props.nextMoveSlotIndex >= sourceSize) {
			prevAllocSmallPointer = props.allocations[props.currentAllocationLog - 1];
			// Clear out the previous allocation
			props.allocations[props.currentAllocationLog - 1] = 0;
			props.currentAllocationLog = props.moveAllocationLog;
			props.moveAllocationLog = 0;
			props.nextMoveSlotIndex = 0;
		}
	}

	if (props.currentFillCount > allocLogToSize(props.currentAllocationLog)) {
		// Probably out of memory, not really an error... it should be handed well
		//	Could also be a mistake an in the resizing code not making us big enough...
		return 10;
	}

	// Add to setIntents, deduping/solving multiple writes, and then apply everything finally

	if (state->change.isAdd) {
		HashEntry* entry = state->change.entry;
		uint64_t targetIndex = hashToIndex(entry->hash, props.currentAllocationLog);

		uint64_t allocUsedLog = props.currentAllocationLog;
		if (props.moveAllocationLog && targetIndex < props.nextMoveSlotIndex) {
			// It is in the already moved memory
			targetIndex = hashToIndex(entry->hash, props.moveAllocationLog);
			allocUsedLog = props.moveAllocationLog;
		}

		// If we are moving it, then make sure our insert piggybacks on the insert
		bool foundIntent = false;
		for (uint64_t i = 0; i < setIntentCount; i++) {
			if (setIntents[i].allocLog == allocUsedLog && setIntents[i].allocIndex == targetIndex) {
				foundIntent = true;
				entry->nextEntry = setIntents[i].listHead.listHead;
				setIntents[i].listHead.listHead = (uint32_t)state->change.entrySmallPointer;
			}
		}

		if (!foundIntent) {
			AtomicUnit* units;
			uint32_t allocUsed = props.allocations[allocUsedLog - 1];
			units = SafeReferenceSmallPointer32(&allocUsed);
			if (!units) {
				return 1;
			}

			uint32_t prevEntryPointer = (uint32_t)units[targetIndex].value;
			entry->nextEntry = prevEntryPointer;

			EntrySetIntent newState = { 0 };
			newState.allocLog = allocUsedLog;
			newState.allocIndex = targetIndex;
			newState.listHead.listHead = (uint32_t)state->change.entrySmallPointer;
			setIntents[setIntentCount++] = newState;

			DereferenceSmallPointer(allocUsed);
		}
	}
	else {
		uint64_t targetIndex = hashToIndex(state->change.hash, props.currentAllocationLog);

		AtomicUnit* units;
		uint64_t allocUsedLog = props.currentAllocationLog;
		if (props.moveAllocationLog && targetIndex < props.nextMoveSlotIndex) {
			// It is in the already moved memory
			targetIndex = hashToIndex(state->change.hash, props.moveAllocationLog);
			allocUsedLog = props.moveAllocationLog;
		}
		uint32_t allocUsed = props.allocations[allocUsedLog - 1];
		units = SafeReferenceSmallPointer32(&allocUsed);
		if (!units) {
			return 1;
		}

		uint32_t listHeadPointer = (uint32_t)units[targetIndex].value;

		bool foundIntent = false;
		uint64_t foundIntentIndex = 0;
		for (uint64_t i = 0; i < setIntentCount; i++) {
			if (setIntents[i].allocLog == allocUsedLog && setIntents[i].allocIndex == targetIndex) {
				foundIntent = true;
				foundIntentIndex = i;
				listHeadPointer = setIntents[i].listHead.listHead;
			}
		}

		bool isRemovalCondidate = false;

		uint32_t listHeadTemp = listHeadPointer;
		while (listHeadTemp) {
			HashEntry* entry = SafeReferenceSmallPointer32(&listHeadTemp);
			if (!entry) {
				// Invalid entries should be treated the same as removed entries.
				isRemovalCondidate = true;
				break;
			}

			uint32_t valuePointer = entry->value;
			if (valuePointer) {
				void* value = SafeReferenceSmallPointer32(&valuePointer);
				if(value) {
					bool shouldDelete = state->change.callback(state->change.callbackContext, value);
					if (shouldDelete) {
						if((uint32_t)InterlockedCompareExchange((void*)&entry->value, 0, valuePointer) == valuePointer) {
							state->countRemoved++;
							// Otherwise someone else deleted us, and so we better not try to also delete it
							// TEMPORARY
							//DereferenceSmallPointer(valuePointer);
						}
					}
					// TEMPORARY
					//DereferenceSmallPointer(valuePointer);
				}
			}

			if (!entry->nextEntry && !entry->value) {
				isRemovalCondidate = true;
			}

			uint64_t nextListHeadPointer = entry->nextEntry;
			DereferenceSmallPointer(listHeadTemp);
			listHeadTemp = (uint32_t)nextListHeadPointer;
		}

		// TEMPORARY
		/*
		if(isRemovalCondidate) {
			if(atomicHashTable_removeEmptyEntries(listHeadPointer)) {
				if(foundIntent) {
					setIntents[foundIntentIndex].listHead.listHead = 0;
				} else {
					EntrySetIntent newState = { 0 };
					newState.allocLog = allocUsedLog;
					newState.allocIndex = targetIndex;
					newState.listHead.listHead = 0;
					setIntents[setIntentCount++] = newState;
				}
			}
		}
		*/

		DereferenceSmallPointer(allocUsed);
	}

	// Update changes props
	CASSERT(sizeof(TransactionProperties) % sizeof(uint32_t) == 0);

	uint32_t* origP = (void*)&originalProps;
	uint32_t* newP = (void*)&props;
	uint64_t atomicCount = sizeof(TransactionProperties) / sizeof(uint32_t);

	for (uint64_t i = 0; i < atomicCount; i++) {
		if (origP[i] == newP[i]) continue;
		int result = Set_uint32_t(
			context,
			insert,
			this->zeroAllocation,
			(void*)((uint64_t)this->zeroAllocation + i * sizeof(uint32_t)),
			newP[i]
		);
		if (result != 0) {
			return result;
		}
	}

	// Set values
	for (uint64_t i = 0; i < setIntentCount; i++) {
		EntrySetIntent set = setIntents[i];

		TransactionChange change = { 0 };
		DataIndex index = { 0 };
		// Below assemptions require perfect alignment
		CASSERT(sizeof(uint32_t) == 4);
		index.index = set.allocIndex;
		index.allocation = set.allocLog;

		TransactionChange_set_dataIndex(&change, index.value);
		change.newValue = set.listHead.listHead;

		int result = insert(context, change);

		if (result != 0) {
			return result;
		}
	}

	int finalResult = finish(context);
	if (finalResult != 0) {
		return finalResult;
	}

	// Now that we committed our change, null out the entries we used
	for (uint64_t i = 0; i < usedNewListEntries; i++) {
		state->entries[i] = 0;
	}

	// Now the source lists have moved (confirmed), so we can free them
	for (uint64_t i = 0; i < MOVE_FACTOR; i++) {
		state->listsToFree[i] = sourceLists[i];
	}

	if (prevAllocSmallPointer) {
		DereferenceSmallPointer(prevAllocSmallPointer);
	}

	if (usedAllocationToAdd) {
		state->allocationToAdd = 0;
	}

	return 0;
}

int AtomicHashTable_mutate(
	AtomicHashTable* this,
	uint64_t hash,
	uint32_t valueSmallPointer,
	void* removeContext,
	bool(*shouldRemove)(void* removeContext, void* value)
) {
	InsertState state = { 0 };

	state.this = this;
	state.change.isAdd = !shouldRemove;
	if (state.change.isAdd) {
		state.change.entrySmallPointer = AllocateAsSmallPointer(sizeof(HashEntry), &state.change.entry);

		state.change.hash = hash;
		state.change.entry->hash = hash;
		state.change.entry->value = valueSmallPointer;
		state.change.entry->nextEntry = 0;
	}
	else {
		state.change.hash = hash;
		state.change.callbackContext = removeContext;
		state.change.callback = shouldRemove;
	}

	uint64_t lastAllocationSize = 0;
	uint64_t lastEntriesCount = 0;

	int result = 0;
	while (true) {
		result = TransactionQueue_ApplyWrite(
			&this->transactions,
			&state,
			atomicHashTable_insertTransaction,
			this,
			atomicHashTable_applyChange
		);

		bool hasMetaRequest = false;
		if (state.needsAllocationOfSize != lastAllocationSize) {
			hasMetaRequest = true;
			if (state.allocationToAdd) {
				DereferenceSmallPointer(state.allocationToAdd);
				state.allocationToAdd = 0;
			}
			if (state.needsAllocationOfSize) {
				void* p;
				state.allocationToAdd = AllocateAsSmallPointer(state.needsAllocationOfSize, &p);
				if (!state.allocationToAdd) {
					// TODO: We should free entries if we allocated any, otherwise we leak it...
					return 2;
				}
			}
			lastAllocationSize = state.needsAllocationOfSize;
		}
		if (state.needsListEntries != lastEntriesCount) {
			hasMetaRequest = true;
			if (state.entries) {
				for (uint64_t i = 0; i < lastEntriesCount; i++) {
					DereferenceSmallPointer(state.entries[i]);
					state.entries[i] = 0;
				}
				free(state.entries);
				state.entries = nullptr;
			}
			state.entries = malloc(sizeof(uint64_t*) * state.needsListEntries);
			for (uint64_t i = 0; i < state.needsListEntries; i++) {
				void* p;
				state.entries[i] = (uint32_t)AllocateAsSmallPointer(sizeof(HashEntry), &p);
				if (!state.entries[i]) {
					// TODO: We should free entries if we allocated any, otherwise we leak it...
					OnError(2);
					return 2;
				}
			}
			lastEntriesCount = state.needsListEntries;
		}
		// In this case the error is probably just a request, so continue the loop.
		if (hasMetaRequest) continue;
		break;
	}

	if (state.allocationToAdd) {
		DereferenceSmallPointer(state.allocationToAdd);
		state.allocationToAdd = 0;
	}

	if (state.entries) {
		for (uint64_t i = 0; i < lastEntriesCount; i++) {
			if (state.entries[i]) {
				DereferenceSmallPointer(state.entries[i]);
				state.entries[i] = 0;
			}
		}
		free(state.entries);
		state.entries = nullptr;
	}

	for(uint64_t i = 0; i < MOVE_FACTOR; i++) {
		uint32_t list = state.listsToFree[i];
		state.listsToFree[i] = 0;
		while(list) {
			HashEntry* entry = SafeReferenceSmallPointer32(&list);
			if(!entry) {
				// Impossible...
				OnError(3);
				break;
			}

			DereferenceSmallPointer(list);

			uint32_t nextList = entry->nextEntry;
			DereferenceSmallPointer(list);
			list = nextList;
		}
	}

	if (result != 0) {
		if(state.change.entrySmallPointer) {
			DereferenceSmallPointer(state.change.entrySmallPointer);
			state.change.entrySmallPointer = 0;
		}
	}
	
	// So... even on hard failures we still deallocated, so this should be fine...
	if (state.countRemoved > 0) {
		DecrementState decState = { 0 };
		decState.this = this;
		decState.count = state.countRemoved;

		int decResult = TransactionQueue_ApplyWrite(
			&this->transactions,
			&decState,
			atomicHashTable_decrementTransaction,
			this,
			atomicHashTable_applyChange
		);
		
		if (decResult != 0 && result == 0) {
			result = decResult;
		}
	}

	return result;
}



typedef struct {
	AtomicHashTable* this;
	uint64_t hash;

	// (input/output)
	uint64_t valueSmallPointersCount;
	// All of these pointers should have references to them, or else our caller might complain about inconsistency in the results.
	uint32_t* valueSmallPointers;
	uint64_t valueSmallPointersUsed;
} FindState;

int AtomicHashTable_findTransactionInner(
	FindState* state
) {
	// Handle retry loops (by reseting our results)
	for (uint64_t i = 0; i < state->valueSmallPointersUsed; i++) {
		uint32_t valueSmallPointer = state->valueSmallPointers[i];
		if (valueSmallPointer) {
			state->valueSmallPointers[i] = 0;
			DereferenceSmallPointer(valueSmallPointer);
		}
	}
	state->valueSmallPointersUsed = 0;

	AtomicHashTable* this = state->this;
	uint64_t hash = state->hash;

	uint64_t currentAllocationLog = Get_uint64_t(this->zeroAllocation, &this->base.currentAllocationLog);

	if (!currentAllocationLog) {
		return 0;
	}

	uint64_t nextMoveSlotIndex = Get_uint64_t(this->zeroAllocation, &this->base.nextMoveSlotIndex);

	uint32_t allocSmallPointer = Get_uint32_t(this->zeroAllocation, (this->base.allocations + currentAllocationLog - 1));

	//TimeBlock(FindTransactionSetup,

	int result = 0;
	uint64_t nextSmallRefIndex = 0;
	// Used so we can cleanup our references after we finish. We only use 2 of these entries right now.
	uint64_t smallRefs[10] = { 0 };

	uint64_t unitsIndex = nextSmallRefIndex++;
	uint64_t entryIndex = nextSmallRefIndex++;

	AtomicUnit* units = SafeReferenceSmallPointer32(&allocSmallPointer);
	if (!units) {
		result = 1;
		goto cleanup;
	}
	smallRefs[unitsIndex] = allocSmallPointer;
	
	uint64_t index = hashToIndex(hash, currentAllocationLog);
	if(index < nextMoveSlotIndex) {
		uint64_t moveAllocationLog = Get_uint64_t(this->zeroAllocation, &this->base.moveAllocationLog);
		DereferenceSmallPointer(allocSmallPointer);
		smallRefs[unitsIndex] = 0;
		allocSmallPointer = Get_uint32_t(this->zeroAllocation, (this->base.allocations + moveAllocationLog - 1));
		units = SafeReferenceSmallPointer32(&allocSmallPointer);
		if (!units) {
			result = 1;
			goto cleanup;
		}
		smallRefs[unitsIndex] = allocSmallPointer;
		index = hashToIndex(hash, moveAllocationLog);
	}
	uint32_t listHead = (uint32_t)units[index].value;
	//);

	// So... the retries in this loop are funny. Other retries are when a function detects contention in the synced data,
	//	and so returns. The transaction queue should be able to figure out where the contention happened (but won't, as
	//	there is no need). HOWEVER, these retries are for contention outside of the synced data, so the transaction
	//	queue won't know why it is retrying (it will still work, but... you know...).

	//TimeBlock(FindSlotLoop, {
	while (listHead) {
		//TimeBlock(FindSlotLoopInner, {

		HashEntry* entry = SafeReferenceSmallPointer32(&listHead);
		if (!entry) {
			// Invalid pointer, assume it is the same as no pointer.
			break;
		}

		smallRefs[entryIndex] = listHead;

		uint32_t valueSmallPointer = entry->value;
		if (valueSmallPointer) {
			uint64_t valueIndex = state->valueSmallPointersUsed++;
			if (valueIndex >= state->valueSmallPointersCount) {
				state->valueSmallPointersCount = state->valueSmallPointersCount * 2;
				result = 2;
				goto cleanup;
			}

			void* value = SafeReferenceSmallPointer32(&valueSmallPointer);
			if (value) {
				state->valueSmallPointers[valueIndex] = valueSmallPointer;
			}
			else {
				state->valueSmallPointersUsed--;
			}
			// (we purposely keep a reference to valueSmallPointer, to keep value alive while we return it)
			
		} // else it is an empty value, which is okay

		smallRefs[entryIndex] = 0;

		uint64_t nextHead = entry->nextEntry;
		DereferenceSmallPointer(listHead);
		listHead = (uint32_t)nextHead;
		//});
	}
	//});

cleanup:
	for(uint64_t i = 0; i < nextSmallRefIndex; i++) {
		uint64_t smallP = smallRefs[i];
		if(smallP) {
			DereferenceSmallPointer(smallP);
		}
	}
	return result;
}

int AtomicHashTable_findTransaction(
	FindState* state
) {
	TimeBlock(AtomicHashTable_findTransaction,
	int result = AtomicHashTable_findTransactionInner(state);
	);

	return result;
}

// TODO: Add a multi-find? As once we are inside the loop and have the allocations we can probably run many finds really quickly.
int AtomicHashTable_find(
	AtomicHashTable* this,
	uint64_t hash,
	void* callbackContext,
	// May return results that don't equal the given hash, so a deep comparison should be done on the value
	//	to determine if it is the one you want.
	// (and obviously, may call this callback multiple times for one call)
	void(*callback)(void* callbackContext, void* value)
) {

	FindState state = { 0 };
	state.this = this;
	state.hash = hash;

#define STACK_OUTPUT_COUNT 10
	uint32_t stackOutput[STACK_OUTPUT_COUNT] = { 0 };

	uint64_t lastValueSmallPointersCount = state.valueSmallPointersCount = STACK_OUTPUT_COUNT;
	state.valueSmallPointers = stackOutput;

	int result = 0;

	TimeBlock(AtomicHashTable_find_RunLoop, {
	while (true) {
		result = TransactionQueue_RunGetter(
			&this->transactions,
			&state,
			AtomicHashTable_findTransaction,
			this,
			atomicHashTable_applyChange
		);
		if (state.valueSmallPointersCount != lastValueSmallPointersCount) {
			for (uint64_t i = 0; i < lastValueSmallPointersCount; i++) {
				uint32_t prevP = state.valueSmallPointers[i];
				if (prevP) {
					DereferenceSmallPointer(prevP);
				}
			}

			if (state.valueSmallPointers && state.valueSmallPointers != stackOutput) {
				free(state.valueSmallPointers);
				state.valueSmallPointers = nullptr;
			}
			lastValueSmallPointersCount = state.valueSmallPointersCount;
			state.valueSmallPointers = malloc(lastValueSmallPointersCount * sizeof(uint32_t));
			if (!state.valueSmallPointers) {
				OnError(2);
				return 2;
			}
			continue;
		}
		break;
	}
	});

	TimeBlock(AtomicHashTable_find_finish, {
	if (result == 0) {
		HashEntry* stackEntries[STACK_OUTPUT_COUNT] = { 0 };
		HashEntry** entries = stackEntries;
		if(lastValueSmallPointersCount > STACK_OUTPUT_COUNT) {
			entries = malloc(sizeof(HashEntry*) * lastValueSmallPointersCount);
			memset(entries, 0, sizeof(HashEntry*) * lastValueSmallPointersCount);
		}

		for (uint64_t i = 0; i < state.valueSmallPointersUsed; i++) {
			uint32_t smallP = state.valueSmallPointers[i];

			HashEntry* entry = SafeReferenceSmallPointer32(&smallP);
			if (!entry) {
				// What!? Our inner transaction function failed us, it should have added a reference to the pointer!
				//	TODO: Don't leak everything here. Its annoying, because the reference count varies depending on i
				//	(either it is 1 or 2)
				return 3;
			}
			entries[i] = entry;
		}

		// Call callbacks
		// TODO: Add a try/catch around the callback function...
		for (uint64_t i = 0; i < state.valueSmallPointersUsed; i++) {
			HashEntry* entry = entries[i];
			callback(callbackContext, entry);
		}

		for (uint64_t i = 0; i < state.valueSmallPointersUsed; i++) {
			uint32_t smallP = state.valueSmallPointers[i];
			// 1 for the inner loop, another for our loop which gets the HashEntry value
			DereferenceSmallPointer(smallP);
			DereferenceSmallPointer(smallP);
		}

		if(entries != stackEntries) {
			free(entries);
			entries = nullptr;
		}
	}
	else {
		for (uint64_t i = 0; i < lastValueSmallPointersCount; i++) {
			uint32_t smallP = state.valueSmallPointers[i];
			if (smallP) {
				state.valueSmallPointers[i] = 0;
				DereferenceSmallPointer(smallP);
			}
		}
	}

	if (state.valueSmallPointers && state.valueSmallPointers != stackOutput) {
		free(state.valueSmallPointers);
		state.valueSmallPointers = nullptr;
	}

	});

	return result;
}

int AtomicHashTable_insert(AtomicHashTable* this, uint64_t hash, uint32_t valueSmallPointer) {
	return AtomicHashTable_mutate(this, hash, valueSmallPointer, (void*)0, (void*)0);
}

int AtomicHashTable_remove(
	AtomicHashTable* this,
	uint64_t hash,
	void* callbackContext,
	// On true, removes the value from the table
	bool(*callback)(void* callbackContext, void* value)
) {
	return AtomicHashTable_mutate(this, hash, 0, callbackContext, callback);
}

TransactionProperties DebugAtomicHashTable_properties(AtomicHashTable* this) {
	TransactionProperties props;

	Get_Bytes(this->zeroAllocation,
		&this->base,
		(unsigned char*)&props,
		sizeof(props)
	);

	return props;
}