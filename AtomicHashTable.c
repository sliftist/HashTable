#include "AtomicHashTable.h"

#pragma pack(push, 1)
union DataIndex {
	uint64_t value;
	struct {
		uint64_t index : 32;
		uint64_t allocation : 8;
	};
};
#pragma pack(pop)


int atomicHashTable_applyChange(
	AtomicHashTable* this, TransactionChange change
) {
	DataIndex index;
	index.value = TransactionChange_get_dataIndex(change);
	
	AtomicUnit* allocation = nullptr;
	uint32_t allocRef = 0;
	if (index.index == 0) {
		allocation = this->zeroAllocation;
	}
	else {
		allocRef = this->allocations[index.allocation - 1];
		// Reference it, in case it is removed from the allocation while we are running
		allocation = SafeReferenceSmallPointer32(&allocRef);
	}
	if (!allocation) {
		return 1;
	}

	AtomicUnit* unit = allocation[index.index];

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

	Get_Bytes(&this->zeroAllocation,
		&this->base->allocations,
		&this->allocations,
		sizeof(this->allocations)
	);

	TransactionProperties props = { 0 };

	// Base sets use an allocation index of 0, and so can just be set raw
	return Set_Bytes(context, insertWrite, &this->zeroAllocation,
		&this->base,
		(unsigned char*)&props,
		sizeof(props)
	);
}
int AtomicHashTable_dtor(AtomicHashTable* this) {
	DtorTransactionState state = { 0 };
	state.this = this;

	int result = TransactionQueue_ApplyWrite(
		&this->transactions,
		state,
		atomicHashTable_dtorTransaction,
		this,
		atomicHashTable_applyChange
	);
	if (result != 0) {
		OnError(result);
		return result;
	}

	for (int i = 0; i < ALLOCATION_COUNT; i++) {
		uint32_t alloc = state.allocation[i];
		if (alloc) {
			// We atomically swapped it out of the table (via a transaction), so... it is safe to just free
			//	the table reference.
			DereferenceSmallPointer(alloc);
		}
	}
	return 0;
}

uint64_t logMinEntryCount = 6;

// TransactionChange_set_dataIndex

// (when we need to allocate we hard failure, and allocate outside the transaction loop, and then retry. In this way
//	we can reuse the allocation for multiple contention retries inside the transaction loop, instead of resetting everything
//	after a contention failure. Also, this makes loops more consistent)
//	(and we also do all free outside/after the loop too)

typedef struct {
	AtomicHashTable* this;

	uint64_t entrySmallPointer : BITS_IN_SMALL_POINTER;
	HashEntry* entry;

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
	uint64_t* entries;

	// (output)
	// May be sparsely populated, (so we do have to iterate all of them).
	uint64_t listsToFree[4];

} InsertState;

uint64_t allocLogToSize(uint64_t log) {
	return count == 0 ? 0 : (1 << (count - 1));
}

// Returns < 0 if an error occurs when reading
int64_t atomicHashTable_countListSize(uint64_t listHead) {
	int64_t count = 0;
	while (listHead) {
		HashEntry* entry = SafeReferenceSmallPointer64(&oldListHeadTemp);
		if (!entry) {
			return -1;
		}
		if (entry->value) {
			count++;
		}
		uint64_t nextOldListHeadTemp = entry->nextEntry;
		DereferenceSmallPointer(oldListHeadTemp);
		oldListHeadTemp = nextOldListHeadTemp;
	}
	return count;
}

int atomicHashTable_insertTransaction(
	InsertState* state,
	void* context,
	int(*insert)(void*context, TransactionChange change),
	int(*finish)(void* context)
) {
	AtomicHashTable this = state->this;

	// Eh... we don't need to read so much. But... this makes things slightly easier, and
	//	reading should be orders of magnitude faster than writing, so this should be fine...
	TransactionProperties props;

	Get_Bytes(this->zeroAllocation,
		&this->base,
		&props,
		sizeof(props)
	);

	TransactionProperties originalProps = props;


	uint64_t newFillCount = props.currentFillCount + 1;
	uint64_t currentEntrySize = allocLogToSize(props.currentAllocationLog);

	bool resizeGrow = (
		newFillCount >= currentEntrySize / 10 * 7
		// One exact chance, which will give us some time to resize uninterrupted, until we reach 70% at which point
		//	we will absolutely need to resize.
		|| newFillCount == currentEntrySize / 2
	);
	bool resizeShrink = (
		newFileCount < currentEntrySize / 10 * 1
		|| newFileCount == (currentEntrySize / 10 * 2 - 1)
	);

	bool usedAllocationToAdd = false;

	if (resizeGrow || resizeShrink) {
		if (props.moveAllocationCount) {
			// Impossible. We haven't finished the previous move.
			return 2;
		}

		uint64_t newAllocationLog = props.currentAllocationLog + (resizeGrow ? 1 : -1);
		if (resizeGrow && newAllocationLog < logMinEntryCount) {
			// Skip the first few allocation sizes
			newAllocationLog = logMinEntryCount;
		}
		uint64_t newAllocationSize = allocLogToSize(newAllocationLog) * sizeof(HashSlot);
		uint64_t newAllocation = 0;
		if (state.needsAllocationOfSize >= newAllocationSize) {
			usedAllocationToAdd = true;
			newAllocation = state.allocationToAdd;
		}
		else {
			state.needsAllocationOfSize = newAllocationSize;
			return 2;
		}

		props.allocations[newAllocationLog - 1] = newAllocation;
		props.moveAllocationLog = newAllocationLog;
		props.nextMoveSlotIndex = 0;
	}


	typedef struct {
		uint64_t allocLog;
		uint32_t allocIndex;
		HashSlot listHead;
	} EntrySetIntent;
	uint64_t setIntentCount = 0;
	// 4 for resizing, and 1 for insert
	EntrySetIntent setIntents[5] = { 0 };

	// We only remove from the end of the list, and insert to the beginning, so we don't have to worry about our list
	//	being cut up (without the transaction memory from changing, which would cause us to rerun anyway)...

	uint64_t sourceLists[4] = { 0 };
	uint64_t destListHeads[4] = { 0 };

	if (props.moveAllocationLog) {
		uint64_t nextMoveSlotIndex = props.nextMoveSlotIndex;
		uint64_t nextMoveSize = allocLogToSize(moveAllocationLog);
		uint64_t sourceSize = allocLogToSize(props.currentAllocationLog);

		uint64_t destChooseBit = 0;

		bool growing = moveAllocationLog > props.currentAllocationLog;
		if (growing) {
			destChooseBit = props.moveAllocationLog;
		}
		else {
			destChooseBit = props.currentAllocationLog;
		}

		uint64_t sourceCount = growing ? 2 : 4;
		uint64_t destCount = growing ? 4 : 2;

		uint64_t destSlotIndex = growing ? nextMoveSlotIndex * 2 : nextMoveSlotIndex / 2;

		
		AtomicUnit* sourceUnits = nullptr;
		for (uint64_t i = 0; i < sourceCount; i++) {
			uint64_t sourceSlotIndex = nextMoveSlotIndex + i;
			if (sourceSlotIndex >= sourceSize) break;
			if (!sourceUnits) {
				sourceUnits = SafeReferenceSmallPointer32(&props.allocations[props.currentAllocationLog]);
				if (!sourceUnits) {
					return 2;
				}
			}
			HashSlot sourceSlot;
			Get_Bytes(
				sourceUnits,
				(void*)(sourceSlotIndex * sizeof(slot)),
				(unsigned char*)&slot,
				sizeof(slot)
			);
			sourceLists[i] = sourceSlot.listHead;
		}

		int64_t neededEntries = 0;
		for (uint64_t i = 0; i < sourceCount; i++) {
			uint64_t sourceList = sourceLists[i];
			int64_t neededEntriesCur = atomicHashTable_countListSize(oldListHead);
			if (neededEntriesCur < 0) return 1;
			neededEntries += neededEntriesCur;
		}

		if (neededEntries > state.needsListEntries) {
			// + 5 + 25%, so even if more entries are added, we will probably still have enough buffer to expand
			state.needsListEntries = (neededEntries + 5) * 5 / 4;
			return 2;
		}

		uint64_t usedNewListEntries = 0;
		for (uint64_t i = 0; i < sourceCount; i++) {
			uint64_t sourceList = sourceLists[i];
			if (sourceList == 0) continue;

			while (sourceList) {
				HashEntry* entry = SafeReferenceSmallPointer64(&sourceList);
				if (!entry) {
					return 1;
				}

				// Empty, no need to move it...
				if (!entry->value) continue;

				if (usedNewListEntries >= state.needsListEntries) {
					// Surely we ran out just because entries were added while we were iterating
					return 1;
				}

				uint64_t destIndex = (entry->hash >> (64 - destChooseBit)) & 0x3;
				uint64_t prevListHead = destListHeads[destIndex];

				uint64_t newListHead = state.entries[usedNewListEntries];
				state.entries[usedNewListEntries] = 0;
				usedNewListEntries++;
				state.listsToFree[newBit] = destListHeads[newBit] = newListHead;

				HashEntry* newListEntry = SafeReferenceSmallPointer64(&newListHead);
				if (!newListEntry) {
					// Should be impossible, how could this have been freed already?
					// TODO: We should free anything we put in destListHeads already here... because right now we leak all of that...
					return 2;
				}
				newListEntry->hash = entry->hash;
				// value can be safely moved, as anything that might remove it will have to operate at least with a get
				//	on the transaction loop, which means either we will get a retry upon calling insert, or they will...
				newListEntry->value = entry->value;
				newListEntry->nextEntry = *prevListHead;
			}

			// So, set the two new slots in moveUnits to destListHeads, and then advance 
			// (AND IMPORTANTLY! Always set! Even if we set 0, this is important, as we may need to wipe out previous entries)
		}

		for (uint64_t i = 0; i < destCount; i++) {
			uint64_t destSlotIndexCur = destSlotIndex + i;

			// Okay, so... the indexes inside of destListHeads (i) doesn't linearily map as offsets to destSlotIndex.
			//	Those indexes are just the bits choosen, which may need to be mapped [3, 0, 1, 2], [2,3,0,1], [1,2,3,0] or [0,1,2,3]
			uint64_t destListHead = destListHeads[(destSlotIndexCur % destCount)];
			setIntents[setIntentCount++] = { props.moveAllocationLog, destSlotIndexCur, destListHead };
		}

		// Finish the move, if its done
		if (nextMoveSlotIndex + sourceCount >= sourceSize) {
			props.currentAllocationLog = props.moveAllocationLog;
			props.moveAllocationLog = 0;
			props.nextMoveSlotIndex = 0;
		}
	}


	// Add to setIntents, deduping/solving multiple writes, and then apply everything finally

	HashEntry* entry = state.entry;
	
	uint64_t targetIndex = entry->hash >> (64 - props.currentAllocationLog);

	// If we are moving it, then make sure our insert piggybacks on the insert
	bool foundIntent = false;
	for (uint64_t i = 0; i < setIntentCount; i++) {
		if (setIntents[i].allocLog == props.currentAllocationLog && setIntents[i].allocIndex == targetIndex) {
			foundIntent = true;
			entry->nextEntry = setIntents[i].listHead.listHead;
			setIntents[i].listHead = state->entrySmallPointer;
			setIntents[i].freeAll = false;
		}
	}

	if (!foundIntent) {
		AtomicUnit* units;
		uint64_t allocUsedLog = props.currentAllocationLog;
		if (props.moveAllocationLog && targetIndex < nextMoveSlotIndex) {
			// It is in the already moved memory
			targetIndex = entry->hash >> (64 - props.moveAllocationLog);
			allocUsedLog = props.moveAllocationLog;
		}
		units = SafeReferenceSmallPointer32(&props.allocations[allocUsedLog]);
		if (!unit) return 1;
		
		uint32_t prevEntryPointer = nullptr;
		Get_uint32_t(
			units,
			(void*)targetIndex,
			(unsigned char*)&prevEntryPointer,
			sizeof(uint32_t)
		);
		entry->nextEntry = prevEntryPointer;

		setIntents[setIntentCount++] = { allocLogUsed, targetIndex, state->entrySmallPointer };
	}


	AtomicUnit* unitsPerAllocLog[ALLOCATION_COUNT] = { 0 };

	// Set values
	for (uint64_t i = 0; i < setIntentCount; i++) {
		EntrySetIntent set = setIntents[i];
		
		if (!unitsPerAllocLog[set.allocLog]) {
			uint64_t alloc = props[set.allocLog];
			AtomicUnit* allocUnits = SafeReferenceSmallPointer64(&alloc);
			if (!allocUnits) {
				return 1;
			}
			unitsPerAllocLog[set.allocLog] = allocUnits;
		}
		AtomicUnit* units = unitsPerAllocLog[set.allocLog];

		int result = Set_uint32_t(
			context,
			insert,
			units,
			(void*)(set.allocIndex * sizeof(uint32_t)),
			set.listHead.listHead
		);
		if (result != 0) {
			return result;
		}
	}

	// Update changes props
	CASSERT(sizeof(TransactionProperties) % sizeof(uint32_t) == 0);
	uint32_t* origP = (void*)&originalProps;
	uint32_t* newP = (void*)&props;
	uint64_t size = sizeof(TransactionProperties) / sizeof(uint32_t);
	for (uint64_t i = 0; i < size; i++) {
		if (origP[i] == newP[i]) continue;
		int result = Set_uint32(
			context,
			insert,
			this->zeroAllocation,
			(void*)(i * sizeof(uint32_t)),
			newP[i]
		);
		if (result != 0) {
			return result;
		}
	}

	int finalResult = finish(context);
	if (finalResult != 0) {
		return finalResult;
	}

	// Now the source lists have moved (confirmed), so we can free them (instead of the destination lists)
	for (uint64_t i = 0; i < 4; i++) {
		state->listsToFree[i] = sourceLists[i];
	}

	if (usedAllocationToAdd) {
		state->allocationToAdd = nullptr;
	}

	return 0;
}
int AtomicHashTable_insert(AtomicHashTable* this, uint64_t hash, uint64_t valueSmallPointer) {
	InsertState state = { 0 };

	state.this = this;
	state.entrySmallPointer = AllocateAsSmallPointer(sizeof(HashEntry), &state.entry);
	
	state.entry->prevEntry = 0;
	state.entry->hash = hash;
	state.entry->value = valueSmallPointer;
	state.entry->nextEntry = 0;

	uint64_t lastAllocationSize = 0;
	uint64_t lastEntriesCount = 0;

	int result = 0;
	while (true) {
		result = TransactionQueue_ApplyWrite(
			&this->transactions,
			state,
			atomicHashTable_insertTransaction,
			this,
			atomicHashTable_applyChange
		);

		bool hasMetaRequest = false;
		if (state.needsAllocationOfSize != lastAllocationSize) {
			hasMetaRequest = true;
			if (state.allocationToAdd) {
				DereferenceSmallPointer(state.allocationToAdd);
				state.allocationToAdd = nullptr;
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
				}
				free(state.entries);
				state.entries = nullptr;
			}
			state.entries = malloc(sizeof(uint64_t*) * state.needsListEntries);
			for (uint64_t i = 0; i < state.needsListEntries; i++) {
				void* p;
				state.entries[i] = AllocateAsSmallPointer(sizeof(HashEntry), &p);
				if (!state.entries[i]) {
					// TODO: We should free entries if we allocated any, otherwise we leak it...
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
		state.allocationToAdd = nullptr;
	}

	if (state.entries) {
		for (uint64_t i = 0; i < lastEntriesCount; i++) {
			DereferenceSmallPointer(state.entries[i]);
		}
		free(state.entries);
		state.entries = nullptr;
	}

	return result;
}

todonext
// Remember, we use the high bits as the index, that way it adding 1 more bits either overlaps, or is the neighbor

int AtomicHashTable_find(
	AtomicHashTable* this,
	uint64_t hash,
	void* callbackContext,
	// May return results that don't equal the given hash, so a deep comparison should be done on the value
	//	to determine if it is the one you want.
	// (and obviously, may call this callback multiple times for one call)
	void(*callback)(void* callbackContext, uint64_t valueSmallPointer)
);

todonext
// Probably never resize below allocLogToSize(logMinEntryCount)
int AtomicHashTable_remove(
	AtomicHashTable* this,
	uint64_t hash,
	void* callbackContext,
	// On true, removes the value from the table
	bool(*callback)(void* callbackContext, uint64_t valueSmallPointer)
);