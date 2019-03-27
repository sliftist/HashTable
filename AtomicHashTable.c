#include "AtomicHashTable.h"

todonext
// Get this compiling, and then fix the many single threaded bugs, and then fix the many multithreaded races, and then
//	make sure it is fast, and then... make sure it is fast in the kernel, and then finally, use it in the kernel.

#pragma pack(push, 1)
typedef union {
	uint64_t value;
	struct {
		uint64_t index : 32;
		uint64_t allocation : 8;
	};
} DataIndex;
#pragma pack(pop)


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
		Get_uint32_t(
			this->zeroAllocation,
			&this->base.allocations[index.allocation - 1],
			&allocRef
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
		AtomicUnit* units = SafeReferenceSmallPointer32(&alloc);
		if (!units) {
			continue;
		}
		uint64_t size = 1ll << i;
		for (int k = 0; k < size; k++) {
			uint32_t listHead = units[i].value;
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
	}


	TransactionProperties props = { 0 };
	// Base sets use an allocation index of 0, and so can just be set raw
	return Set_Bytes(context, insertWrite, this->zeroAllocation,
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
	uint64_t* entries;

	// (output)
	// May be sparsely populated, (so we do have to iterate all of them).
	uint64_t listsToFree[4];

	// (output)
	uint64_t countRemoved;

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
	Get_uint64_t(
		state->this->zeroAllocation,
		&state->this->base.currentFillCount,
		&currentFillCount
	);

	currentFillCount -= state.count;

	return Set_uint64_t(
		context,
		insert,
		state->this->zeroAllocation,
		&state->this->base.currentFillCount,
		currentFillCount
	);
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

	// We decrement currentFillCount outside of the main transaction, as we remove values outside of transactional memory.
	//	This insures anything we unallocate WILL actually be removed from the transaction count. The resize after removing
	//	is then delayed... but only by 1 call (or the number of threads), which is fine...
	uint64_t newFillCount = props.currentFillCount;
	if (state->isAdd) {
		newFillCount++;
	}
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
				sourceUnits = SafeReferenceSmallPointer32(&props.allocations[props.currentAllocationLog - 1]);
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

	if (state->change.isAdd) {
		HashEntry* entry = state->change.entry;
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
			uint32_t allocUsed = props.allocations[allocUsedLog - 1];
			units = SafeReferenceSmallPointer32(&allocUsed);
			if (!unit) return 1;

			uint32_t prevEntryPointer = units[targetIndex].value;
			entry->nextEntry = prevEntryPointer;

			setIntents[setIntentCount++] = { allocLogUsed, targetIndex, state->entrySmallPointer };

			DereferenceSmallPointer(allocUsed);
		}
	}
	else {
		/*
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

		typedef struct {
			AtomicHashTable* this;

			StateChangeCore change;	
		*/
		

		uint64_t targetIndex = state->change.hash >> (64 - props.currentAllocationLog);

		AtomicUnit* units;
		uint64_t allocUsedLog = props.currentAllocationLog;
		if (props.moveAllocationLog && targetIndex < nextMoveSlotIndex) {
			// It is in the already moved memory
			targetIndex = entry->hash >> (64 - props.moveAllocationLog);
			allocUsedLog = props.moveAllocationLog;
		}
		uint32_t allocUsed = props.allocations[allocUsedLog - 1];
		units = SafeReferenceSmallPointer32(&allocUsed);
		if (!unit) return 1;

		// We add a reference to this
		uint32_t lastPointerWithNoValue = 0;

		uint32_t listHeadPointer = units[targetIndex].value;
		while (listHeadPointer) {
			HashEntry* entry = SafeReferenceSmallPointer32(&listHeadPointer);
			if (!entry) break;

			uint64_t valuePointer = entry->value;
			void* value = SafeReferenceSmallPointer64(&valuePointer);
			bool shouldDelete = state->change.callback(state->change.callbackContext, value);
			if (shouldDelete) {
				DereferenceSmallPointer(valuePointer);
				entry->value = 0;
			}
			DereferenceSmallPointer(valuePointer);

			if (!entry->nextEntry && !entry->value) {
				state->countRemoved++;
				lastPointerWithNoValue = listHeadPointer;
				SafeReferenceSmallPointer32(&lastPointerWithNoValue);
			}

			uint32_t nextListHeadPointer = entry->nextEntry;
			DereferenceSmallPointer(listHeaderPointer);
			listHeadPointer = nextListHeadPointer;
		}

		if (lastPointerWithNoValue) {
			listHeadPointer = units[targetIndex].value;
			// Eh... 10 seems good? It is really arbitrary though...
			#define lastEntriesSize 10;
			uint32_t lastEntries[lastEntriesSize] = { 0 };
			uint32_t lastEntriesAddIndex = 0;
			while (listHeadPointer) {
				lastEntries[lastEntriesAddIndex] = listHeadPointer;
				lastEntriesAddIndex = (lastEntriesAddIndex + 1) % lastEntriesSize;
				HashEntry* entry = SafeReferenceSmallPointer32(&listHeadPointer);
				if (!entry) break;
				uint32_t nextListHeadPointer = entry->nextEntry;
				DereferenceSmallPointer(listHeaderPointer);
				listHeadPointer = nextListHeadPointer;
			}

			// Now iterate over the last X backwards...
			bool prevHasNoValue = false;
			for (uint64_t i = 1; i <= lastEntriesSize; i++) {
				uint64_t ii = (lastEntriesAddIndex - i + lastEntriesSize) % lastEntriesSize;
				uint32_t entryPointer = lastEntries[ii];

				HashEntry* entry = SafeReferenceSmallPointer32(&entryPointer);

				// Should be true...
				if (entry->nextEntry != lastPointerWithNoValue) {
					DereferenceSmallPointer(entryPointer);
					break;
				}

				// So this is the second last entry
				Dereference(lastPointerWithNoValue);
				// And the second deref should free the last entry
				Dereference(lastPointerWithNoValue);

				// If the last entry now has a value, stop
				if (entry->value) {
					DereferenceSmallPointer(entryPointer);
					break;
				}

				// Now try to free this entry,
				lastPointerWithNoValue = entryPointer;
				state->countRemoved++;
			}

			DereferenceSmallPointer(lastPointerWithNoValue);

			// We deallocated the head, now wipe it out (if this fails... it is okay. The list becomes slower, because it has
			//	to dereference before knowing the value is really empty, but that is fine...)
			if (lastPointerWithNoValue == units[targetIndex].value) {
				setIntents[setIntentCount++] = { allocLogUsed, targetIndex, 0 };
			}
		}

		DereferenceSmallPointer(allocUsed);
	}

	// Update changes props
	CASSERT(sizeof(TransactionProperties) % sizeof(uint32_t) == 0);

	uint32_t* origP = (void*)&originalProps;
	uint32_t* newP = (void*)&props;
	uint64_t size = sizeof(TransactionProperties) / sizeof(uint32_t);
	for (uint64_t i = 0; i < size; i++) {
		if (origP[i] == newP[i]) continue;
		int result = Set_uint32_t(
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

	// Now the source lists have moved (confirmed), so we can free them (instead of the destination lists)
	for (uint64_t i = 0; i < 4; i++) {
		state->listsToFree[i] = sourceLists[i];
	}

	if (usedAllocationToAdd) {
		state->allocationToAdd = nullptr;
	}

	return 0;
}

int AtomicHashTable_mutate(
	AtomicHashTable* this,
	uint64_t hash,
	uint64_t valueSmallPointer,
	void* removeContext,
	bool(*shouldRemove)(void* removeContext, void* value)
) {
	InsertState state = { 0 };

	state.this = this;
	state.change.isAdd = !shouldRemove;
	if (state.change.isAdd) {
		state.change.entrySmallPointer = AllocateAsSmallPointer(sizeof(HashEntry), &state.entry);

		state.change.entry->prevEntry = 0;
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

	if (result != 0) {
		DereferenceSmallPointer(state.entrySmallPointer);
	}
	
	// So... even on hard failures we still deallocated, so this should be fine...
	if (state.countRemoved > 0) {
		DecrementState decState = { 0 };
		decState.this = this;
		decState.count = state.countRemoved;

		int decResult = TransactionQueue_ApplyWrite(
			&this->transactions,
			decState,
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
	void* callbackContext;
	void(*callback)(void* callbackContext, void* value);
} FindState;

int AtomicHashTable_findTransaction(
	FindState* state
) {
	AtomicHashTable* this = state->this;
	uint64_t hash = state->hash;
	void* callbackContext = state->callbackContext;
	void(*callback)(void* callbackContext, void* value) = state->callback;

	if (!this->currentAllocationLog) {
		return;
	}

	uint32_t allocSmallPointer = this->allocations[this->currentAllocationLog - 1];
	AtomicUnit* units = SafeReferenceSmallPointer32(&allocSmallPointer);
	if (!units) {
		return 1;
	}
	
	uint64_t index = hash >> (64 - props.currentAllocationLog);
	uint32_t listHead = units[index].value;

	// So... the retries in this loop are funny. Other retries are when a function detects contention in the synced data,
	//	and so returns. The transaction queue should be able to figure out where the contention happened (but won't, as
	//	there is no need). HOWEVER, these retries are for contention outside of the synced data, so the transaction
	//	queue won't know why it is retrying (it will still work, but... you know...).
	while (listHead) {
		HashEntry* entry = SafeReferenceSmallPointer32(&listHead);
		if (!entry) {
			return 1;
		}
		uint64_t valueSmallPointer = entry->value;
		uint32_t nextHead = entry->nextEntry;
		DereferenceSmallPoitner(listHead);
		void* value = SafeReferenceSmallPointer64(&valueSmallPointer);
		if (value) {
			callback(callbackContext, value);
			DereferenceSmallPointer(valueSmallPointer);
		} // else it is just an empty value, this is fine...
		listHead = nextHead;
	}
}

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
	state.callbackContext = callbackContext;
	state.callback = callback;
	return TransactionQueue_RunGetter(
		&this->transactions,
		&state,
		AtomicHashTable_findTransaction,
		this,
		atomicHashTable_applyChange
	);
}

int AtomicHashTable_insert(AtomicHashTable* this, uint64_t hash, uint64_t valueSmallPointer) {
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