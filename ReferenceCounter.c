#include "ReferenceCounter.h"
#include "environment.h"
#include "AtomicHelpers.h"

#include "Timing.h"

#include "bittricks.h"

uint64_t log2RoundUp(uint64_t v) {
	return log2(v * 2 - 1);
}


SmallPointerTable this = { 0 };
SmallPointerTable* debug_getPointerTable() {
	return &this;
}

uint64_t allocSize(uint64_t allocIndex) {
	return 1ll << allocIndex;
}

// (index can be beyond the end of our memory, we will just wrap it around.
SmallPointerEntry* getEntry(uint64_t* index) {
	uint64_t indexValue = *index = *index % ((1 << this.allocationsCreated) - 1);
	uint64_t allocationIndex = log2(indexValue + 1);
	uint64_t indexInAllocation = indexValue % allocSize(allocationIndex);
	
	return (SmallPointerEntry*)&this.allocations[allocationIndex][indexInAllocation];
}

// Returns false on error
bool incrementFilledCount() {

	uint64_t filledCount = InterlockedIncrement64(&this.filledEntries);

	uint64_t allocationsCreated = this.allocationsCreated;
	uint64_t availableCount = (1 << allocationsCreated) - 1;

	// At exactly one value, or above another. This means one thread will try to expand, and if that fails,
	//	all threads will start trying later on.
	bool resizeNow = (
		filledCount == availableCount / 4
		|| filledCount > availableCount / 2
	);
	if (!resizeNow) return true;

	uint64_t requiredAllocations = (log2RoundUp(filledCount) + 1);
	if (requiredAllocations > BITS_IN_SMALL_POINTER) {
		return false;
	}

	for (uint64_t allocIndex = allocationsCreated; allocIndex < requiredAllocations; allocIndex++) {
		InterlockedIncrement64(&this.mallocCalls);
		uint64_t size = (1ll << allocIndex) * sizeof(SmallPointerEntry_Extended);
		SmallPointerEntry* newArray =  malloc(size);
		if (!newArray) {
			return false;
		}
		// TODO: We should probably slowly initialize this, as needed, because in theory a large malloc can be constant time,
		//	but a large memset will be linear.
		memset(newArray, 0, size);
		if (InterlockedCompareExchangePointer(&this.allocations[allocIndex], newArray, nullptr) != nullptr) {
			free(newArray);
			// The other thread probably allocated more than just 1, so jump to the next allocation actually needed.
			allocIndex = this.allocationsCreated;
		}

	}

	while (true) {
		allocationsCreated = this.allocationsCreated;
		if (allocationsCreated >= requiredAllocations) return true;
		if ((uint64_t)InterlockedCompareExchange64(&this.allocationsCreated, requiredAllocations, allocationsCreated) == allocationsCreated) {
			return true;
		}
	}
	return true;
}

bool tryClaimIndex(uint64_t* seekIndex) {
	SmallPointerEntry* pointer = getEntry(seekIndex);
	SmallPointerEntry claimedValue;
	claimedValue.pointer = nullptr;
	claimedValue.referenceCount = 1;
	while (true) {
		SmallPointerEntry value = *pointer;
		if (value.pointer || value.referenceCount) {
			return false;
		}
		if (InterlockedCompareExchangeStruct128(
			pointer,
			&value,
			&claimedValue
		)) {
			return true;
		}
	}
}


uint64_t rotateBits(uint64_t value) {
	uint64_t output = 0;
	uint64_t mask = 1;
	for (int bit = 0; bit < BITS_IN_SMALL_POINTER; bit++) {
		output = output | ((value & mask) >> bit << (BITS_IN_SMALL_POINTER - 1 - bit));
		mask = mask << 1;
	}
	return output;
}

uint64_t getMaxIterations() {
	uint64_t maxIterations = (1ll << this.allocationsCreated);
	if (maxIterations < 1000) {
		maxIterations = 1000;
	}
	return maxIterations;
}

// returns false on failure
bool claimIndex(uint64_t* seekIndex) {
	InterlockedIncrement64(&this.searchStarts);

	uint64_t maxIterations = getMaxIterations();
	uint64_t iterationCount = 0;
	while (true) {
		uint64_t iterationIndex = InterlockedIncrement64(&this.searchIterations);
		if (iterationCount % 2 != 0) {
			// So... it is likely that pointers allocated sequentially in time will be freed (or not freed) together.
			//	And so... we don't want to just iterate linearily, as that will always eventualyl reach these regions.
			//	So we mix up our iteration. However... it also means we allocate according to this pattern... so this is
			//	probably of limited benefit...
			// TODO: We should probably just use a linked list...
			iterationIndex = rotateBits(iterationIndex);
		}
		*seekIndex = iterationIndex;
		if (tryClaimIndex(seekIndex)) {
			return true;
		}
		iterationCount++;
		// So... we could fail if there are few entries, but they are constantly being allocated
		//	and unallocated. So, we iterate over twice as many as the maximum. If we fail after that...
		//	then what can we do?
		if (iterationCount >= maxIterations) {
			uint64_t newMaxIterations = getMaxIterations();
			if (newMaxIterations > maxIterations) {
				maxIterations = newMaxIterations;
				iterationCount = 0;
			}
			else {
				// Should be impossible, we should eventually find a space...
				return false;
			}
		}
	}
}


uint64_t AllocateAsSmallPointerRaw(uint64_t size, void** pointerOut, const char* fileName, uint64_t line) {
	if (!incrementFilledCount()) {
		return 0;
	}

	InterlockedIncrement64(&this.mallocCalls);

	void* pointer = malloc(size);
	if (!pointer) {
		return 0;
	}
	memset(pointer, 0, size);

	uint64_t index = (uint64_t)pointer;
	if (!claimIndex(&index)) {
		free(pointer);
		return 0;
	}

	SmallPointerEntry_Extended* entry = (void*)getEntry(&index);
	// Eh... we could do an interlocked compare exchange, but... this should always be exclusively ours,
	//	as it should have a reference (so it can't be freed or reused), and no pointer (so it can't be
	//	referenced by anyone else).
	entry->pointer = pointer;
	entry->size = size;
	entry->fileName = fileName;
	entry->line = line;
	entry->smallPointerNumber = index;

	*pointerOut = pointer;

	//printf("alloc %llu\n", index);

	return (uint64_t)(index + 1);

}

void* ReferenceSmallPointerInner(uint64_t smallPointer) {

	//TimeBlock(ReferenceGetEntry, 
	smallPointer--;
	SmallPointerEntry* entry = getEntry(&smallPointer);
	//);

	void* p = nullptr;
#ifdef FAST_UNSAFE_CHANGES
	//TimeBlock(ReferenceInterlocked,
		entry->referenceCount++;
		p = entry->pointer;
	//);
#else
	//TimeBlock(ReferenceInterlocked,
	while (true) {
		SmallPointerEntry value = *entry;
		if (!value.pointer) {
			// Should be impossible, it means it has been freed. This is a bad case, as someone else could
			//	have reused the entry before we got here, resulting in them referencing a random pointer...
			return nullptr;
		}
		SmallPointerEntry newValue = value;
		newValue.referenceCount++;
		if (InterlockedCompareExchangeStruct128(
			entry,
			&value,
			&newValue
		)) {
			p = newValue.pointer;
			break;
		}
	}
	//);
#endif
	return p;
}
void* ReferenceSmallPointer(uint64_t smallPointer) {
	//TimeBlock(ReferenceSmallPointer,
		void* p = ReferenceSmallPointerInner(smallPointer);
	//);
	return p;
}

void* SafeReferenceSmallPointer64(uint64_t* smallPointer) {
	// Verifying the ref we have is still in the allocation is essential. This proves it is of a certain size,
	//	and because we always have a ref on it, it proves it will stay that size (otherwise it may be for a reused
	//	small pointer and be of a different size).

	uint64_t initial = *smallPointer;
	if (!initial) return nullptr;
	void* p = ReferenceSmallPointer(initial);
	if (initial != *smallPointer) {
		DereferenceSmallPointer(initial);
		p = nullptr;
	}
	return p;
}
void* SafeReferenceSmallPointer32(uint32_t* smallPointer) {
	// Verifying the ref we have is still in the allocation is essential. This proves it is of a certain size,
	//	and because we always have a ref on it, it proves it will stay that size (otherwise it may be for a reused
	//	small pointer and be of a different size).

	uint32_t initial = *smallPointer;
	if (!initial) return nullptr;
	void* p = ReferenceSmallPointer(initial);
	if (initial != *smallPointer) {
		DereferenceSmallPointer(initial);
		p = nullptr;
	}
	return p;
}


void DereferenceSmallPointerInner(uint64_t smallPointer) {
	smallPointer--;
	SmallPointerEntry* entry = getEntry(&smallPointer);

#ifdef FAST_UNSAFE_CHANGES
	entry->referenceCount--;
	if (entry->referenceCount == 0) {
		free(entry->pointer);
		entry->pointer = nullptr;
	}
#else
	while (true) {
		SmallPointerEntry value = *entry;
		if (!value.pointer) {
			// Should be impossible, it means it has been freed. This is a bad case, as someone else could
			//	have reused the entry before we got here, resulting in us dereferencing a random pointer...
			OnError(3);
			return;
		}
		SmallPointerEntry newValue = value;
		if (newValue.referenceCount == 0) {
			// Too many frees. Should be impossible...
			OnError(3);
			return;
		}
		newValue.referenceCount--;
		if (newValue.referenceCount == 0) {
			newValue.pointer = nullptr;
		}
		if (InterlockedCompareExchangeStruct128(
			entry,
			&value,
			&newValue
		)) {
			// We WILL leak the pointer here, if our thread dies.
			if (value.referenceCount == 1) {
				//printf("free %llu\n", smallPointer);
				free(value.pointer);
			}
			return;
		}
	}
#endif
}
void DereferenceSmallPointer(uint64_t smallPointer) {
	//TimeBlock(DereferenceSmallPointer, 
	DereferenceSmallPointerInner(smallPointer);
	//);
}

uint64_t Debug_GetAllReferenceCounts() {
	uint64_t allRefsCount = 0;
	for(uint64_t j = 0; j < this.allocationsCreated; j++) {
		for(uint64_t i = 0; i < allocSize(j); i++) {
			SmallPointerEntry_Extended* entry = (void*)&this.allocations[j][i];
			uint64_t count = entry->referenceCount;
			if (count) {
				allRefsCount += count;
			}
		}
	}
	return allRefsCount;
}

uint64_t Debug_GetReferences() {
	uint64_t allRefsCount = 0;
	for (uint64_t j = 0; j < this.allocationsCreated; j++) {
		for (uint64_t i = 0; i < allocSize(j); i++) {
			SmallPointerEntry_Extended* entry = (void*)&this.allocations[j][i];
			uint64_t count = entry->referenceCount;
			if (count) {
				allRefsCount++;
			}
		}
	}
	return allRefsCount;
}

SmallPointerTable* Debug_GetSmallPointerTable() {
	return &this;
}

PointersSnapshot* Debug_GetPointersSnapshot() {
	PointersSnapshot* snapshot = malloc(sizeof(PointersSnapshot));
	snapshot->entryCount = Debug_GetReferences();
	snapshot->entries = (void*)malloc(snapshot->entryCount * sizeof(SmallPointerEntry_Extended));
	uint64_t nextIndex = 0;
	for(uint64_t j = 0; j < this.allocationsCreated; j++) {
		for(uint64_t i = 0; i < allocSize(j); i++) {
			SmallPointerEntry_Extended* entry = (void*)&this.allocations[j][i];
			uint64_t count = entry->referenceCount;
			if (count) {
				uint64_t index = nextIndex++;
				if(index >= snapshot->entryCount) {
					// Reference counts change during snapshot
					return nullptr;
				}
				snapshot->entries[index] = *entry;
			}
		}
	}
	if(nextIndex != snapshot->entryCount) {
		// Reference counts change during snapshot
		return nullptr;
	}

	return snapshot;
}

void Debug_DeallocatePointersSnapshot(PointersSnapshot* info) {
	free(info->entries);
	info->entries = nullptr;
	info->entryCount = 0;
	free(info);
}