#include "ReferenceCounter.h"
#include "environment.h"
#include "AtomicHelpers.h"

// https://graphics.stanford.edu/~seander/bithacks.html
#define LT(n) n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n
static const char LogTable256[256] =
{
	-1, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
	LT(4), LT(5), LT(5), LT(6), LT(6), LT(6), LT(6),
	LT(7), LT(7), LT(7), LT(7), LT(7), LT(7), LT(7), LT(7)
};
uint64_t log2(uint64_t v) {
	unsigned r;
	uint64_t t, tt, ttt;

	if (ttt = v >> 32) {
		if (tt = ttt >> 16) {
			if (t = tt >> 8) {
				r = 56 + LogTable256[t];
			} else {
				r = 48 + LogTable256[tt];
			}
		} else {
			if (t = ttt >> 8) {
				r = 40 + LogTable256[t];
			} else {
				r = 32 + LogTable256[ttt];
			}
		}
	} else {
		if (tt = v >> 16) {
			if (t = tt >> 8) {
				r = 24 + LogTable256[t];
			} else {
				r = 16 + LogTable256[tt];
			}
		} else {
			if (t = v >> 8) {
				r = 8 + LogTable256[t];
			} else {
				r = LogTable256[v];
			}
		}
	}
	return r;
}

uint64_t log2RoundUp(uint64_t v) {
	return log2(v * 2 - 1);
}


SmallPointerTable this = { 0 };
SmallPointerTable* debug_getPointerTable() {
	return &this;
}

// (index can be beyond the end of our memory, we will just wrap it around.
SmallPointerEntry* getEntry(uint64_t* index) {
	uint64_t indexValue = *index = *index % ((1 << this.allocationsCreated) - 1);
	uint64_t allocationIndex = log2(indexValue + 1);
	uint64_t indexInAllocation = indexValue % (1ll << (allocationIndex));
	
	return &this.allocations[allocationIndex][indexInAllocation];
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
		uint64_t size = (1ll << allocIndex) * sizeof(SmallPointerEntry);
		SmallPointerEntry* newArray =  malloc(size);
		// TODO: We should probably slowly initialize this, as needed, because in theory a large malloc can be constant time,
		//	but a large memset will be linear.
		memset(newArray, 0, size);
		if (!newArray) {
			return false;
		}
		if (InterlockedCompareExchangePointer(&this.allocations[allocIndex], newArray, nullptr) != nullptr) {
			free(newArray);
			// The other thread probably allocated more than just 1, so jump to the next allocation actually needed.
			allocIndex = this.allocationsCreated;
		}
	}

	while (true) {
		uint64_t allocationsCreated = this.allocationsCreated;
		if (allocationsCreated >= requiredAllocations) return true;
		if (InterlockedCompareExchange64(&this.allocationsCreated, requiredAllocations, allocationsCreated) == allocationsCreated) {
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

uint64_t AllocateAsSmallPointer(uint64_t size, void** pointerOut) {
	if (!incrementFilledCount()) {
		return 0;
	}

	InterlockedIncrement64(&this.mallocCalls);

	void* pointer = malloc(size);
	if (!pointer) {
		return 0;
	}

	uint64_t index = (uint64_t)pointer;
	if (!claimIndex(&index)) {
		free(pointer);
		return 0;
	}

	SmallPointerEntry* entry = getEntry(&index);
	// Eh... we could do an interlocked compare exchange, but... this should always be exclusively ours,
	//	as it should have a reference (so it can't be freed or reused), and no pointer (so it can't be
	//	referenced by anyone else).
	entry->pointer = pointer;

	*pointerOut = pointer;

	return (uint64_t)(index + 1);

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

void* ReferenceSmallPointer(uint64_t smallPointer) {
	smallPointer--;
	SmallPointerEntry* entry = getEntry(&smallPointer);
	
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
			return newValue.pointer;
		}
	}
}
void DereferenceSmallPointer(uint64_t smallPointer) {
	smallPointer--;
	SmallPointerEntry* entry = getEntry(&smallPointer);

	while (true) {
		SmallPointerEntry value = *entry;
		if (!value.pointer) {
			// Should be impossible, it means it has been freed. This is a bad case, as someone else could
			//	have reused the entry before we got here, resulting in us dereferencing a random pointer...
			return;
		}
		SmallPointerEntry newValue = value;
		if (newValue.referenceCount == 0) {
			// Too many frees. Should be impossible...
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
				free(value.pointer);
			}
			return;
		}
	}
}