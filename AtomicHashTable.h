#pragma once

#include "TransactionQueue.h"
#include "TransactionQueueHelpers.h"
#include "ReferenceCounter.h"

//todonext
// So... how is the client even going to access the dynamic memory? It would be nice if we could disperse the memory
//	upon copying it... although... even for hash tables that is problematic when there are collisions.
// Maybe... we just do copy on write semantics...
// Although, still, we do want hash tables to be dispersed in some fashion... Ugh... or maybe we should just write a hashtable,
//	and forget about abstracting it...


#define ALLOCATION_COUNT 32

// (Because we store allocations as uint32_t)
CASSERT(BITS_IN_SMALL_POINTER <= (sizeof(uint32_t) * 8));

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
	// (not including the allocation we are moving to)

	uint64_t currentAllocationLog;
	uint64_t currentFillCount;

	uint64_t moveAllocationLog;
	uint64_t nextMoveSlotIndex;

	// size of pointers is 1 << (log - 1), so 1 with log 1 (and hardcoded 0 with log 0),
	//	where log 1 is at index 0.
	// HashSlot*
	uint32_t allocations[ALLOCATION_COUNT];

} TransactionProperties;
#pragma pack(pop)

#pragma pack(push, 1)
typedef struct {
	uint64_t hash;
	// Value must be a concrete value, for interlocked exchanges
	uint32_t value;// : BITS_IN_SMALL_POINTER;
	uint32_t nextEntry;// : BITS_IN_SMALL_POINTER;
} HashEntry;
#pragma pack(pop)

// Oh, we don't need atomic swaps here even?
//CASSERT(sizeof(HashEntry) == 16);

#pragma pack(push, 1)
typedef struct {
	uint32_t listHead : BITS_IN_SMALL_POINTER;
} HashSlot;
CASSERT(sizeof(HashSlot) == 4);
#pragma pack(pop)


#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
	// 1 << index is the size of the allocation.
	// index = 0 is reserved, as it redirects to zeroAllocation
	//	Other allocations

	union {
		TransactionProperties base;
		AtomicUnit zeroAllocation[(sizeof(TransactionProperties) + BYTES_PER_TRANSACTION - 1) / BYTES_PER_TRANSACTION];
	};

	TransactionQueue transactions;

} AtomicHashTable;
#pragma pack(pop)

#ifdef __cplusplus
extern "C" {
#endif



// (all functions return non-zero error codes on failure)

// Ctor isn't needed, just zero out the memory and it should be fine...
//void AtomicHashTable_ctor(AtomicHashTable* self);

int AtomicHashTable_dtor(AtomicHashTable* self);

// hash must have all bits filled, as we may use only the top bits, or only the low bits.
// REMEMBER! A pointer may only be added once. We take ownership of a pointer when it is added,
//	and free it upon removal of the value (this makes freeing pointers easier, as otherwise we would
//	need to allocate some memory to let us atomically return removes values, like we do with find,
//	which is slightly bad, as we should have to allocate more memory to free some memory).
//	So if something is added multiple times, on removal, we WILL double free it.
int AtomicHashTable_insert(AtomicHashTable* self, uint64_t hash, uint32_t valueSmallPointer);
int AtomicHashTable_find(
	AtomicHashTable* self,
	uint64_t hash,
	void* callbackContext,
	// May return results that don't equal the given hash, so a deep comparison should be done on the value
	//	to determine if it is the one you want.
	// (and obviously, may call this callback multiple times for one call)
	void(*callback)(void* callbackContext, void* value)
);


int AtomicHashTable_remove(
	AtomicHashTable* self,
	uint64_t hash,
	void* callbackContext,
	// On true, removes the value from the table
	bool(*callback)(void* callbackContext, void* value)
);


// Debug function to return the internal fill count
TransactionProperties DebugAtomicHashTable_properties(AtomicHashTable* self);


#ifdef __cplusplus
}
#endif
