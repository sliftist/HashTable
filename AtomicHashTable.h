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
typedef struct {
	// (not including the allocation we are moving to)

	uint64_t currentAllocationLog;
	uint64_t currentFillCount;

	uint64_t moveAllocationLog;
	uint64_t nextMoveSlotIndex;

	// size of pointers is 1 << (log - 1), so 1 with log 1 (and hardcoded 0 with log 0),
	//	where log 1 is at index 0.
	uint32_t allocations[ALLOCATION_COUNT];

} TransactionProperties;
#pragma pack(pop)


#pragma pack(push, 1)
typedef struct {
	uint64_t hash;
	uint64_t value : BITS_IN_SMALL_POINTER;
	uint64_t nextEntry : BITS_IN_SMALL_POINTER;
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

int AtomicHashTable_insert(AtomicHashTable* self, uint64_t hash, uint64_t valueSmallPointer);
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



//todnext
// Hmm... actually... so if the client passes us a pointer, how can we tell if that pointer is alive?
//	So... we need to create a pointer reference counting thing. And... we can use the TransactionQueue to do it!
// But... at the end of the day, this basically becomes a hash table? Although, if we use it to remap pointers to
//	smaller integers... then we never have collisions.
// Although... this remap table, perhaps we could do this without the transaction queue? Hmm... That would be faster...
//	So the remap table... searching it for new entries can become expensive? Oh... well, we should keep it highly compacted, that's easy.
// And it would need its own allocations table. Huh...

// So, our underlying slots will be filled with linked list nodes (which we allocate in a small wrapper,
//	then convert to small pointers via our reference counter).
// So... our linked list nodes will reference each other, only requiring one node in the table... Hmm...
//	which is a problem.
//	(Because remember, what about hanging removals?)
//	- Hmm... what if we use transaction ids, oh, internal transaction ids! Not the TransactionQueue ones,
//		and then use those to make sure we only try to remove stuff added before us!
// We still need to be able to atomically remove nodes... hmm... and atomically insert nodes...
//	- Insert seems harder...
//		- So... I guess we just insert to the start of the linked list
//			- IF the start is no longer attached to the hash table, then our insert will rerun anyway,
//				as the start pointer will be resident in the Transaction memory, and so changes to it
//				will invalidate our write (and our read, which it basically becomes if we only read transaction
//				memory to get the pointer and then write to non transaction memory).
//		- So we hook up the node to point to the current head, and then set the current head to be the new pointer
//			- Because our primary mutation is to transaction data, this is safe, as if this fails we just dereference the new pointer,
//				and if it succeeds all of our changes are already in transaction data.
//		OH! Actually! With insertions, we should cache the linked list node pointer outside of the insert function (so outside
//			the apply loop), so we can reuse it in the case of failure and inevitable retry. This will mean insert will only do a constant
//			number of allocations! And actually, this also just wraps our function to get the "caller" to pass the linked list node...
//			which is basically what we wanted anyway...
//			- And also, possibly do frees in a similar way, having a few slots on the stack, moving memory their we intend to free,
//				and then not freeing any more than a certain amount (which should be fine), that way both allocations and frees
//				happen outside the transaction loop!
//	- Remove pointer
//		- Just null out the pointer, AND while the last entry has no value, remove it from the previous
//			(hmm... so I guess make this a doubly linked list?)
//			- This only leaks badly if we have a lot of collision, and the first added entry never destruct.
//				BUT! When copying to a larger allocation we can filter out empty entries, so it will only leak so badly...
//		- Even if the remove operation fails, it is fine that we removed anything, as removes will eventually happen, and aren't conditional in our case.



// Resizing
//	- So... we have to split up the nodes by their new hashes. We can work somewhat off the transaction data while doing this...
//		- If any part of setting the two new nodes and wiping out the old node fails, we have to dereference everything we created.
//	- We will need to create all new lists nodes when moving the data. Because unfortunately we will be very much destroying the old lists
//		connections while we are working, we just can't split a list without breaking iteration on it...
//		- So we will need to free the new lists if the move fails...
// AND REMEMBER! When getting the pointer from the small pointer from any shared location, we have to verify
//	the small pointer in the original location is still the same. Otherwise it may have been unallocated between
//	the original read, and the time we referenced it.
//	- And of course, this means when freeing it we must wipe it out in the original location.
//	- And also, when moving it we should store an additional reference to it, and only free the reference
//		upon atomically wiping it out from the original location.
// Upon getting close to the amount we need to resize, we should claim the "premature" resize
//	flag, allocate the new allocation, and initialize it, OUTSIDE the transaction loop, and then apply it inside the loop.
//	- We still need backup code to allocate inside the transaction loop (which we need to test by disabling the premature
//		resize code), but most the of the time this single claimed resize should work, both reducing contention,
//		and keeping write calls a consistent speed.




#ifdef __cplusplus
}
#endif
