#pragma once

#include "environment.h"

// TODO: We should probably rewrite this to force the users to give a memory location of where
//	the pointer is stored, and use that to maintain a reference. THEN, we would be able to reach
//	in their memory address and change the underlying small pointer! Of course... if their pointer
//	is stored in dynamic memory, we would need to at least know about when that memory was deallocated
//	(ideally we would just handle that memory), and we would need to store linked lists for each allocations,
//	and stuff... but it would all be possible...
//	- This would require helper functions to duplicate references from an existing reference (a uint64_t*),
//		to a new one, and also to move references, and of course to free references.


// NOTE: Usually references shouldn't be gained or released inside transaction queue operations.
//	Sometimes it will be required, but attempts should be made to cache needed reference changes
//	until outside the transaction queue loop.


// Okay, so when finding slots, hash the original pointer. And if that fails, maybe hash the hash...
//	After a certain amount of failures, we have to iterate (as our hash is inevitably going to be cyclic,
//	so we can't use it to check every entry).
//	- This works nicely, because finding the first small pointer can be one way.
// Also, adding more allocations is nice, because we don't need to copy anything, we just suddenly have more space
// Removing allocations is... pretty much not going to happen. In theory we could fold down small pointers,
//	but they would almost certainly collide, unless we reach a logarithm usage compared to our total allocation,
//	which pretty much can't happen.


#ifdef __cplusplus
extern "C" {
#endif



#define BITS_IN_SMALL_POINTER 32


#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
	void* pointer;
	uint64_t referenceCount;
} SmallPointerEntry;
#pragma pack(pop)
CASSERT(sizeof(SmallPointerEntry) == 16);

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
	void* pointer;
	uint64_t referenceCount;
	uint64_t size;
	const char* fileName;
	uint64_t line;
	uint64_t smallPointerNumber;
} SmallPointerEntry_Extended;
#pragma pack(pop)


#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
	// 1 << index is the size of the allocation.
	// index = 0 is reserved, as it redirects to zeroAllocation
	//	Other allocations

	// Because we never (and can never) deallocate, it allows filledEntries to be stored independently from allocationsCreated.
	//	(otherwise we couldn't as then a hanging thread could suddenly decide we can downsize to 0, when we definitely can't).
	volatile uint64_t filledEntries;
	// Always <= BITS_IN_SMALL_POINTER
	volatile uint64_t allocationsCreated;

	// TODO:
	// Oh... we can make a rolling queue of 32 bit integers to keep track of the free small pointers,
	//	and just stick that memory at the beginning of any of our allocations...
	// Still can't handle downsizing allocations though...
	// However, this DOES let us tightly pack (roughly)
	// So... the start/end indexes are just to make finding the start/end faster.
	//	We atomically take entries/add new freed entries to the start/end. We also do 64 bit swaps
	//	to make sure it is actually the end/start
	// Oh, so... start/end should just be "last seen start/end", which are 128 bit structs with ids,
	//	that always increase, so we use the start/end of the latest change to start.

	//	(we never deallocate these)
	//	(each size is 1 << index, and because we never deallocate, we use every allocation, using the requested index
	//		to find which allocation that index is in).
	SmallPointerEntry_Extended* allocations[BITS_IN_SMALL_POINTER];

	// Used to iterate when searching for empty allocations.
	volatile uint64_t searchIterations;

	// Just for tracking
	volatile uint64_t searchStarts;
	volatile uint64_t mallocCalls;

} SmallPointerTable;
#pragma pack(pop)


SmallPointerTable* debug_getPointerTable();

#define AllocateAsSmallPointer(size, pointerOut) AllocateAsSmallPointerRaw(size, pointerOut, __FILE__, __LINE__)

// We are not going to expose as "ConvertToSmallPointer". That would require having the caller guarantee they don't pass a pointer
//	twice, as if they were to pass the same pointer twice, it would guarantee a double free.
// Returns 0 only on error.
uint64_t AllocateAsSmallPointerRaw(uint64_t size, void** pointerOut, const char* file, uint64_t line);


// These leverage an interesting reality of pointers. While observing a pointer in a shared memory location it may
//	be freed before you can gain a reference to it (this is unavoidable). AND, that memory location may then be populated
//	by another item, that happens to have to same smallPointer, and so be indistinguishable. However... IF the original
//	call was expect to give a pointer of an underlying type back (say a certain size, or length prefixed, or whatever type
//	of data the user is storing), then we can give the user that new memory location (if it has the same small pointer value).
//	Even though technically it isn't the pointer the user asked for, it will work in all cases (and as WE dereference the small pointer,
//	it is indistinguisable from us just referencing the pointer slightly later).
//	- Of course if the user checks some auxilary memory to determine the type of the pointer, then calls us, they may get a different
//		type of pointer returned, but this is because there is a race in their code, which is present even if they just had a raw pointer
//		and dereferenced it.
void* SafeReferenceSmallPointer64(uint64_t* smallPointer);
void* SafeReferenceSmallPointer32(uint32_t* smallPointer);

//void* ReferenceSmallPointer(uint64_t smallPointer);

void DereferenceSmallPointer(uint64_t smallPointer);

uint64_t Debug_GetAllReferenceCounts();
SmallPointerTable* Debug_GetSmallPointerTable();

typedef struct {
	SmallPointerEntry_Extended* entries;
	uint64_t entryCount;
} PointersSnapshot;

PointersSnapshot* Debug_GetPointersSnapshot();

typedef struct {
	// 0 = no change
	// 1 = removed
	// 2 = added
	// 3 = changed ref count ()
	int changeType;
	SmallPointerEntry_Extended entry;
	int64_t refCountDelta; // (currrent - previous)
} PointersSnapshotDelta;
PointersSnapshotDelta Debug_ComparePointersSnapshot(PointersSnapshot* info);
void Debug_DeallocatePointersSnapshot(PointersSnapshot* info);


#ifdef __cplusplus
}
#endif