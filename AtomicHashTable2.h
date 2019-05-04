#include "MemPool.h"
#include "RefCount.h"
#include "MemPoolImpls.h"

#ifdef __cplusplus
extern "C" {
#endif


#pragma pack(push, 1)
typedef struct {
    uint64_t hash;

    // After the end is filled with the value
} HashValue;
#pragma pack(pop)

#pragma pack(push, 1)
typedef struct {
    // HashValue*
    // Usually this is a real pointer, 0, BASE_NULL (the tombstone).
    //  If it is any other form of isNull, it means it has been moved, and we should go try to apply
    //  the move algorithm until we finish moving everything in our block.
    OutsideReference value;
} AtomicSlot;
#pragma pack(pop)

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    uint32_t nextSourceSlot;

	// TODO: We can actually make sourceBlockStart be more than 64 bits, and then make nextSourceSlot be an offset instead (that
	//	is less 32 bits), that way we can support more than 2^32 values (but less than 2^32 block size, which is fine, as high block
	//	sizes yield really bad behavior, and anything more than like... 2^10 block size should fail).
    //  (UINT32_MAX to indicate it has no value)
    // TODO: Also... if the offset is small, we will have bits to cache information on the last dest
    //  block we iterated over, allowing for faster searching of dest index locations (making it so
    //  cases with 100% collisions can be O(1), and other cases faster too)
	uint32_t sourceBlockStart;

    // If BASE_NULL then we skip setting it
    //  We own this sourceValue (it is moved correctly), so to move
    //  it into the slot we need to get a reference and create a new outside reference, and then
    //  the thread that swaps this out for 0 has to destroy this outside reference.
    OutsideReference sourceValue;
} MoveStateInner;
#pragma pack(pop)
CASSERT(sizeof(MoveStateInner) == 16);




#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // slotsCount is immutable (obviously)
    // TODO: Try setting these to const, and seeing if it makes the compiler optimize
    uint64_t slotsCount;
    uint64_t logSlotsCount;

    // Values and the underlying memory valuePool takes from are both taken from the rest of the allocation.
    AtomicSlot* slots;
    // Value count equals slot count. Because we always have extra slots,
    //  and if we run out of values we should first run out of slotsReserved.
    MemPoolHashed* valuePool;

    // Initialized to 0
    MoveStateInner moveState;
    
    //AtomicHashTableBase*
    OutsideReference newAllocation;
    
    uint64_t slotsReserved;
    uint64_t slotsReservedWithNulls;

    bool finishedMovingInto;

} AtomicHashTableBase;
#pragma pack(pop)



// Should be initialized as:
//  AtomicHashTable2 table = AtomicHashTableDefault(VALUE_SIZE, void (*deleteValue)(void* value));

#define AtomicHashTableDefault(VALUE_SIZE, deleteValue) { \
    MemPoolFixedDefault(), \
    VALUE_SIZE,\
    VALUE_SIZE + sizeof(HashValue), \
    deleteValue, \
}
#pragma pack(push, 1)
typedef struct {
    MemPoolRecycle findValuePool;

    // (in bytes)
    uint64_t VALUE_SIZE;
    uint64_t HASH_VALUE_SIZE;

    void (*deleteValue)(void* value);

    OutsideReference currentAllocation;

} AtomicHashTable2;
#pragma pack(pop)






// We take ownership of value on insertion, and free it on removing it from the hashtable. So...
//  if it is added twice, or used after removal, there will be a problem. You can try to remove
//  it multiple times though, we will reference count it internally.
// Returns 0 on success, and > 0 on failure
int AtomicHashTable2_insert(AtomicHashTable2* self, uint64_t hash, void* value);

int AtomicHashTable2_remove(
	AtomicHashTable2* self,
	uint64_t hash,
	void* callbackContext,
	// On true, removes the value from the table
    //  May be called multiple times for the value for one call.
	bool(*callback)(void* callbackContext, void* value)
);


// internal
int atomicHashTable2_findFull(
    AtomicHashTable2* self,
    uint64_t hash,
    void* callbackContext,
    // May return results that don't equal the given hash, so a deep comparison should be done on the value
	//	to determine if it is the one you want.
	// (and obviously, may call this callback multiple times for one call, but not more than one time per call
    //  for the same value, assuming no value is added to the table more than once).
    void(*callback)(void* callbackContext, void* value)
);

#define atomicHashTable_getSlotBaseIndex(alloc, hash) ((hash) >> (64 - (alloc)->logSlotsCount))

// internal
__forceinline bool atomicHashTable2_fastNotContains(
    AtomicHashTable2* self,
	uint64_t hash
) {
    if(self->currentAllocation.valueForSet) {
        InsideReference* tableRef = Reference_Acquire(&self->currentAllocation);
        AtomicHashTableBase* table = (AtomicHashTableBase*)Reference_GetValueFast(tableRef);
        if(!table->newAllocation.valueForSet) {
            uint64_t index = atomicHashTable_getSlotBaseIndex(table, hash);

            uint64_t loops = 0;
            while(true) {
                OutsideReference value = table->slots[index].value;
                if(value.valueForSet == 0) {
					Reference_ReleaseFast(&self->currentAllocation, tableRef);
                    return 1;
                }
                if(!value.isNull) {
                    // This is actually... not dangerous. IS_VALUE_MOVED checks for null, and value will always be inside our own
                    //	allocation, so this is safe. We only really need to acquire the reference IF the user has pointers inside
                    //	the structure, and wants to dereference them (in which case we need to make sure deletes don't happen in other allocations
                    //	while are are accessing it here).
                    uint64_t quickHash = ((HashValue*)((byte*)value.pointerClipped + InsideReferenceSize))->hash;
                    if(quickHash == hash) {
						Reference_ReleaseFast(&self->currentAllocation, tableRef);
                        return 0;
                    }
                } else {
                    const OutsideReference x = { 0, 0, 1 };
                    if(value.valueForSet != x.valueForSet) {
                        // A move occured since we started, so we cannot continue.
						Reference_ReleaseFast(&self->currentAllocation, tableRef);
                        return 0;
                    }
                }
                index = (index + 1) % table->slotsCount;
                if(index == 0) {
                    loops++;
                    if(loops > 10) {
						Reference_ReleaseFast(&self->currentAllocation, tableRef);
                        // Too many loops around the table...
                        OnError(9);
                        return 0;
                    }
                }
            }
        }
        Reference_Release(&self->currentAllocation, tableRef);
    }
    return 0;
}

// This is optimized for the case when we don't find any matches, as this really needs to be fastest when used
//  as a filter, and other cases will presumably spend more time in processing a match than it takes to find a match.
//  However, if you know most calls will find matches (> 50%), then AtomicHashTable2_findWithMatches will
//  be slightly faster (20% faster for cases that find a match, taking them from 5x slower than the non match
//  case, to be only 4x slower).
__forceinline int AtomicHashTable2_find(
    AtomicHashTable2* self,
    uint64_t hash,
    void* callbackContext,
    // May return results that don't equal the given hash, so a deep comparison should be done on the value
	//	to determine if it is the one you want.
	// (and obviously, may call this callback multiple times for one call, but not more than one time per call
    //  for the same value, assuming no value is added to the table more than once).
    void(*callback)(void* callbackContext, void* value)
) {
    if(atomicHashTable2_fastNotContains(self, hash)) { return 0; }
    return atomicHashTable2_findFull(self, hash, callbackContext, callback);
}
inline int AtomicHashTable2_findWithMatches(
    AtomicHashTable2* self,
    uint64_t hash,
    void* callbackContext,
    void(*callback)(void* callbackContext, void* value)
) {
    return atomicHashTable2_findFull(self, hash, callbackContext, callback);
}

uint64_t DebugAtomicHashTable2_reservedSize(AtomicHashTable2* self);
uint64_t DebugAtomicHashTable2_allocationSize(AtomicHashTable2* self);

#ifdef __cplusplus
}
#endif
