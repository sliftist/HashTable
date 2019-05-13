#include "MemPool.h"
#include "RefCount.h"
#include "MemPoolImpls.h"

#ifdef __cplusplus
extern "C" {
#endif

// All functions return > 0 on error. You might start to encounter a lot of errors if your system runs out of memory.
//  Technically it is possible to grow a table big enough that it can't be shrunk, however in practice I don't see this happening.
//  To grow we need to hold on 2 steps of allocations at once, so if it is ever possible to hold those two allocations, it will probably
//  be possible in the future.
// TODO: Change resizing behavior, so if we get stuck at a larger size we... recover somehow?
// TODO: Add support for greater table capacities when memory usage becomes very high. Or maybe just allow changing the capacity target per table?

// TODO: We need to add support for a dedicated malloc thread, so we can have a malloc prepared ahead of time,
//  which lets us greatly reduce our worst case behavior.

// TODO: High value sizes allow low slot count, and whenever the core count is much higher than the slot count it is
//  much more likely for our retry loops to hit their limits. So... we should add some kind of minimum slot count?
//  Or... something...


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
typedef struct {
    union {
        struct {
            uint32_t sourceIndex;

            // Set to UINT32_MAX before we have a dest
            uint32_t destIndex;
        };
        uint64_t valueForSet;
    };
} MoveStateInner;
#pragma pack(pop)
CASSERT(sizeof(MoveStateInner) == 8);



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
    deleteValue, \
}
#pragma pack(push, 1)
typedef struct {
    MemPoolRecycle findValuePool;

    // (in bytes)
    uint64_t VALUE_SIZE;

    void (*deleteValue)(void* value);

    OutsideReference currentAllocation;

    uint64_t destructed;

    // Disabling this instrumentation makes our fastest case about 30% faster. But it is already basically a NOOP, and
    //  checking if our hash algorithm is bad in realtime is very useful...
    #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
    uint64_t searchStarts;
    uint64_t searchLoops;
    #endif

} AtomicHashTable2;
#pragma pack(pop)






// We take ownership of value on insertion, and free it on removing it from the hashtable. So...
//  if it is added twice, or used after removal, there will be a problem. You can try to remove
//  it multiple times though, we will reference count it internally.
// If it has been inserted 0 is returned, if non-0 is returned, the value will not have been inserted.
// Returns 0 on success, and > 0 on failure
int AtomicHashTable2_insert(AtomicHashTable2* self, uint64_t hash, void* value);

// This function may have removed some of the values when an error is returned, or non of the values may have been removed.
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
    const OutsideReference BASE_NULL_CPLUSPLUS = { 0, 0, 1 };
    if(self->currentAllocation.valueForSet) {
        InsideReference* tableRef = Reference_AcquireFast(&self->currentAllocation);
        AtomicHashTableBase* table = (AtomicHashTableBase*)Reference_GetValueFast(tableRef);
        if(!table->newAllocation.valueForSet) {
            uint64_t index = atomicHashTable_getSlotBaseIndex(table, hash);
            uint64_t startIndex = index;

            uint64_t loops = 0;
            while(true) {
                #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
                InterlockedIncrement64((LONG64*)&self->searchLoops);
                #endif
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
                    uint64_t quickHash = Reference_GetHash((InsideReference*)value.pointerClipped);
                    if(quickHash == hash) {
						Reference_ReleaseFast(&self->currentAllocation, tableRef);
                        return 0;
                    }
                } else {
                    // BASE_NULL, but different because C++ doesn't like our macro
                    if(value.valueForSet != BASE_NULL_CPLUSPLUS.valueForSet) {
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
        Reference_ReleaseFast(&self->currentAllocation, tableRef);
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
    #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
    InterlockedIncrement64((LONG64*)&self->searchStarts);
    #endif
    if(atomicHashTable2_fastNotContains(self, hash)) {return 0;}
    return atomicHashTable2_findFull(self, hash, callbackContext, callback);
}
inline int AtomicHashTable2_findWithMatches(
    AtomicHashTable2* self,
    uint64_t hash,
    void* callbackContext,
    void(*callback)(void* callbackContext, void* value)
) {
    #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
    InterlockedIncrement64((LONG64*)&self->searchStarts);
    #endif
    return atomicHashTable2_findFull(self, hash, callbackContext, callback);
}

// Eventually causes future calls to other functions (with the exception of find, as a dtored table is basically just empty),
//  and removes every entry in the table.
void AtomicHashTable2_dtor(
    AtomicHashTable2* self
);


uint64_t DebugAtomicHashTable2_reservedSize(AtomicHashTable2* self);
uint64_t DebugAtomicHashTable2_allocationSize(AtomicHashTable2* self);

#ifdef __cplusplus
}
#endif
