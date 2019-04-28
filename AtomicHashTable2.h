#include "MemPool.h"
#include "RefCount.h"
#include "MemPoolImpls.h"

#ifdef __cplusplus
extern "C" {
#endif


#pragma pack(push, 1)
typedef struct {
    uint64_t deleted;
    // If this is set to non-0, it means we are swapping with this null value (that is below us),
    //  and therefore, we might be duplicated (so find has to take extra steps to screen out duplicates).
    OutsideReference nullSwappingWith;
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
    // TODO: We can move some bits from destOffset to nextSourceSlot, which will reduce the maximum continous slot count,
    //  which is probably fine, and we should return errors after the continous slot count reaches a certain limit anyway.
    // TODO: We could also do this by swapping the Operation out, allowing operations to be write once. But using the 128 bit
    //  atomic state should be faster?
    uint32_t nextSourceSlot;
    int32_t destOffset;

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

int AtomicHashTable2_find(
    AtomicHashTable2* self,
    uint64_t hash,
    void* callbackContext,
    // May return results that don't equal the given hash, so a deep comparison should be done on the value
	//	to determine if it is the one you want.
	// (and obviously, may call this callback multiple times for one call, but not more than one time per call
    //  for the same value, assuming no value is added to the table more than once).
    void(*callback)(void* callbackContext, void* value)
);

uint64_t DebugAtomicHashTable2_reservedSize(AtomicHashTable2* self);
uint64_t DebugAtomicHashTable2_allocationSize(AtomicHashTable2* self);

#ifdef __cplusplus
}
#endif
