#include "MemPool.h"
#include "RefCount.h"

#ifdef __cplusplus
extern "C" {
#endif


//todonext
// During contention holes appear in continous blocks in the hash table. So... clearly some of our code is wrong.
//  Maybe just print every time we completely remove an element?
//  The problem is probably our fast retry code failing to check for something, allowing it to continue even after we should
//  restart the outer loop...

//todonext
// I think with our new code we can actually make this work without a transaction. When we swap
//  a value down, duplicating it, we need to mark it (with just a bit) saying it is being duplicated.
//  Then, as find needs to keep a copy of all values anyway... it can just check all of the previous
//  values if it sees that flag. It needs to store all values anyway, as it can't just call find,
//  because of retries (and because of our own downward swaps).
//  - So setting the move flag is always fine, all it does is makes finds slower
//  - Wiping out the value we are deleting is fine, we have a reference to it, so we know the pointer
//      is the correct value, and once the delete request comes in we can delete all versions until we finish
//      the delete call
//  - Having duplicates is fine, because of the move flag
//  - Resurrecting an old value is fine, because we we store a delete flag inside the value

//todonext
// Store a delete flag inside of values (and keep checking for moved values after we set it, so we know
//  it is in a leaf now). That way, if an old move command resurrects a value, it will be marked
//  as deleted, and so not break anything...

//todonext
// Hmm... can moving work without a transaction? Maybe deleting can, because deleting actually won't
//  resurrect a value, as it always moves into existing slots, but inserting by definition creates a value,
//  which makes... well, moving requires multiple threads all coming to the same decision of where to insert.
//  But if one thread is slow and happens to see an insert the other threads didn't... that could be a problem...
// Perhaps... oh, I guess before moving, we could go through all the empty slots and mark them as used?

//todonext
// Hmm... but what if we delete, while it is being moved? Oh... then we will just know to retry the delete.
//  Just like a find during a move, or deletion, will know to retry. And an insertion will retry if a move,
//  or deletion happens. And we even retry a delete if a delete happens.

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
    // If isNull then this is a skip value, so we have to keep searching
    OutsideReference value;
} AtomicSlot;
#pragma pack(pop)

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    todonext
    // Ugh... this transaction is becoming a clusterfuck. Basically, we have to fix the obvious error when try to
    //  swap out OutsideReferences, which clearly won't work due to recurrence, AND we need to also make it
    //  abstract enough to handle the HashValue.nullSwappingWith, so we can make find's checks of nullSwappingWith
    //  transactional, allowing find to correctly retry if any swaps (well really deletes of swapped values) may have happened.
    union {
        OutsideReference nullToDelete;
        // We take ownership of this, because we have to, or else we can't ensure the pointer won't be recurrent.
        InsideReference* refToDelete;
    };
    // targetToDelete is set to 0 when it has been applied
    OutsideReference* targetToDelete;
} DeleteInsertState;
#pragma pack(pop)
CASSERT(sizeof(DeleteInsertState) == 16);


#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // slotsCount is immutable (obviously)
    uint64_t slotsCount;
    uint64_t logSlotsCount;

    // ATTENTION! ALL transition to Null, or 0 have to go through deleteState, EXCEPT for moves,
    //  which are allowed to forcefully remove data, and force function to try to fix it post-hoc.
    // This is needed so inserts can tell if it is possible a value was deleted before them
    //  (making them no longer connect to their base index, and so not findable).
    //  It is not possible to tell this by iterating, as it is possible that as we iterate
    //  values are added, and then deleted, following our iteration.
    DeleteInsertState deleteState;

    // Values and the underlying memory valuePool takes from are both taken from the rest of the allocation.
    AtomicSlot* slots;
    // Value count equals slot count. Because we always have extra slots,
    //  and if we run out of values we should first run out of slotsReserved.
    MemPoolHashed* valuePool;

} AtomicHashTableBase;
#pragma pack(pop)


// TODO: Make reference counting optional, as sometimes we won't even need it



#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    union {
        //todonext
        // Actually, rewrite all of these to have AtomicHashTable2 context, and complete a full operation.

        uint64_t type;
        struct {
            // type == 1, replace (dest with source)
            // Sets special flag on source, clearing it on dest
            OutsideReference* source;
            OutsideReference* dest;
            InsideReference* sourceRef;
            InsideReference* destRef;
        };
        struct {
            // type == 2, delete
            //  (clears special flag)
            OutsideReference* target;
            InsideReference* targetRef;
            // Sets this to 1
            uint64_t* deleteFlagToSet;
        };
        struct {
            // type == 3, move
            //  (redirects the old ref to the new ref, and then puts the new ref into dest)
            //  Sets the special flag on the dest ref, and leaves it set if it is possible
            //      other transactions were applied before the set happened.
            // Dest must be 0 before we set it
            OutsideReference* dest;
            InsideReference* oldRef;
            InsideReference* newRef;
        };
        struct {
            // type == 4, insert
            uint64_t hash;
            OutsideReference valueRef;
        };
    };
} TransChange;
#pragma pack(pop)

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // First, so we can overlap our .value with a uint64_t inside a union.
    uint64_t value;
    uint64_t version;
} AtomicUnit2;
#pragma pack(pop)


#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // TODO: We can move some bits from destOffset to nextSourceSlot, which will reduce the maximum continous slot count,
    //  which is probably fine, and we should return errors after the continous slot count reaches a certain limit anyway.
    // TODO: We could also do this by swapping the Operation out, allowing operations to be write once. But using the 128 bit
    //  atomic state should be faster?
    uint32_t nextSourceSlot;
    int32_t destOffset;
    // Set to 0 when we increment nextSourceSlot, and then read in while reading nextSourceSlot.
    // Unique, because the destination is populate with GetNextNull
    //  As we only read this one, and only populate it once per nextSourceSlot, moves
    //  can't introduce hanging inserts.
    OutsideReference destSlot;
} MoveStateInner;
#pragma pack(pop)
CASSERT(sizeof(MoveStateInner) == 16);

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // Below here is initialized to 0
    uint64_t indexToSwapPlusOne;
    OutsideReference valueToSwap;
} DeleteStateInner;
#pragma pack(pop)
CASSERT(sizeof(DeleteStateInner) == 16);

#pragma pack(push, 1)
typedef struct {
    // type = 1, move table (does entire table move)
    OutsideReference newAllocation;
    // Must be initialized with sourceSlot as GetNextNull
    MoveStateInner moveState;
} Operation;
#pragma pack(pop)

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // This actually also resides in our value mem pool, and THAT is the true owner of this.
    //  (we could duplicate it, but there's no need)
    OutsideReference currentAllocation;
    // This is replaced with a GetNextNull, never with 0
    // Operation*
    OutsideReference currentOperation;
} TransactionState;
#pragma pack(pop)
CASSERT(sizeof(TransactionState) == 16);



// Should be initialized as:
//  AtomicHashTable2 table = AtomicHashTableDefault(VALUE_SIZE, void (*deleteValue)(void* value));

#define AtomicHashTableDefault(VALUE_SIZE, deleteValue) { \
    VALUE_SIZE,\
    VALUE_SIZE + sizeof(HashValue), \
    2, \
    deleteValue, \
    { {}, BASE_NULL }, \
    MemPoolFixedDefault(), \
}
#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // (in bytes)
    uint64_t VALUE_SIZE;
    uint64_t HASH_VALUE_SIZE;

    uint64_t nextUniqueId;
    void (*deleteValue)(void* value);

    TransactionState state;
    MemPoolFixed operationPool;

    // As slotsReserved isn't inside of allocationsState (there isn't enough room),
    //  it means there is a slight race condition. If one thread wants to shrink the time
    //  between seeing slotsReserved and changing allocationsState is not safe, and therefore
    //  slotsReserved can change drastically. If it changes enough to require an allocation change
    //  then the exchange will fail, but it can change enough to not require an allocation change,
    //  for example, from the shrink threshold to just under the grow threshold. This should not
    //  break the code as long as our grow threshold is below 50% (so shrinking instead of growing
    //  is still valid).
    uint64_t slotsReserved;

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
