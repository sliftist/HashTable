#include "MemPool.h"
#include "RefCount.h"
#include "MemPoolImpls.h"

#ifdef __cplusplus
extern "C" {
#endif

// TODO: Oh... if we actually spread out moves, over many operations, we can just use tombstone deleting, and
//  then trigger moving once our effective fill rate hits a certain limit, and move from the same size to the
//  same size allocation... because moving can filter out tombstones...
//  - Moving over time requires better checking for consistency during a move
//      - Because values don't move insertions are trivial to check to see if they've been inserted
//      - Oh... but deletions... how can we tell if a block has been moved? Hmm... maybe...
//          we just shouldn't spread out moves? It is okay if insertions and deletions are sometimes slower,
//          as long as finds are fast...
//todonext;
// Hmm... how can finds work with moves? Messing up block starts is a problem...
//  - Oh, we can have two reserved null values, one that is swapped with non 0 values, and a different
//  one for 0 values.
//      - And then... this actually helps support partial moves, because it means...
//          - If you iterate over both blocks (one maybe partially moved), then you will always get every entry,
//              because for something to be removed from the first block it will have to already be in the second...
//todonext;
// Oh.. but deletes... how can we be sure something is deleted? I guess... we can tell if the move
//  range overlapped with both blocks we searched, and if it didn't, then we are fine. Oh, and
//  we can tell if either of our blocks are borderline moved
//  - Actually... maybe we should just make it so if anything finds any moved null values
//  while searching it should help with moving just until its block gets moved, and then
//  it should try to access the list again.
//      - Although even then, delete will still need to do extra checks...

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

todonext;
// Oh... so, if we see isNull flag set, and it isn't BASE_NULL1, then we help with the move
//  (BASE_NULL1 is the tombstone marker)


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
    //todonext
    // Ugh... this transaction is becoming a clusterfuck. Basically, we have to fix the obvious error when try to
    //  swap out OutsideReferences, which clearly won't work due to recurrence, AND we need to also make it
    //  abstract enough to handle the HashValue.nullSwappingWith, so we can make find's checks of nullSwappingWith
    //  transactional, allowing find to correctly retry if any swaps (well really deletes of swapped values) may have happened.
    // Oh... the ref to delete has be an outside reference too, but a reference we own. And then when we wipe out targetToDelete,
    //  we have to always put a null reference in nullToDelete, so we always have a unique value
    //  - The intermediate outside reference won't be good enough to give us our delete version HOWEVER, anyone that sees
    //      it will have to apply it, and in applying it they will have to acquire the reference, which will protect us
    //      from hanging writes in application. And then after we apply it our delete version WILL be good enough, so... it will all work...
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

// When deleting we replace values with BASE_NULL
// When moving we replace 0s with BASE_NULL2
// When moving we replace non-0s with BASE_NULL3
// We default new allocations to BASE_NULL4

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
    // Below here is initialized to 0
    uint64_t indexToSwapPlusOne;
    OutsideReference valueToSwap;
} DeleteStateInner;
#pragma pack(pop)
CASSERT(sizeof(DeleteStateInner) == 16);

#pragma pack(push, 1)
typedef struct {
    //AtomicHashTableBase*
    OutsideReference newAllocation;
    // Must be initialized with sourceSlot as GetNextNull
    MoveStateInner moveState;
} Operation;
#pragma pack(pop)

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // This actually also resides in our value mem pool, and THAT is the true owner of this.
    //  (we could duplicate it, but there's no need)
    //AtomicHashTableBase*
    OutsideReference currentAllocation;
    // This is replaced with a GetNextNull, never with 0
    // Operation*
    OutsideReference currentOperation;
} TransactionState;
#pragma pack(pop)
CASSERT(sizeof(TransactionState) == 16);



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
    VALUE_SIZE,\
    VALUE_SIZE + sizeof(HashValue), \
    deleteValue, \
    MemPoolFixedDefault(), \
}
#pragma pack(push, 1)
typedef struct {
    // (in bytes)
    uint64_t VALUE_SIZE;
    uint64_t HASH_VALUE_SIZE;

    void (*deleteValue)(void* value);

    MemPoolFixed findValuePool;

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
