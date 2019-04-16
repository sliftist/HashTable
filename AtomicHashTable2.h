#include "TransApply.h"
#include "RefCount.h"
#include "MemoryPool.h"


// Values rounded up to 64 bit increments, as we can't really store less
#define AtomicHashTableSlotSize(VALUE_SIZE) (sizeof(AtomicSlot) - sizeof(AtomicUnit2) + ((VALUE_SIZE) + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t) * 2)

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // (our move code has hardcoded assumptions that this only contains AtomicUnits, so... keep it that way, or change the move code)
    //  (also, it depends on ref being first, and valueUniqueId being second)
    OutsideReferenceAtomic ref;

    // This is a unique number created on insert of the value. This allows us to store duplicate values and then
    //  dedupe them before we return them, which allows inserts to be broken up into many transactions,
    //  which then lets each transactions have a fixed number of writes, which... makes memory reuse much easier.
    // 0 means there is no value
    // 1 means this should be skipped when searching (but searching shouldn't stop), and skipped when moving (don't move this).
    //  - If set to 1, ref should be destroyed.
    //  - We set this to 1 when we swap a value down during a deletion. We will keep swapping down
    //      to eventually actually delete a block (and so set this to 0), but to make sure each transaction
    //      has a constant amount of writes, we can't do the swaps in one transaction, so we need to
    //      just set the slot to a temporary "non use and non empty" state.

    // TODO: Oh, my idea about making this a unique number... I don't think it works. A value could be swapped, and then replaced,
    //  or a hole could appear below it. Either way, we need to retry on version changes, so... what is the point of this?
    //  So... move its two flags to hash, and reserve those hash values, increasing them to the next number.
    AtomicUnit2 valueUniqueId;

    AtomicUnit2 hash;

    // The values are variable size, depending on AtomicHashTableSlotSize/AtomicHashTable2.VALUE_SIZE
    AtomicUnit2 valueUnits;
} AtomicSlot;
#pragma pack(pop)

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // slotsCount is immutable (obviously)
    uint64_t slotsCount;
    // Because slots must be 16 byte aligned, and at the end
    uint64_t padding;

    // The class is allocated larger than AtomicHashTableBase, so &slots can be used as an array of slots, slotsCount in length.
    byte* slots;
} AtomicHashTableBase;
#pragma pack(pop)


// TODO: Make reference counting optional, as sometimes we won't even need it


// Should be initialized as:
//  AtomicHashTable2 table = AtomicHashTableDefault(VALUE_SIZE, void (*deleteValue)(void* value));

#define AtomicHashTableDefault(VALUE_SIZE, deleteValue) { VALUE_SIZE, deleteValue, TransApplyDefault(), { AtomicHashTableSlotSize(VALUE_SIZE) + sizeof(InsideReference) }, { VALUE_SIZE } }


#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // (in bytes)
    uint64_t VALUE_SIZE;

    void (*deleteValue)(void* value);

    TransApply transaction;

    MemoryPool valueInsertPool;
    MemoryPool valueFindPool;


    // Gain references to these and use Reference_ReplaceOutside when dealing with them
    OutsideReferenceAtomic currentAllocation;
    OutsideReferenceAtomic newAllocation;

    // index of the next slot in currentAllocation to move to newAllocation (if newAllocation has a value)
    AtomicUnit2 nextMoveSlot;

    uint64_t slotsReserved;
    // next unique value is this, +2
    uint64_t nextValueUniqueIdMinus2;

} AtomicHashTable2;
#pragma pack(pop)






// We take ownership of value on insertion, and free it on removing it from the hashtable. So...
//  if it is added twice, or used after removal, there will be a problem. You can try to remove
//  it multiple times though, we will reference count it internally.
// Returns 0 on success, and > 0 on failure
int AtomicHashTable2_insert(AtomicHashTable2* self, uint64_t hash, void* value);

int AtomicHashTable_remove(
	AtomicHashTable2* self,
	uint64_t hash,
	void* callbackContext,
	// On true, removes the value from the table
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