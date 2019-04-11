//todonext
// Alright, 3rd try. So... allocate for transactions, swap them to a global "currentTransaction" (which can be 64 bit if we never
//  free transactions, and just reuse them).

// So... even with this, we still need to increment reference counts... Crap... Maybe...
//  Eh... maybe densely populate it, then we only need to ref count the big allocations?

// So... densely populated

// When removing something, swap a value down.
//  Take the farthest away value that can be swapped without putting it past its start.
//      (If there is no value that can do this, it means we are done, and the hole is fine)
//      - We could iterate from both far and close at the same time... but... whatever, we should just make
//          sure the table doesn't get very dense, and it will be fine...
//  Then, after we move this value, do the same thing to the new hole we created
// All of these changes get put in the transaction, and so it might be as big as the entire array...


// So... reference counts with the allocation tables, fairly vanilla reference counting... and if something leaks we just reuse that
//  allocation when we encounter it in the future... (and we start references at 1, and deallocate by freeing that first reference)

// Transactions should have a reference count beside the global one, and an internal one. They should get the global reference first,
//  and then the internal one, and when the global transaction is removed that thread should decrement that value from
//  the internal reference count...

// Wait... doesn't this recover our linked list concept? Which we could use with our hash table, to make hash table inserts
//  no longer require arbitrary length transactions, and probably make transactions faster...

// So... we need both the concept of external/internal reference counting AND write transaction queues

// Also... let's just go back to linked lists? Because... we are going to need to get a reference (CAS) to the memory allocation
//  regardless of how we implement it... and so if the first entry of the linked list is inline... then single entry
//  linked lists will be just as fast as without linked lists, and more entries will just multiple the number of
//  compare and swaps, which isn't that bad...

// So... if values are tied with an enty (or even allocated just with that entry), and never moved between
//  entries (even though entries can be moved around), then:
//  A -> B -> C
//  If we have a reference to all entries, and then try to swap A.next from B to C, this is safe. Because...
//  B won't/can't be reused (as we have a reference to it, it's value has been removed, and we never change
//  entry values), so either the change hasn't happened, or it has, and then A.next will never be B again.

// So... transactions... will have to have references to values, AND anything trying to fulfill the transaction
//  has to add references to the outer reference in the transaction, as well as an outer reference to the transaction
//  itself.

#include "TransApply.h"
#include "RefCount.h"

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    TransApply transaction;

    union {
        OutsideReference transactionToApply;
        AtomicUnit2 transactionToApplyUnit;
    };
} AtomicHashTable2;
#pragma pack(pop)

// We take ownership of value on insertion, and free it on removing it from the hashtable. So...
//  if it is added twice, or used after removal, there will be a problem. You can try to remove
//  it multiple times though, we will reference count it internally.
// Returns 0 on success, and > 0 on failure
int AtomicHashTable2_insert(AtomicHashTable2* self, uint64_t hash, void* value);

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

int AtomicHashTable_remove(
	AtomicHashTable2* self,
	uint64_t hash,
	void* callbackContext,
	// On true, removes the value from the table
	bool(*callback)(void* callbackContext, void* value)
);