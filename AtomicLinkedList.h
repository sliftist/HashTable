#pragma once

#include "environment.h"

// The list head requires the user to manages its memory, but otherwise... we
//  manage the memory of all the entries inside us, making everything non-blocking and consistent.

// TODO: Make sure we do some calculations to calculate how long it will take moveVersion to wrap around
//  once we finialize our moving and hash table, and then add a comment with profiled time in the relevant places,
//  so we will know not to lower our minimum size, or reduce the bits in moveDest, etc...


// References must only ever exist in one place in memory, so there is only one location in memory referenceCount is,
//  ensuring it is always close the pointer, and therefore always consistent.
#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
	void* pointer;
    // TODO: Well... this seems a bit low. But... maybe it's fine, because... it is create, not access? Eh...
    // This has to be globally unique... :(
    uint64_t createIndex: 45;
    uint64_t markedForDestruction: 1;
	uint64_t referenceCount: 18;
} Reference;
#pragma pack(pop)

CASSERT(sizeof(Reference) == 16);

// MOVING
// If pointerMoved is set to true, it means we are either moving or something happened should you
//  should enter the move retry loop.
//  - BUT! Your reference is still valid, the storage location has just changed. So you have to go iterate to find it again.
// Also, you have to check for moving AFTER you finish looping too, because we might leave destructed nodes orphaned and
//  therefore not update their move versions, as if we leave them orphaned we have to keep their move versions or else
//  it will be impossible for old holders to release their references.

// May return nullptr
void* Reference_Acquire(Reference*);

void Reference_Release(Reference*, void* pointer, void** pointerToFree);

// The free happens when the last reference is released, not immediately
void Reference_ReleaseAndFree(Reference*, void* pointer, void** pointerToFree);


// (in the A -> B -> C case called when we see !B.value.pointer,
//  as Reference_MoveExclusive(&A.nextEntry, C.nextEntry), to try to erase the B entry slot)

// Is only possible IF the caller has acquired both references.
//  - Upon success sets pointerToFree to the original dest pointer
//  - So upon success it will return the pointer in dest in pointerToFree, as that pointer will have been clobbered
//      and have no storage location anymore. So IMPORTANTLY! Don't try to free the reference at &source, as that is
//      now a deprecated location. Only free the reference at dest (which is not the reference that was at source).
// ALSO! MOST IMPORTANTLY! This only works if everything which acquires a reference to source FIRST acquires a reference to dest, universally.
void Reference_MoveExclusive(Reference* dest, Reference source, void** pointerToFree);


// This requires pointer to be exclusively held by the calling thread, and destNewRef to be a location
//  initialized to 0, exclusively held by the calling thread.
// Pointer will be moved into dest, initialized to have a reference count of 0, and the previous value of dest
//  will be moved into destNewRef (atomically, in regards to dest, and as pointer and destNewRef should
//  be exclusive to this thread, this also means it will appear atomic in regards to all of the parameters).
void Reference_MoveNew(Reference* dest, void* pointer, Reference* destNewRef);

// Returns true on success
bool Reference_Move(
    Reference* dest,
    Reference prevDest,
    Reference* source
);


// Moving
//  1) Set the state with the new dest list head index, getting the next move version
//  2) Iterate through the original list, setting all entries to the next move version
//      (getting references as we go)
//  3) Go through the list again, using this as a chance to remove all remove entries from the list, which
//      we couldn't do before, but now we can, because the move version gives us better locking capabilities
//  4) Now simply set the dest list head to be the source list head, and then go update our state accordingly
//todonext
// Oh fuck, yeah... the whole reason we wanted to iterate through the list was because we would be splitting/joining lists. Oops..

//  2) Make the dest memory available, atomically, with the uniqueMoveId, and nextMoveSlotIndex
//  3) Go through all of the current memory, setting move dest
//  4) Go through all current memory, moving it to dest memory,
//  5) Trigger the move state transition


//and upon moving incrementing nextMoveSlotIndex
//      (moves only fail if another thread did the move first, so upon calling move we can safely atomically apply
//          nextMoveSlotIndex = max(startNextMoveSlotIndex + 1, nextMoveSlotIndex))
//      - Every move should mark the dest for deletion, which is safe, because nothing will iterate down to these
//          nodes again, as before it does it will see the previous node was marked as moved, and so not even try...

//  5) Upon seeing nextMoveSlotIndex is beyond the end of the current memory, set the source list to the dest list.


// So... A -> B -> C
//  When we are thinking of removing our reference from B.nextEntry, we check if we have the last reference,
//  and if the value is marked for deletion. If so, we try to move the reference from B.nextEntry to A.nextEntry,
//  making B hidden. Then we can all the references we have, and as B will be hidden (and no one else knew about it),
//  we can safely delete it.
//  (In theory we could move references that have an active count, but... this gets complicated, as it could mess
//      up the simple way we keep track of our references, and even if we found a solution for that, it would
//      anything that has a reference to reiterate to find where we moved the reference to).


// So, inserting is easy. We look at the Head.nextEntry, copy it to be the NewNode.nextEntry,
//  and then try to swap NewNode to Head.nextEntry, only if it was what we saw before.



#pragma pack(push, 1)
typedef struct {
    Reference head;
} AtomicListHead;
#pragma pack(pop)


#pragma pack(push, 1)
typedef struct {
    Reference nextEntry;
    void* value;
} AtomicListEntry;
#pragma pack(pop)


typedef void (*FindMovedLists)(
    void* context,
    void* callbackContext,
    void(*callback)(void* callbackContext, AtomicListHead* list)
);


// (If anything sets moving to true, then the caller should call AtomicList_MoveEntries, and then try again)

void AtomicList_MoveEntries(
    // Created from the global move state atomically, which is nice...
    uint64_t moveCreateIndex,
    AtomicListHead* list,
    void* context,
    // Max size of 2...
    int destinationsCount,
    AtomicListHead** destinations,
    int*(*getDestinationIndex)(void* context, void* value),
    FindMovedLists findMovedLists
);

void AtomicList_InsertValue(
    AtomicListHead* list,
    void* value,
    FindMovedLists findMovedLists
);

void AtomicList_RemoveValue(
    AtomicListHead* list,
    void* context,
    bool(*shouldRemove)(void* context, void* value),
    FindMovedLists findMovedLists
);

// Will iterate over every entry previous added, minus entries previous removed, and possibly entries pending remove and possibly entries
//  that has removed called on them before we exit.
void AtomicList_IterateValues(
    AtomicListHead* list,
    void* context,
    void(*iterate)(void* context, void* value),
    FindMovedLists findMovedLists
);

// So... moving is fun. We mark the head as moved,
//  and then start iterating down, setting values in the destination, and then 