#include "AtomicLinkedList.h"

#include "AtomicHelpers.h"

void* Reference_Acquire(Reference* pRef) {
    while(true) {
        Reference ref = *pRef;
        Reference refOriginal = ref;
        if(!ref.pointer) {
            return nullptr;
        }
        ref.referenceCount++;
        if(ref.referenceCount == 0) {
            // referenceCount wrapped around
            OnError(8);
            return nullptr;
        }
        
        if(!InterlockedCompareExchangeStruct128(pRef, &refOriginal, &ref)) {
            continue;
        }

        return ref.pointer;
    }
}


void reference_ReleaseInner(Reference* pRef, void* pointer, void** pointerToFree, bool setFree) {
    while(true) {
        Reference ref = *pRef;
        Reference refOriginal = ref;
        if(!ref.pointer) {
            // pointer was released while we supposed held a reference, should be impossible
            OnError(9);
            return;
        }
        if(ref.referenceCount == 0) {
            // reference count was released while we supposed held a reference, should be impossible
            OnError(10);
            return;
        }

        if(setFree) {
            ref.markedForDestruction = true;
        }

        ref.referenceCount--;
        void* pointerToFreeTemp = nullptr;
        if(ref.markedForDestruction && ref.referenceCount == 0) {
            ref.markedForDestruction = false;
            pointerToFreeTemp = ref.pointer;
            ref.pointer = nullptr;
        }
        
        if(!InterlockedCompareExchangeStruct128(pRef, &refOriginal, &ref)) {
            continue;
        }

        *pointerToFree = pointerToFreeTemp;

        return;
    }
}

void Reference_Release(Reference* pRef, void* pointer, void** pointerToFree) {
    reference_ReleaseInner(pRef, pointer, pointerToFree, false);
}

void Reference_ReleaseAndFree(Reference* pRef, void* pointer, void** pointerToFree) {
    reference_ReleaseInner(pRef, pointer, pointerToFree, true);
}

void Reference_MoveExclusive(Reference* pDest, Reference source, void** pointerToFree) {
    while(true) {
        Reference dest = *pDest;

        if(source.referenceCount == 0) {
            // Invalid argument, we should have a reference to source
            OnError(3);
            return;
        }

        if(dest.referenceCount == 0) {
            // Invalid argument, we should have a reference to dest
            OnError(3);
            return;
        }

        // So, interesting enough source.referenceCount > dest.referenceCount is valid.
        //  This occurs when a new entry is added, so anything currently iterating only has references
        //      to previous list items, not the new head.
        // However, this doesn't break any of our code, as we are only concerned about future references,
        //  as we verify we are the only holder of both references, so all FUTURE references will
        //  require adding a reference to dest before a reference is added to source.
        // So this works...

        if(source.referenceCount != 1) {
            return;
        }
        if(dest.referenceCount != 1) {
            return;
        }
        
        // dest.markedForDestruction is fine... we are going to be destroying it anyway

        if(!InterlockedCompareExchangeStruct128(pDest, &dest, &source)) {
            continue;
        }

        *pointerToFree = dest.pointer;
        return;
    }
}

// This requires pointer to be exclusively held by the calling thread, and destNewRef to be a location
//  initialized to 0, exclusively held by the calling thread.
// Pointer will be moved into dest, initialized to have a reference count of 1, and the previous value of dest
//  will be moved into destNewRef (atomically, in regards to dest, and as pointer and destNewRef should
//  be exclusive to this thread, this also means it will appear atomic in regards to all of the parameters).
void Reference_MoveNew(Reference* pDest, void* pointer, Reference* destNewRef) {
    while(true) {
        Reference dest = *pDest;
        if(dest.pointer == pointer) {
            // By definition this is wrong... as pointer must be new, and pDest is probably not, so... this is bad...
            OnError(3);
            return;
        }

        *destNewRef = dest;

        Reference newRef = { 0 };
        newRef.pointer = pointer;
        // No referenceCount, I don't think the caller needs to use the reference after this...

        if(!InterlockedCompareExchangeStruct128(pDest, &dest, &newRef)) {
            continue;
        }
        return;
    }
}


bool Reference_Move(
    Reference* pDest,
    Reference prevDest,
    Reference* pSource
) {
    return InterlockedCompareExchangeStruct128(pDest, &prevDest, pSource);
}





AtomicListEntry atomicList_Acquire(AtomicListHead* list);
// Eh... this is... not great. But, I can't think of another way to do this... except maybe via some sort of tree?
//  Oh well, it should be fine, these lists are just used to power hash tables, so the lists should be barely populated,
//  and moves should only double the number of elements to search, and only when code is running during a move...
void atomicList_Release(
    AtomicListEntry headAcquired,
    // Context presumably has the original slot and hash information, which let us which mask was used
    //  to create the original list, which is sufficient information to find where all parts of the list
    //  may have ended up.
    void* context,
    FindMovedLists findMovedLists
);


void atomicList_InsertValue(AtomicListHead* list, AtomicListEntry* entry) {

}



void AtomicList_MoveEntries(
    uint64_t moveCreateIndex,
    AtomicListHead* list,
    void* context,
    int destinationsCount,
    AtomicListHead** destinations,
    int*(*getDestinationIndex)(void* context, void* value),
    FindMovedLists findMovedLists
) {
    Reference prevDestHeads[2] = { 0 };
    for(int i = 0; i < destinationsCount; i++) {
        prevDestHeads[i] = destinations[i]->head;
        if(prevDestHeads[i].createIndex > moveCreateIndex) {
            // Inserts after our move means one thread has finished our move
            return;
        }
    }

    // TODO: Lock and free the list. Probably via an inner function, to make things easier...
    //AtomicListEntry startSourceHead = atomicList_Acquire(list);


    while(true) {
        Reference sourceHead = list->head;
        
        AtomicListEntry* pHeadEntry = sourceHead.pointer;
        if(!pHeadEntry) break;
        AtomicListEntry head = *pHeadEntry;

        if(!sourceHead.markedForDestruction && head.value) {
            if(head.nextEntry.createIndex >= moveCreateIndex) {
                // Then we have already moved this value
            } else {
                int destIndex = getDestinationIndex(context, head.value);

                Reference destHead = destinations[destIndex]->head;

                //todonext
                // Wait... no matter how I do this... it will be destructive.
                // After source.next = dest no one else can iterate to the next value... And if I do this after,
                //  then the previous head will be lost...

                // Hmm... could I move source.next into a place in the head? That has it's own transaction id? That is then used
                //  in the case... ah fuck... I think I need transaction queues here...
                //  We could probably do something nice where each thread has it's transaction, and swaps it to the
                //      global transaction, or if there is one works on the global transaction.
                // And then... when splitting, we use the source tranasction queue, and when joining we use the dest queue?
                // So... 

                // source.next = dest
                if(Reference_Move(
                    &pHeadEntry->nextEntry,
                    head.nextEntry,
                    &destHead
                )) {
                    // dest = source
                    if(!Reference_Move(
                        &destHead,
                        prevDestHeads[destIndex],
                        &sourceHead
                    )) {
                        // The destination may be done, or... it might just be that another thread did this single move,
                        //  but there are still more moves to do.
                        if(destinations[destIndex]->head.createIndex > moveCreateIndex) {
                            return;
                        }
                    }
                } else {
                    if(destinations[destIndex]->head.createIndex > moveCreateIndex) {
                        return;
                    }
                }
            }
        }

        //head.nextEntry

        /*
            head.nextEntry = prevDestHeads[destIndex];
            
            head.nextEntry


            // Another check here, to save us time from excessive iteation
            
            
            // InterlockedCompareExchangeStruct128(pDest, &prevDest, pSource);

            if(destHead.createIndex > moveCreateIndex) {
                // Something has been inserted since we started moving. So... our move must have finished a long time ago
                break;
            }

            Reference sourceHead2 = list->head;
            // If they aren't equal, then our value must have been long inserted by now
            if(!EqualsStruct128(&sourceHead, &sourceHead2)) continue;



            // Upon failure it means someone else moved it first. So... we still have to iterate, as they may have
            //  just moved one value and then died...
            Reference_Move(&dest->head, prevHeadRef, &headRef);
        } else {

        }
        prevHeadRef = headRef;

        */
    }

    /*

    Reference* pEntryRef = &list->head;

    while(pEntryRef) {
        bool pointerMoved = false;
        AtomicListEntry* entry = Reference_Acquire(moveVersion, pEntryRef, &pointerMoved);
        if(pointerMoved) break;
        if(!entry) break;

        Reference_SetMoving(moveVersion, pEntryRef)

        entry->value
        Reference_SetMoving()
    }


    Reference_SetMoving()

    entry->value
    */
}

// Hmm... we could have a "createIndex", because... even if we scatter and combine lists, with the hash we can determine if they were part
//  of the list we were part of before, and if so if they were created before or after we started.
//  - We could need to take the max nextCreateIndex when joining