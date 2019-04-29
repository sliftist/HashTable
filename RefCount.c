#include "RefCount.h"
#include "AtomicHelpers.h"


// ATTTENTION! We can't call PACKED_POINTER_GET_POINTER on any threaded values, because there is a race between access of p.

// If the highest bit in the address space is set, then we have to set all the high bits. Otherwise bit fields does the rest for us.
//  (and all other fields can just be get/set via the bitfield)
#if defined(KERNEL) && defined(_MSC_VER)
// Windows kernel space may have kernel addresses
#define PACKED_POINTER_GET_POINTER(p) ((p).isNull ? 0 : (((p).pointerClipped & (1ull << (BITS_IN_ADDRESS_SPACE - 1))) ? ((p).pointerClipped | 0xFFFF000000000000ull) : (p).pointerClipped))
#else
#define PACKED_POINTER_GET_POINTER(p) ((p).isNull ? 0 : (p).pointerClipped)
#endif

static OutsideReference emptyReference = { 0 };

static OutsideReference PREV_NULL = BASE_NULL_LAST_CONST;
OutsideReference GetNextNull() {
    OutsideReference value;
    value.valueForSet = InterlockedIncrement64((LONG64*)&PREV_NULL.valueForSet);
    return value;
}

bool Reference_IsNull(uint64_t value) {
    // The highest bit set, the second highest bit not set
    return (value & (1ull << 63)) && ((~value) & (1ull << 62));
}


// Anyone with InsideReference*, should either have a count incremented in the reference count,
//  or an OutsideReference that points to that InsideReference*.

// When InsideReference reaches a count of 0, it should be freed (as this means nothing knows about it,
//  and so if we don't free it now it will leak).
#pragma pack(push, 1)
struct InsideReference {
    union {
        uint64_t countFullValue;
        struct {
            uint64_t count : 63;
            uint64_t rearranging : 1;
        };
    };

    MemPool* pool;

    // Means, the underlying value should be read from nextRedirectValue
    OutsideReference nextRedirectValue;

    // Used to update nextRedirectValue of all of our nodes, as we can't free any nodes
    //  if a previous node has a nextRedirectValue pointing to them.
    OutsideReference prevRedirectValue;
};
#pragma pack(pop)

CASSERT(sizeof(InsideReference) == 4 * sizeof(uint64_t));
CASSERT(sizeof(InsideReference) == InsideReferenceSize);

#ifdef DEBUG
#define IsInsideRefCorrupt(pRef) IsInsideRefCorruptInner(pRef)
#define IsOutsideRefCorrupt(ref) IsOutsideRefCorruptInner(ref)
#else
#define IsInsideRefCorrupt(pRef) false
#define IsOutsideRefCorrupt(ref) false
#endif

bool IsInsideRefCorruptInner(InsideReference* pRef) {
    if(!pRef) {
        // You should have had a reference to this, preventing it from becoming null...
        OnError(9);
        return true;
    }
    InsideReference ref = *pRef;
	if (!pRef) {
		// nullptr passed as InsideReference...
		OnError(3);
		return true;
	}
    
    if (ref.pool == (void*)0xdddddddddddddddd) {
        // Probably freed memory
        OnError(3);
        return true;
    }
	if (ref.count <= 0) {
		// Something decremented our ref count more times than it held it... this is invalid...
		//  Our outside references should store at least once reference, and if not the caller should have
		//  a reference, or else accessing it is invalid.
		OnError(3);
		return true;
	}
    if(ref.count > (1ull << (64 - BITS_IN_ADDRESS_SPACE - 4))) {
        // Getting close to running out of references...
        OnError(1);
        return true;
    }
	return false;
}
bool IsOutsideRefCorruptInner(OutsideReference ref) {
    if(ref.isNull || ref.valueForSet == 0) {
        return true;
    }
    if(ref.count < 0) {
        // Is the outside reference being used for memory other than an outside reference? If so... that's bad,
        //  because it could overlap with the previous outside ref... which will break everything.
        OnError(3);
        return true;
    }
    if(ref.count > (1ull << (64 - BITS_IN_ADDRESS_SPACE - 4))) {
        // Getting close to running out of references...
        OnError(1);
        return true;
    }
    InsideReference* insideRef = (void*)PACKED_POINTER_GET_POINTER(ref);
    if(insideRef && IsInsideRefCorrupt(insideRef)) {
        return true;
    }
    return false;
}


bool Reference_ReplaceOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef);

bool Reference_HasBeenRedirected(InsideReference* ref) {
    if(IsInsideRefCorrupt(ref)) return false;

    // Include nulls, as they are how we mark something as destructed, but still redirected.
    return ref->nextRedirectValue.valueForSet != 0;
}

void* Reference_GetValue(InsideReference* ref) {
    if(!ref) return nullptr;
    if(IsInsideRefCorruptInner(ref)) return nullptr;
    
    return (void*)((byte*)ref + sizeof(InsideReference));
}

// true on success
bool XchgOutsideReference(
	OutsideReference* structAddress,
	OutsideReference* structOriginal,
	OutsideReference* structNew
) {
    return InterlockedCompareExchange64((LONG64*)structAddress, *(LONG64*)structNew, *(LONG64*)structOriginal) == *(LONG64*)structOriginal;
}

void Reference_RedirectReferenceInner(
    // Must be acquired first
    // If already redirects, fails and returns false.
    InsideReference* oldRef,
    // Must be allocated normally, and have the value set as desired
    InsideReference* newRef,
    // If this is passed, frees oldRef on return.
    OutsideReference* oldRefSource
) {
    if(IsInsideRefCorrupt(oldRef)) return;
    if(IsInsideRefCorrupt(newRef)) return;

  
    // Basically a faster Reference_SetOutside (avoids extra dereference checks, because we know we already have a reference)
    InterlockedIncrement64((LONG64*)&oldRef->countFullValue);
    OutsideReference newRefPrevRedirectValue;
    newRefPrevRedirectValue.valueForSet = InterlockedCompareExchange64(
        (LONG64*)&newRef->prevRedirectValue,
        (LONG64)oldRef,
        (LONG64)emptyReference.valueForSet
    );
    #ifdef DEBUG
    if(!FAST_CHECK_POINTER(newRefPrevRedirectValue, oldRef)) {
        InterlockedIncrement64((LONG64*)&oldRef->countFullValue);
        // Redirect was called with the same of newRef, but different oldRefs... This should never happen.
        OnError(4);
        return;
    }

    #endif
    if(newRefPrevRedirectValue.valueForSet != 0) {
        InterlockedDecrement64((LONG64*)&oldRef->countFullValue);
    }
    
    
    // Another faster Reference_SetOutside?
    InterlockedIncrement64((LONG64*)&newRef->countFullValue);
    OutsideReference oldRefNextRedirectValue;
    oldRefNextRedirectValue.valueForSet = InterlockedCompareExchange64(
        (LONG64*)&oldRef->nextRedirectValue,
        (LONG64)newRef,
        (LONG64)emptyReference.valueForSet
    );
    #ifdef DEBUG
    if(!FAST_CHECK_POINTER(oldRefNextRedirectValue, newRef)) {
        InterlockedDecrement64((LONG64*)&newRef->countFullValue);
        // Redirect was called with the same of oldRef, but different newRefs... This should never happen.
        OnError(4);
        return;
    }
    #endif
    if(oldRefNextRedirectValue.valueForSet != 0) {
        InterlockedDecrement64((LONG64*)&newRef->countFullValue);
    }

    InsideReference* prevPrev = Reference_Acquire(&oldRef->prevRedirectValue);

    // Eh... it's annoying to get rid of recursion here, it would require making it so oldRef->prevRedirectValue
    //  moves it's count inside, so release can release against the emptyReference, letting us release oldRef
    //  and still be able to release prevPrev safely. But... recursion here isn't so bad... so it should be fine...
    Reference_RedirectReferenceInner(prevPrev, newRef, &oldRef->prevRedirectValue);

    if(oldRefSource) {
        Reference_Release(oldRefSource, oldRef);
    }
}

void Reference_RedirectReference(
    // Must be acquired first
    // If already redirects, fails and returns false.
    InsideReference* oldRef,
    // Must be allocated normally, and have the value set as desired
    InsideReference* newRef
) {
    Reference_RedirectReferenceInner(oldRef, newRef, nullptr);
}

InsideReference* Reference_AcquireInside(OutsideReference* pRef) {
    InsideReference* ref = Reference_Acquire(pRef);
    if(!ref) return nullptr;
    InterlockedIncrement64((LONG64*)&ref->countFullValue);
    Reference_Release(pRef, ref);
    return ref;
}



bool releaseInsideReference(InsideReference* insideRef) {
    if(IsInsideRefCorrupt(insideRef)) return false;

    // Consider if we are only alive because of prevRedirectValue references to us.
    while(true) {
        // If we don't have a next value, then we are the most recent, and so there are no prevRedirectValues to find and remove.
        //  Hmm... is it possible for our previous to not be able to destruct because we are referencing to it,
        //  and for some reason the only time it lost it's real reference it couldn't remove our prevRedirectValue
        //  to it? Hmm... maybe...
        if(insideRef->nextRedirectValue.valueForSet == 0) break;
        
        if(insideRef->count != 2) break;
        // 2 references, meaning the one we are about to remove, and 1 from a prevRedirectValue pointing to us.

        // We could re-enter here with no prevRedirectValue pointing to us. But that is fine... we will just do a wasteful search.
        //  And once we get here, we know there are no non-internal references to us, as we always start with a prevRedirectValue,
        //  so 2 here means there are none, and we only wipe out all prevRedirectValue that point to us in here, so...
        //  we can't reach 2 again because of external references...

        // Hold 3 references to ourself, so we can't re-enter, even if someone else
        //  freed the prevRedirectValue pointing to us.
        if(InterlockedCompareExchange64(
            (LONG64*)&insideRef->countFullValue,
            4,
            2
        ) != 2) {
            continue;
        }

        InsideReference* prevRedirect = nullptr;
        if(!insideRef->prevRedirectValue.valueForSet) {
            // Then... we should replace anything pointing to us with just 0...
            // Also... the only way for nothing to have a prevRedirectValue to us, is if at one time we had only 2 references,
            //  1 being the prevRedirectValue, and 1 freed before returning... so we don't have to worry about gaining new
            //  outside references while looking for our prevRedirectValue, because more can't occur.
        } else {
            OutsideReference refForSet;
            refForSet.valueForSet = InterlockedAdd64((LONG64*)&insideRef->prevRedirectValue, 1ll << COUNT_OFFSET_BITS);
            if(refForSet.isNull || !refForSet.pointerClipped) {
                InterlockedAdd64((LONG64*)&insideRef->countFullValue, -2);
                continue;
            }
            InsideReference* prevRedirect = (void*)refForSet.pointerClipped;

            InterlockedAdd64((LONG64*)&prevRedirect->countFullValue, 3);

            if(!FAST_CHECK_POINTER(insideRef->prevRedirectValue, prevRedirect)) {
                // Oh... our previous has likely changed, so we need to use the most recent. If insideRef->prevRedirectValue was the same
                //  then we would know it would stay the same, because we increased the ref count by so much...
                InterlockedAdd64((LONG64*)&prevRedirect->countFullValue, -2);
                releaseInsideReference(prevRedirect);
                continue;
            }

            Reference_Release(&insideRef->prevRedirectValue, prevRedirect);
            // Now we have 3 reference, all inside, to prevRedirect, 2 of which we hold as a lock, and 1 which are going to use to
            //  make an outside reference.
        }

        // Goes to the start of the list, as nextRedirectValue is kept pointing to the most recent node, not the opposite of prevRedirectValue.
        InsideReference* searchNode = Reference_AcquireInside(&insideRef->nextRedirectValue);
        // Oh... while searching we need to turn our references into inside references, because the outside reference
        //  storage locations may be freed...
        while(searchNode) {
            if(FAST_CHECK_POINTER(searchNode->prevRedirectValue, insideRef)) {
                OutsideReference prevRedirectRef;
                // Turn the inside reference into an outside reference
                prevRedirectRef.valueForSet = (uint64_t)prevRedirect;
                if(!Reference_ReplaceOutside(&searchNode->prevRedirectValue, insideRef, prevRedirectRef)) {
                    InterlockedAdd64((LONG64*)&insideRef->countFullValue, -2);
                    InterlockedAdd64((LONG64*)&prevRedirect->countFullValue, -2);
                    releaseInsideReference(searchNode);
                    releaseInsideReference(prevRedirect);
                    continue;
                }
                
                InterlockedAdd64((LONG64*)&insideRef->countFullValue, -2);
                InterlockedAdd64((LONG64*)&prevRedirect->countFullValue, -1);
                releaseInsideReference(searchNode);
                releaseInsideReference(prevRedirect);
                break;
            }

            InsideReference* prevSearchNode = searchNode;
            searchNode = Reference_AcquireInside(&searchNode->prevRedirectValue);
            releaseInsideReference(prevRedirect);
        }
    }


    LONG64 decrementResult = InterlockedDecrement64((LONG64*)&insideRef->countFullValue);

    if(decrementResult == 0) {
        // No more references, and no more outside references so there will never be more references, so we can exclusively free now.
        insideRef->pool->Free(insideRef->pool, insideRef);
        return true;
    }
    return false;
}


InsideReference* Reference_Acquire(OutsideReference* pRef) {
    if(IsOutsideRefCorrupt(*pRef)) return nullptr;
    if(!PACKED_POINTER_GET_POINTER(*pRef)) return nullptr;

    OutsideReference refForSet;
    // Getting a reference to a nullptr can happen, but it is fine, we can just ignore it. It won't happen
    //  enough to overrun the ref count, as only threads that have seen it have a value once will try this,
    //  so it won't continue to build up over time.
    refForSet.valueForSet = InterlockedAdd64((LONG64*)pRef, 1ll << COUNT_OFFSET_BITS);

    InsideReference* ref = (void*)PACKED_POINTER_GET_POINTER(refForSet);
    if(!ref) return nullptr;
    if(IsInsideRefCorrupt(ref)) return nullptr;

    if(ref->nextRedirectValue.valueForSet) {
        //todonext
        // Crap... we need to bring back the nextRedirect following code, otherwise this is just too destructive.
        //  And then we need to replace pRef with a new outside reference to the new inside reference...
        //  because... the caller can't do that, as it can't gain a reference to the old reference, as we stop that here.
        // Oh... but we only need to follow one redirection? And then we can just tail recurse...
        InsideReference* newRef = Reference_Acquire(&ref->nextRedirectValue);
        OutsideReference newOutsideRef = { 0 };
        Reference_SetOutside(&newOutsideRef, newRef);
        if(!Reference_ReplaceOutside(pRef, ref, newOutsideRef)) {
            // Must mean pRef has changed what it is pointing to, so try again with whatever it is pointing to now
            Reference_DestroyOutside(&newOutsideRef, newRef);
            
        }
        Reference_Release(&ref->nextRedirectValue, newRef);
        // And now that we updated pRef, try again, with the new deeper value.
        return Reference_Acquire(pRef);
    }

    return ref;
}


void Reference_Allocate(MemPool* pool, OutsideReference* outRef, void** outPointer, uint64_t size, uint64_t hash) {
    InsideReference* ref = pool->Allocate(pool, size, hash);
    if(!ref) {
        outRef->valueForSet = 0;
        *outPointer = nullptr;
        return;
    }

    outRef->valueForSet = 0;
    outRef->pointerClipped = (uint64_t)ref;

    // Count of 1, for the OutsideReference
    ref->count = 1;
    ref->pool = pool;

    *outPointer = ref;
}


void Reference_Release(OutsideReference* outsideRef, InsideReference* insideRef) {
    if(!insideRef) {
        // This saves our cleanup code constantly null checking insideRef. Instead it can just unconditional
        //  release it, we we can null check it.
        return;
    }

    if(IsOutsideRefCorrupt(*outsideRef)) return;

    // If our reference still exists in OutsideReference this is easy, just decrement the count.
    //  Of course, we may not be able to, even if OutsideReference is valid and correlates to our insideRef...
    //  because of stuff... but that is fine, if our reference isn't in the outside reference for any reason,
    //  it will have been moved to inside ref, and we can decrement it from there.

    while(true) {
        OutsideReference ref = *outsideRef;
        OutsideReference refOriginal = ref;

        // Break when the outside reference is dead

        // If the outer reference doesn't even refer to this inside reference, then our reference must have been moved
        //  to the inner reference
        if((void*)PACKED_POINTER_GET_POINTER(ref) != insideRef) break;

        // If there is nothing to decrement in the outer reference, our reference must have been moved to the inner,
        //  so... go release it there
        if(ref.count == 0) break;
        
        // Unfortunately, we can't do a InterlockedDecrement64, because... the outside reference might
        //  move, which would cause our decrement to wrap around the number, messing up the pointer field.
        ref.count--;
        if(!XchgOutsideReference(outsideRef, &refOriginal, &ref)) continue;

        // If we exchange, then we decremented the outside count, so the reference is freed.
        return;
    }

    // The OutsideReference is dead. Our reference MUST be in the inside reference. References (counts) only
    //  flow from the outside to the inside.
    //  (while new references may be added to the outside reference, and it may be reused with the same pointer,
    //      meaning it is possible to retry to use the outside reference, we should always be able to dereference
    //      from the inside reference, as our reference had to go somewhere if it wasn't in the outside ref...)
    releaseInsideReference(insideRef);
}

bool Reference_ReplaceOutsideInner(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef, bool freeOriginalOutsideRef) {
    if(IsOutsideRefCorrupt(*pOutsideRef)) return false;
    if(IsInsideRefCorrupt(pInsideRef)) return false;

    // First move the references from outside ref to inside ref (causing them to be duplicated for a bit)
    //  (and which if removing from the outside ref fails may require removing from the inside ref)
    // Then try to make the outside ref wiped out

	// As we make changes in two parts, we have to keep track of the changes we made to the inner count, so our retry loop can
	//	combine the new changes with undoing the previous changes, which makes this more efficient (for retries).
	int64_t insideRefDelta = 0;

    #ifdef DEBUG
    int loops = 0;
    #endif
    while(true) {
        #ifdef DEBUG
        loops++;
        #endif
		OutsideReference outsideRef = *pOutsideRef;
        
        if((void*)PACKED_POINTER_GET_POINTER(outsideRef) != pInsideRef) {
			if (insideRefDelta != 0) {
                InterlockedAdd64((LONG64*)&pInsideRef->countFullValue, (LONG64)insideRefDelta);
			}
			return false;
        }

		int64_t amountAdded = outsideRef.count + insideRefDelta;
        InterlockedAdd64((LONG64*)&pInsideRef->countFullValue, (LONG64)amountAdded);
        if(pInsideRef->count > (1ull << (64 - BITS_IN_ADDRESS_SPACE - 4))) {
            // Getting close to running out of references...
            OnError(1);
        }


        // Now try to remove from outside ref. If we fail... we have to go fix insideRef before we retry again
		if (!XchgOutsideReference(pOutsideRef, &outsideRef, &newOutsideRef)) {
			// Subtract outsideRefOriginal.count from insideRef, but don't worry about it destructing the inside ref, the caller has a reference to it anyway
			insideRefDelta -= amountAdded;
			continue;
		}

        // Now that the outside ref is atomically gone, we can remove the outside's own ref to the inside ref
        //  (this is NOT the reference the caller has, this is the outside reference's own ref, completely different)
        if(freeOriginalOutsideRef && releaseInsideReference(pInsideRef)) {
            // If our reference's own reference was the last reference, that means the caller lied about
            //  having a reference, and bad stuff is going to happen...
            OnError(3);
        }
        #ifdef DEBUG
        //printf("moved %lld to inside from %p to %p, %s:%d\n", amountAdded, pOutsideRef, pInsideRef, file, line);
        if(loops == 1 && amountAdded == 0) {
            // (loops == 1 because retries will mess up amountAdded. We can check if loops > 1, it just takes more code,
            //      and this case should cover it anyway...)
            // This is actually invalid. You have to gain a reference via the outside reference to release it.
            //  We checked to make sure your reference was for the outside reference, but if it has no references,
            //  you must have obtained your reference via a different outside reference, which is bad.
            OnError(3);
        }
        #endif
        return true;
    }
    return false;
}
bool Reference_ReplaceOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef) {
    return Reference_ReplaceOutsideInner(pOutsideRef, pInsideRef, newOutsideRef, true);
}
bool Reference_ReplaceOutsideStealOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef) {
    return Reference_ReplaceOutsideInner(pOutsideRef, pInsideRef, newOutsideRef, false);
}

bool Reference_DestroyOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef) {
    return Reference_ReplaceOutside(pOutsideRef, pInsideRef, emptyReference);
}
bool Reference_DestroyOutsideMakeNull(OutsideReference* pOutsideRef, InsideReference* pInsideRef) {
    return Reference_ReplaceOutside(pOutsideRef, pInsideRef, BASE_NULL);
}

bool Reference_SetOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef) {
    if(IsOutsideRefCorrupt(*pOutsideRef)) return false;
    if(IsInsideRefCorrupt(pInsideRef)) return false;

    // Refuse to create more references for redirects values
    if(pInsideRef->nextRedirectValue.valueForSet) return false;

    while(true) {
        OutsideReference outsideRef = *pOutsideRef;

        if((void*)PACKED_POINTER_GET_POINTER(outsideRef) != 0) return false;
        // The ref count on a nullptr can be positive. We will never release this ref, so it doesn't matter, just wipe it out
        //if(outsideRef.count != 0) return false;

        InterlockedIncrement64((LONG64*)&pInsideRef->countFullValue);

        OutsideReference outsideRefOriginal = outsideRef;
        outsideRef.count = 0;
        outsideRef.pointerClipped = (uint64_t)pInsideRef;
        // So, going from {0, 0} to {0, pInsideRef}
        if(!XchgOutsideReference(pOutsideRef, &outsideRefOriginal, &outsideRef)) {
            // Failure means it is already being used. So, undo our inside ref increment, and abort
            if(releaseInsideReference(pInsideRef)) {
                // If our reference's own reference was the last reference, that means the caller lied about
                //  having a reference, and bad stuff is going to happen...
                OnError(3);
            }
            return false;
        }

        //printf("set outside %p from inside %p\n", pOutsideRef, pInsideRef);
        return true;
    }
    return false;
}