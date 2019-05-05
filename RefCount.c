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

bool Reference_ReplaceOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef);
bool Reference_ReplaceOutsideInner(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef, bool freeOriginalOutsideRef, bool allowNotHavingSpecificOutsideRef);

InsideReference* Reference_AcquireInternal(OutsideReference* pRef, bool redirect);

OutsideReference emptyReference = { 0 };

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
            uint64_t count: 63;
            uint64_t destructStarted: 1;
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
#define IsInsideRefCorrupt(pRef) IsInsideRefCorruptInner(pRef, false)
#define IsOutsideRefCorrupt(ref) IsOutsideRefCorruptInner(ref)
#else
#define IsInsideRefCorrupt(pRef) false
#define IsOutsideRefCorrupt(ref) false
#endif

bool IsInsideRefCorruptInner(InsideReference* pRef, bool allowFreed) {
    if(!pRef) {
        // You should have had a reference to this, preventing it from becoming null...
        OnError(9);
        return true;
    }
    InsideReference ref = *pRef;
    if(ref.nextRedirectValue.valueForSet != 0 && ref.nextRedirectValue.valueForSet == ref.prevRedirectValue.valueForSet) {
        // What!? The redirection is cyclic! This means that an older reference somehow got set at the new reference. This
        //  shouldn't be happening...
        OnError(3);
        return true;
    }
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
	if (!allowFreed && ref.count <= 0) {
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
        return false;
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
    if(insideRef) {
        if(IsInsideRefCorrupt(insideRef)) return true;
    }
    return false;
}



bool Reference_HasBeenRedirected(InsideReference* ref) {
    if(IsInsideRefCorruptInner(ref, true)) return false;

    // Include nulls, as they are how we mark something as destructed, but still redirected.
    return ref->nextRedirectValue.valueForSet != 0;
}

void* Reference_GetValue(InsideReference* ref) {
    if(!ref) return nullptr;
    if(IsInsideRefCorrupt(ref)) return nullptr;
    
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

InsideReference* Reference_AcquireInsideNoRedirect(OutsideReference* pRef) {
	InsideReference* ref = Reference_AcquireInternal(pRef, false);
	if (!ref) return nullptr;
	InterlockedIncrement64((LONG64*)&ref->countFullValue);
	Reference_Release(pRef, ref);
	return ref;
}

bool releaseInsideReference(InsideReference* insideRef);
void Reference_RedirectReference(
    // Must be acquired first
    // If already redirects, fails and returns false.
    InsideReference* oldRef,
    // Must be allocated normally, and have the value set as desired
    InsideReference* newRef
) {
    if(IsInsideRefCorrupt(oldRef)) return;
    if(IsInsideRefCorrupt(newRef)) return;

	#ifdef DEBUG
	if (newRef->prevRedirectValue.valueForSet != 0 && !FAST_CHECK_POINTER(newRef->prevRedirectValue, oldRef)) {
		// Redirect was called with the same of newRef, but different oldRefs... This should never happen.
		OnError(4);
		return;
	}
	#endif

    // Basically a faster Reference_SetOutside (avoids extra dereference checks, because we know we already have a reference)
    InterlockedIncrement64((LONG64*)&oldRef->countFullValue);
    OutsideReference newRefPrevRedirectValue;
    newRefPrevRedirectValue.valueForSet = InterlockedCompareExchange64(
        (LONG64*)&newRef->prevRedirectValue,
        (LONG64)oldRef,
        (LONG64)emptyReference.valueForSet
    );
    #ifdef DEBUG
    if(newRefPrevRedirectValue.valueForSet != 0 && !FAST_CHECK_POINTER(newRefPrevRedirectValue, oldRef)) {
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

    if(oldRefNextRedirectValue.valueForSet != 0) {
		// The newRef->prevRedirectValue will be freed when our newRef is freed.
		// But we do need to free the ref we added for the failed outside ref set
        InterlockedDecrement64((LONG64*)&newRef->countFullValue);

		if (IsInsideRefCorrupt(oldRef)) return;
		if (IsInsideRefCorrupt(newRef)) return;

		return;
    }

	// Update all previous values to point to the new head
    InsideReference* prevToRedirect = Reference_AcquireInsideNoRedirect(&oldRef->prevRedirectValue);
	while (prevToRedirect) {
		#ifdef DEBUG
		if (IsSingleThreadedTest) {
			// Redirecting old values is only required when there are hanging references on other threads. If we need this with a single thread
			//	it means we are leaking references, which is bad...
			OnError(13);
		}
		#endif

		// Only redirect from oldRef to newRef
		InterlockedIncrement64((LONG64*)&newRef->countFullValue);
		bool redirected = false;
		while (true) {
			OutsideReference nextRedirectValue = prevToRedirect->nextRedirectValue;
			if (!FAST_CHECK_POINTER(nextRedirectValue, oldRef)) {
				break;
			}
			if (InterlockedCompareExchange64(
				(LONG64*)&prevToRedirect->nextRedirectValue,
				(LONG64)newRef,
				(LONG64)nextRedirectValue.valueForSet
			) == nextRedirectValue.valueForSet) {
				redirected = true;
				break;
			}
		}
		
		if (!redirected) {
			releaseInsideReference(prevToRedirect);
			InterlockedDecrement64((LONG64*)&newRef->countFullValue);
			// If we see a new value then another thread must have already applied our redirection, to this node and
			//	future nodes, so we might as well break now.
			break;
		}

		InsideReference* prevToRedirectPrev = prevToRedirect;
		prevToRedirect = Reference_AcquireInsideNoRedirect(&prevToRedirectPrev->prevRedirectValue);
		releaseInsideReference(prevToRedirectPrev);
	}
}

bool setDestructValue(InsideReference* ref) {
    while(true) {
        uint64_t count = ref->countFullValue;
        if(count & (1ull << 63)) {
            return false;
        }
        if(InterlockedCompareExchange64(
            (LONG64*)&ref->countFullValue,
            count | (1ull << 63),
            count
        ) == count) {
            return true;
        }
    }
    return false;
}

void DestroyUniqueOutsideRef(OutsideReference* ref) {
	void* temp = Reference_Acquire(ref);
	if (!temp) return;
	Reference_DestroyOutside(ref, temp);
	Reference_Release(&emptyReference, temp);
	ref->valueForSet = 0;
}
bool releaseInsideReference(InsideReference* insideRef) {
    if(IsInsideRefCorrupt(insideRef)) return false;

    //todonext
    // So... it looks like our redirect handling code is destroying references incorrectly... which is causing references
    //  to lose all references at points that should be impossible.

    // Consider if we are only alive because of prevRedirectValue references to us.
    while(true) {
        // If we don't have a next value, then we are the most recent, and so there are no prevRedirectValues to find and remove.
        //  Hmm... is it possible for our previous to not be able to destruct because we are referencing to it,
        //  and for some reason the only time it lost it's real reference it couldn't remove our prevRedirectValue
        //  to it? Hmm... maybe...
        if(insideRef->nextRedirectValue.valueForSet == 0) break;
        
        if(insideRef->count != 2) break;
        // 2 references, meaning the one we are about to remove, and 1 from a prevRedirectValue pointing to us.

        if(!setDestructValue(insideRef)) {
            break;
        }
        
        // We find our next node and swap it from pointing to us, to pointing to our previous node.
        //  If the next removed itself we have nothing to point to, so that can't happen.
        //  And, what if the previous node removed itself, then we need to change the new prev node.

        InsideReference* prevNode = nullptr;
        {
            prevNode = Reference_AcquireInsideNoRedirect(&insideRef->prevRedirectValue);
            if(prevNode) {
                if(!setDestructValue(prevNode)) {
                    prevNode->destructStarted = 0;
                    break;
                }
            }
        }

        InsideReference* nextNode = nullptr;
        {
            // Goes to the start of the list, as nextRedirectValue is kept pointing to the most recent node, not the opposite of prevRedirectValue.
            InsideReference* searchNode = Reference_AcquireInsideNoRedirect(&insideRef->nextRedirectValue);
            while(searchNode) {
                if(FAST_CHECK_POINTER(searchNode->prevRedirectValue, insideRef)) {
                    nextNode = searchNode;
                    break;
                }
                
                InsideReference* prevSearchNode = searchNode;
                searchNode = Reference_AcquireInsideNoRedirect(&searchNode->prevRedirectValue);
                releaseInsideReference(prevSearchNode);
            }
        }

        bool stateChanged = false;
        bool retry = false;
        if(!nextNode) {
            stateChanged = true;
            retry = true;
        } else {
            if(!FAST_CHECK_POINTER(nextNode->prevRedirectValue, insideRef)) {
                stateChanged = true;
                retry = true;
			}
			else {
				if (!setDestructValue(nextNode)) {
					stateChanged = true;
				}
			}
        }
        
        if(!FAST_CHECK_POINTER(insideRef->prevRedirectValue, prevNode)) {
            stateChanged = true;
            retry = true;
        }

        if(!stateChanged) {
            OutsideReference newPrevRef = { 0 };
            if(prevNode) {
                Reference_SetOutside(&newPrevRef, prevNode);
            }
            if(!Reference_ReplaceOutside(
                &nextNode->prevRedirectValue,
                insideRef,
                newPrevRef
            )) {
                retry = true;
                if(prevNode) {
                    Reference_DestroyOutside(&newPrevRef, prevNode);
                }
            }
        }
        
        if(nextNode) {
            nextNode->destructStarted = 0;
            releaseInsideReference(nextNode);
        }
        if(prevNode) {
            prevNode->destructStarted = 0;
            releaseInsideReference(prevNode);
        }
        insideRef->destructStarted = 0;
        if(retry) {
            continue;
        }
        break;
    }

	LONG64 decrementResult = InterlockedDecrement64((LONG64*)&insideRef->countFullValue);
	if (decrementResult == 0) {
		DestroyUniqueOutsideRef(&insideRef->nextRedirectValue);
		// No more references, and no more outside references so there will never be more references, so we can exclusively free now.
		insideRef->pool->Free(insideRef->pool, insideRef);
		return true;
	}
	return false;
}

InsideReference* Reference_AcquireInternal(OutsideReference* pRef, bool redirect) {
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

    if(redirect && ref->nextRedirectValue.valueForSet) {
        InsideReference* newRef = Reference_Acquire(&ref->nextRedirectValue);
        OutsideReference newOutsideRef = { 0 };
        Reference_SetOutside(&newOutsideRef, newRef);
        if(!Reference_ReplaceOutside(pRef, ref, newOutsideRef)) {
			Reference_Release(&ref->nextRedirectValue, newRef);
            // Must mean pRef has changed what it is pointing to, so try again with whatever it is pointing to now
            Reference_DestroyOutside(&newOutsideRef, newRef);
			// So pRef is valid or moved
			Reference_Release(pRef, ref);
		}
		else {
			Reference_Release(&ref->nextRedirectValue, newRef);
			// We know pRef is moved, so we have to force an inside release.
			Reference_Release(&emptyReference, ref);
		}
		// Reset IsSingleThreadedTest, because IsSingleThreadedTest will actually have the nextRedirectValue changed
		#ifdef DEBUG
		bool originalIsSingleThreadedTest = IsSingleThreadedTest;
		IsSingleThreadedTest = false;
		#endif

		#ifdef DEBUG
		IsSingleThreadedTest = originalIsSingleThreadedTest;
		#endif
        // And now that we updated pRef, try again, with the new deeper value.
        return Reference_Acquire(pRef);
    }

    return ref;
}

InsideReference* Reference_Acquire(OutsideReference* pRef) {
    if(IsOutsideRefCorrupt(*pRef)) return nullptr;

    OutsideReference refForSet;
    
	//InterlockedAdd64 is just InterlockedExchangeAdd64 anyway, so it is faster to use InterlockedExchangeAdd64
	//	and then do the add only if we have to (which shouldn't be on the hot path anyway).
    refForSet.valueForSet = InterlockedExchangeAdd64((LONG64*)pRef, 1ll << COUNT_OFFSET_BITS);

    InsideReference* ref = (void*)PACKED_POINTER_GET_POINTER(refForSet);
    if(ref) {
        if(!ref->nextRedirectValue.valueForSet) {
            return ref;
        }
        Reference_Release(pRef, ref);
        return Reference_AcquireInternal(pRef, true);
    }

    InterlockedCompareExchange64(
        (LONG64*)pRef,
        0,
        refForSet.valueForSet + (1ll << COUNT_OFFSET_BITS)
    );

    return nullptr;
}


void Reference_Allocate(MemPool* pool, OutsideReference* outRef, void** outPointer, uint64_t size, uint64_t hash) {
    InsideReference* ref = pool->Allocate(pool, size + InsideReferenceSize, hash);
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

    *outPointer = (void*)((byte*)ref + InsideReferenceSize);
}

#ifdef DEBUG
bool IsSingleThreadedTest = false;
#endif

void Reference_Release(OutsideReference* outsideRef, InsideReference* insideRef) {
    if(!insideRef) {
        // This saves our cleanup code constantly null checking insideRef. Instead it can just unconditional
        //  release it, we we can null check it.
        return;
    }

    if(IsOutsideRefCorrupt(*outsideRef)) return;

    if(outsideRef != &emptyReference) {
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
    }

    // The OutsideReference is dead. Our reference MUST be in the inside reference. References (counts) only
    //  flow from the outside to the inside.
    //  (while new references may be added to the outside reference, and it may be reused with the same pointer,
    //      meaning it is possible to retry to use the outside reference, we should always be able to dereference
    //      from the inside reference, as our reference had to go somewhere if it wasn't in the outside ref...)
    releaseInsideReference(insideRef);
}

void Reference_ReleaseFast(OutsideReference* outsideRef, InsideReference* insideRef) {
    if(IsOutsideRefCorrupt(*outsideRef)) return;

    // If we are usually releasing 1 outside ref (and outsideRef matches the insideRef), this should usually
    //  succeed, saving a lot of time. However, if it doesn't... then we just have to do a real call
    OutsideReference refOriginal;
    refOriginal.valueForSet = (uint64_t)insideRef;
    refOriginal.fastCount = 1;
    OutsideReference refNew;
    refNew.valueForSet = (uint64_t)insideRef;
    refNew.fastCount = 0;
    if(XchgOutsideReference(outsideRef, &refOriginal, &refNew)) {
        return;
    }

    Reference_Release(outsideRef, insideRef);
}

bool Reference_ReplaceOutsideInner(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef, bool freeOriginalOutsideRef, bool allowNotHavingSpecificOutsideRef) {
    if(IsOutsideRefCorrupt(*pOutsideRef)) return false;
    if(IsInsideRefCorrupt(pInsideRef)) return false;

    // First move the references from outside ref to inside ref (causing them to be duplicated for a bit)
    //  (and which if removing from the outside ref fails may require removing from the inside ref)
    // Then try to make the outside ref wiped out

    while(true) {
		OutsideReference outsideRef = *pOutsideRef;
        
        if((void*)PACKED_POINTER_GET_POINTER(outsideRef) != pInsideRef) {
			return false;
        }

        InterlockedAdd64((LONG64*)&pInsideRef->countFullValue, (LONG64)outsideRef.count);
        if(pInsideRef->count > (1ull << (64 - BITS_IN_ADDRESS_SPACE - 4))) {
            // Getting close to running out of references...
            OnError(1);
        }


        // Now try to remove from outside ref. If we fail... we have to go fix insideRef before we retry again
		if (!XchgOutsideReference(pOutsideRef, &outsideRef, &newOutsideRef)) {
			// Subtract outsideRefOriginal.count from insideRef, but don't worry about it destructing the inside ref, the caller has a reference to it anyway
            InterlockedAdd64((LONG64*)&pInsideRef->countFullValue, -(LONG64)outsideRef.count);
			continue;
		}

        // Now that the outside ref is atomically gone, we can remove the outside's own ref to the inside ref
        //  (this is NOT the reference the caller has, this is the outside reference's own ref, completely different)
        if(freeOriginalOutsideRef && releaseInsideReference(pInsideRef)) {
            // If our reference's own reference was the last reference, that means the caller lied about
            //  having a reference, and bad stuff is going to happen...
            OnError(3);
        }
        return true;
    }
    return false;
}
bool Reference_ReplaceOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef) {
    return Reference_ReplaceOutsideInner(pOutsideRef, pInsideRef, newOutsideRef, true, false);
}
bool Reference_ReplaceOutsideStealOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef) {
    return Reference_ReplaceOutsideInner(pOutsideRef, pInsideRef, newOutsideRef, false, false);
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

bool Reference_ReduceToZeroOutsideRefs(OutsideReference* ref) {
    InsideReference* moveValueRef = Reference_Acquire(ref);
	if (!moveValueRef) return true;

    OutsideReference moveValueOutsideRefWithZeroRefs = { 0 };
    moveValueOutsideRefWithZeroRefs.pointerClipped = (uint64_t)moveValueRef;
    bool reducedToZeroRefs = Reference_ReplaceOutsideInner(
        ref,
        moveValueRef,
        moveValueOutsideRefWithZeroRefs,
        false,
        false
    );
    Reference_Release(&emptyReference, moveValueRef);
    return reducedToZeroRefs;
}