#include "RefCount.h"
#include "AtomicHelpers.h"


// If the highest bit in the address space is set, then we have to set all the high bits. Otherwise bit fields does the rest for us.
//  (and all other fields can just be get/set via the bitfield)
#if defined(KERNEL) && defined(_MSC_VER)
// Windows kernel space may have kernel addresses
#define PACKED_POINTER_GET_POINTER(p) (((p).pointerClipped & (1ull << (BITS_IN_ADDRESS_SPACE - 1))) ? ((p).pointerClipped | 0xFFFF000000000000ull) : (p).pointerClipped)
#else
#define PACKED_POINTER_GET_POINTER(p) (p).pointerClipped
#endif

static OutsideReference NEXT_NULL = { 0, 0, 1 };
OutsideReference GetNextNULL() {
    OutsideReference value;
    value.valueForSet = InterlockedIncrement64((LONG64*)&NEXT_NULL.valueForSet);
    return value;
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
    // NOTE: Of course, this means values may be in multiple locations at once. So either values have to be immutable,
    //  or you have to have detection for values being moved, OR you just have to make it so previous values being read is fine.
    //  (although we do make guarantees about making it so any new references obtained receive the freshest value at that time,
    //  which makes this mostly moot, as long as writing and moving are synchronized via some external atomic technique).
    OutsideReference nextRedirectValue;

    // Means something else is pointing to us for its value. This is important as it allows us to prevent
    //  hanging references from forming infinitely long chains, and instead allows us to shrink the chains
    //  to only contain the current, and hanging reference.
    //  - And long chains is bad for everyone, as any reference that has moved likely has an implicit referenec
    //      to its MemPool... and so if we don't shrink long chains we could easily start leaking all allocations,
    //      just because one thread crashed once.
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
    if(ref.count > (1ull << (64 - BITS_IN_ADDRESS_SPACE - 3))) {
        // Getting close to running out of references...
        OnError(1);
        return true;
    }
	return false;
}
bool IsOutsideRefCorruptInner(OutsideReference ref) {
    if(ref.count < 0) {
        // Is the outside reference being used for memory other than an outside reference? If so... that's bad,
        //  because it could overlap with the previous outside ref... which will break everything.
        OnError(3);
        return true;
    }
    if(ref.count > (1ull << (64 - BITS_IN_ADDRESS_SPACE - 3))) {
        // Getting close to running out of references...
        OnError(1);
        return true;
    }
	if (ref.pointerClipped) {
		InsideReference* insideRef = (void*)PACKED_POINTER_GET_POINTER(ref);
        if(IsInsideRefCorrupt(insideRef)) {
            return true;
        }
	}
    return false;
}


bool Reference_ReplaceOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef, InsideReference* newInsideRef);

bool Reference_HasBeenRedirected(InsideReference* ref) {
    if(IsInsideRefCorrupt(ref)) return false;
    return !!ref->nextRedirectValue.pointerClipped;
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

static OutsideReference emptyRef = { 0 };
bool Reference_RedirectReference(
    // Must be acquired first
    // If already redirects, fails and returns false.
    InsideReference* oldRef,
    // Must be allocated normally, and have the value set as desired
    InsideReference* newRef
) {
    if(IsInsideRefCorrupt(oldRef)) return false;
    if(IsInsideRefCorrupt(newRef)) return false;

   
    // faster Reference_SetOutside
    InterlockedIncrement64((LONG64*)&oldRef->countFullValue);
    newRef->prevRedirectValue.valueForSet = oldRef;


    newRef->count++;
    OutsideReference newOutsideRef;
    newOutsideRef.valueForSet = newRef;
    bool didRedirect = XchgOutsideReference(&oldRef->nextRedirectValue, &emptyRef, &newOutsideRef);
    if(didRedirect) {
        return true;
    }

    newRef->count--;

    Reference_DestroyOutside(&newRef->prevRedirectValue, oldRef);

    return false;
}

bool releaseInsideReference(InsideReference* insideRef) {
    if(IsInsideRefCorrupt(insideRef)) return false;

    // Remove nodes that have no more external references
    while(true) {
        if(!insideRef->rearranging) {
            if(insideRef->nextRedirectValue.pointerClipped) {
                if(insideRef->prevRedirectValue.pointerClipped) {
                    if(insideRef->countFullValue == 3) {
                        // Set rearranging to 1
                        if(InterlockedCompareExchange64(&insideRef->countFullValue, (1ll << 63) | 3, 3) != 3) {
                            continue;
                        }

                        OutsideReference prevOutsideRef;
                        prevOutsideRef.valueForSet = InterlockedAdd64((LONG64*)&insideRef->prevRedirectValue, 1ll << BITS_IN_ADDRESS_SPACE);
                        InsideReference* prev = prevOutsideRef.pointerClipped;
                        if(!prev) {
                            insideRef->rearranging = 0;
                            continue;
                        }

                        OutsideReference nextOutsideRef;
                        nextOutsideRef.valueForSet = InterlockedAdd64((LONG64*)&insideRef->nextRedirectValue, 1ll << BITS_IN_ADDRESS_SPACE);
                        InsideReference* next = nextOutsideRef.pointerClipped;
                        if(!next) {
                            insideRef->rearranging = 0;
                            Reference_Release(&insideRef->prevRedirectValue, prev);
                            continue;
                        }

                        // Set prev->rearranging to 1
                        uint64_t prevCountFullValue = prev->countFullValue;
                        if(InterlockedCompareExchange64(&prev->countFullValue, (1ll << 63) | prevCountFullValue, prevCountFullValue) != prevCountFullValue) {
                            // Probably prev->rearranging, so abort
                            insideRef->rearranging = 0;
                            Reference_Release(&insideRef->prevRedirectValue, prev);
                            Reference_Release(&insideRef->nextRedirectValue, next);
                            break;
                        }


                        // Set next->rearranging to 1
                        uint64_t nextCountFullValue = next->countFullValue;
                        if(InterlockedCompareExchange64(&next->countFullValue, (1ll << 63) | nextCountFullValue, nextCountFullValue) != nextCountFullValue) {
                            // Probably next->rearranging, so abort
                            insideRef->rearranging = 0;
                            prev->rearranging = 0;
                            Reference_Release(&insideRef->prevRedirectValue, prev);
                            Reference_Release(&insideRef->nextRedirectValue, next);
                            break;
                        }

                        // We now have exclusive access to our node, and our siblings. So... we can remove our node now...

                        
                        if(!Reference_ReplaceOutside(&prev->nextRedirectValue, insideRef, next)) {
                            // Shouldn't happen, because rearranging is exclusively set nextRedirectValue should still point to us
                            OnError(9);
                            insideRef->rearranging = 0;
                            prev->rearranging = 0;
                            next->rearranging = 0;
                            Reference_Release(&insideRef->prevRedirectValue, prev);
                            Reference_Release(&insideRef->nextRedirectValue, next);
                            break;
                        }

                        if(!Reference_ReplaceOutside(&next->prevRedirectValue, insideRef, prev)) {
                            // Shouldn't happen, because rearranging is exclusively set nextRedirectValue should still point to us
                            // Hmm... I don't think we can really cleanup next->prevRedirectValue
                            // And then, we treat this the same as a regular successful destruction.
                            OnError(9);
                        }

                        // Replace prev first, as having a next but no prev is valid, but the other way around isn't.
                        if(!Reference_DestroyOutside(&insideRef->prevRedirectValue, prev)) {
                            // Shouldn't happen, rearranging should exclusively hold this
                            OnError(9);
                        }
                        if(!Reference_DestroyOutside(&insideRef->nextRedirectValue, next)) {
                            // Shouldn't happen, rearranging should exclusively hold this
                            OnError(9);
                        }

                        insideRef->rearranging = 0;
                        prev->rearranging = 0;
                        next->rearranging = 0;
                        Reference_Release(&insideRef->prevRedirectValue, prev);
                        Reference_Release(&insideRef->nextRedirectValue, next);
                    }
                } else {
                    // We have no previous, so we just deattach a single node
                    if(insideRef->count == 2) {
                        if(InterlockedCompareExchange64(&insideRef->countFullValue, (1ll << 63) | 2, 2) != 2) {
                            continue;
                        }

                        OutsideReference nextOutsideRef;
                        nextOutsideRef.valueForSet = InterlockedAdd64((LONG64*)&insideRef->nextRedirectValue, 1ll << BITS_IN_ADDRESS_SPACE);
                        InsideReference* next = nextOutsideRef.pointerClipped;
                        if(!next) {
                            insideRef->rearranging = 0;
                            continue;
                        }

                        // Set next->rearranging to 1
                        uint64_t nextCountFullValue = next->countFullValue;
                        if(InterlockedCompareExchange64(&next->countFullValue, (1ll << 63) | nextCountFullValue, nextCountFullValue) != nextCountFullValue) {
                            // Probably next->rearranging, so abort
                            insideRef->rearranging = 0;
                            Reference_Release(&insideRef->nextRedirectValue, next);
                            break;
                        }

                        if(!Reference_DestroyOutside(&next->prevRedirectValue, insideRef)) {
                            // Shouldn't happen, rearranging should exclusively hold this
                            OnError(9);
                        }

                        if(!Reference_DestroyOutside(&insideRef->nextRedirectValue, insideRef)) {
                            // Shouldn't happen, rearranging should exclusively hold this
                            OnError(9);
                        }

                        insideRef->rearranging = 0;
                        next->rearranging = 0;
                        Reference_Release(&insideRef->nextRedirectValue, next);
                    }
                }
            }
        }
        break;
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

    if(!pRef->pointerClipped) {
        return nullptr;
    }
    if(pRef->isNull) {
        return nullptr;
    }


    OutsideReference refForSet;
    // Getting a reference to a nullptr can happen, but it is fine, we can just ignore it. It won't happen
    //  enough to overrun the ref count, as only threads that have seen it have a value once will try this,
    //  so it won't continue to build up over time.
    refForSet.valueForSet = InterlockedAdd64((LONG64*)pRef, 1ll << BITS_IN_ADDRESS_SPACE);

    InsideReference* ref = PACKED_POINTER_GET_POINTER(refForSet);
    if(!ref) return nullptr;
    if(IsInsideRefCorrupt(ref)) return nullptr;


    OutsideReference* nextOutsideRef = nullptr;
    InsideReference* nextRef = ref;
    while(nextRef->nextRedirectValue.pointerClipped) {
        nextOutsideRef = &nextRef->nextRedirectValue;
        OutsideReference nextRefRead;
        nextRefRead.valueForSet = InterlockedAdd64((LONG64*)nextOutsideRef, 1ll << BITS_IN_ADDRESS_SPACE);
        nextRef = PACKED_POINTER_GET_POINTER(nextRefRead);

        if(!nextRef) return nullptr;
        if(IsInsideRefCorruptInner(nextRef)) return nullptr;
    }

    if(nextOutsideRef) {
        // Make the outside reference point to the new, deepest, inside reference
        if(!Reference_ReplaceOutside(pRef, ref, nextRef)) {
            // Outside ref has become something else. Free the refs we got, and tail recurse
            Reference_Release(nextOutsideRef, nextRef);
            Reference_Release(pRef, ref);
            return Reference_Acquire(pRef);
        }
        // Free our original ref (and we already have a reference to nextRef, so we don't have to worry about that)
        Reference_Release(pRef, ref);
        return nextRef;
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

bool Reference_ReplaceOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef, InsideReference* newInsideRef) {
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
        if(pInsideRef->count > (1ull << (64 - BITS_IN_ADDRESS_SPACE - 3))) {
            // Getting close to running out of references...
            OnError(1);
        }


        // Now try to remove from outside ref. If we fail... we have to go fix insideRef before we retry again
        OutsideReference outsideRefOriginal = outsideRef;
        outsideRef.count = 0;
        outsideRef.pointerClipped = newInsideRef;

		if (!XchgOutsideReference(pOutsideRef, &outsideRefOriginal, &outsideRef)) {
			// Subtract outsideRefOriginal.count from insideRef, but don't worry about it destructing the inside ref, the caller has a reference to it anyway
			insideRefDelta -= amountAdded;
			continue;
		}

        if(newInsideRef) {
            InterlockedAdd64((LONG64*)newInsideRef, 1ll << BITS_IN_ADDRESS_SPACE);
        }

        // Now that the outside ref is atomically gone, we can remove the outside's own ref to the inside ref
        //  (this is NOT the reference the caller has, this is the outside reference's own ref, completely different)
        if(releaseInsideReference(pInsideRef)) {
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
bool Reference_DestroyOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef) {
    return Reference_ReplaceOutside(pOutsideRef, pInsideRef, nullptr);
}

bool Reference_SetOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef) {
    if(IsOutsideRefCorrupt(*pOutsideRef)) return false;
    if(IsInsideRefCorrupt(pInsideRef)) return false;

    while(true) {
        OutsideReference outsideRef = *pOutsideRef;

        if((void*)PACKED_POINTER_GET_POINTER(outsideRef) != 0) return false;
        // The ref count on a nullptr can be positive. We will never release this ref, so it doesn't matter, just wipe it out
        //if(outsideRef.count != 0) return false;

        InterlockedIncrement64((LONG64*)&pInsideRef->count);

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