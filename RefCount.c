#include "RefCount.h"
#include "AtomicHelpers.h"




bool XchgOutsideReference(
	OutsideReference* structAddress,
	OutsideReference* structOriginal,
	OutsideReference* structNew
) {
    return InterlockedCompareExchange64((LONG64*)structAddress, *(LONG64*)structNew, *(LONG64*)structOriginal) == *(LONG64*)structOriginal;
}
bool XchgInsideReference(
	InsideReference* structAddress,
	InsideReference* structOriginal,
	InsideReference* structNew
) {
    return InterlockedCompareExchange64((LONG64*)structAddress, *(LONG64*)structNew, *(LONG64*)structOriginal) == *(LONG64*)structOriginal;
}


bool IsOutsideRefCorrupt(OutsideReference ref) {
    if(ref.count < 0) {
        // Is the outside reference being used for memory other than an outside reference? If so... that's bad,
        //  because it could overlap with the previous outside ref... which will break everything.
        OnError(3);
        return true;
    }
    return false;
}
bool IsInsideRefCorrupt(InsideReference ref, InsideReference* pRef) {
    if(!pRef) {
        // nullptr passed as InsideReference...
        OnError(3);
        return true;
    }
    if(ref.count <= 0) {
        // Something decremented our ref count more times than it held it... this is invalid...
        OnError(3);
        return true;
    }
    if((uint64_t)PACKED_POINTER_GET_POINTER(ref) != ((uint64_t)pRef + sizeof(InsideReference))) {
        // The inside ref isn't pointing to itself, so it isn't an inside ref, and this is totally invalid!
        OnError(11);
        return true;
    }
    return false;
}


void Reference_Allocate(uint64_t size, OutsideReference* outRef, void** outPointer) {
    InsideReference* insideRef = malloc(size + sizeof(InsideReference));
    if(!insideRef) {
        OnError(2);
        *outPointer = nullptr;
        return;
    }
    memset(insideRef, 0, size + sizeof(InsideReference));
    insideRef->pointerClipped = (uint64_t)((byte*)insideRef + sizeof(InsideReference));

    // 1, for the OutsideReference
    insideRef->count = 1;

    OutsideReference outsideRef = { 0 };
    outsideRef.pointerClipped = (uint64_t)insideRef;
    // 0, outsid
    outsideRef.count = 0;

    *outRef = outsideRef;
    *outPointer = (void*)PACKED_POINTER_GET_POINTER(*insideRef);
}

InsideReference* Reference_Acquire(OutsideReference* pRef) {
    while(true) {
        OutsideReference ref = *pRef;
        OutsideReference refOriginal = ref;

        if(!ref.pointerClipped) {
            return nullptr;
        }

        if(IsOutsideRefCorrupt(ref)) return nullptr;

        ref.count++;
        if(ref.count > (1ull << (64 - BITS_IN_ADDRESS_SPACE - 3))) {
            // Getting close to running out of references...
            OnError(1);
            return nullptr;
        }

        if(!XchgOutsideReference(pRef, &refOriginal, &ref)) continue;

        // We have a reference to the outside reference, which we verified has a real inside reference, and so
        //  even if the outside reference is destructed immediately, that will just move our reference to the inside
        //  reference, and if it isn't destructed its own reference will keep the inside reference alive. So... this works...

        InsideReference* insideRef = (void*)PACKED_POINTER_GET_POINTER(ref);
        if(IsInsideRefCorrupt(*insideRef, insideRef)) return nullptr;

        return insideRef;
    }
}

void releaseInsideReference(InsideReference* insideRef, InsideReference** pointerToFree) {
    while(true) {
        InsideReference ref = *insideRef;
        InsideReference refOriginal = ref;

        if(IsInsideRefCorrupt(ref, insideRef)) return;

        ref.count--;

        if(!XchgInsideReference(insideRef, &refOriginal, &ref)) continue;

        if(ref.count == 0) {
            // This means our OutsideReference must be gone, and in fact all ourside references are gone... and
            //  we were the last reference, so... free it.
            // (oh, and this is fine, we aren't going inside insideRef, we are using the actual pointer, where
            //  is an argument, so it is thread safe, and the actual pointer we want to free)
            if(pointerToFree) {
                *pointerToFree = insideRef;
            } else {
                free(insideRef);
            }
        }
        return;
    }
}

void Reference_Release(OutsideReference* outsideRef, InsideReference* insideRef, InsideReference** pointerToFree) {
    // If our reference still exists in OutsideReference this is easy, just decrement the count.
    //  Of course, we may not be able to, even if OutsideReference is valid and correlates to our insideRef...
    //  because of stuff... but that is fine, if our reference isn't in the outside reference for any reason,
    //  it will have been moved to inside ref, and we can decrement it from there.

    while(true) {
        OutsideReference ref = *outsideRef;
        OutsideReference refOriginal = ref;

        if(IsOutsideRefCorrupt(ref)) return;

		// Break when the outside reference is dead

        // If the outer reference doesn't even refer to this inside reference, then our reference must have been moved
        //  to the inner reference
        if((void*)PACKED_POINTER_GET_POINTER(ref) != insideRef) break;
        // If there is nothing to decrement in the outer reference, our reference must have been moved to the inner,
        //  so... go release it there
        if(ref.count == 0) break;

        ref.count--;
        if(!XchgOutsideReference(outsideRef, &refOriginal, &ref)) continue;

        // So... OutsideReference still exists. If/when it is destroyed, the inside reference might be freed,
        //  but until then the inside reference has to stay alive (as the outside reference is still exposing it),
        //  so... just return, nothing more to do.
        return;
    }

    // The OutsideReference is dead. Our reference MUST be in the inside reference. References (counts) only
    //  flow from the outside to the inside.
    //  (while new references may be added to the outside reference, and it may be reused with the same pointer,
    //      meaning it is possible to retry to use the outside reference, we should always be able to dereference
    //      from the inside reference, as our reference had to go somewhere if it wasn't in the outside ref...)
    releaseInsideReference(insideRef, pointerToFree);
}


bool Reference_DestroyOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef, InsideReference** pointerToFree) {
    // First move the references from outside ref to inside ref (causing them to be duplicated for a bit)
    //  (and which if removing from the outside ref fails may require removing from the inside ref)
    // Then try to make the outside ref wiped out

	// As we make changes in two parts, we have to keep track of the changes we made to the inner count, so our retry loop can
	//	combine the new changes with undoing the previous changes, which makes this more efficient (for retries).
	int64_t insideRefDelta = 0;

    while(true) {
		OutsideReference outsideRef = *pOutsideRef;
		InsideReference insideRef = *pInsideRef;

        if(IsOutsideRefCorrupt(outsideRef)) return false;
        if(IsInsideRefCorrupt(insideRef, pInsideRef)) return false;

        InsideReference insideRefOriginal = insideRef;
        
        if((void*)PACKED_POINTER_GET_POINTER(outsideRef) != pInsideRef) {
			if (insideRefDelta != 0) {
				while (true) {
					InsideReference insideRef2 = *pInsideRef;
					InsideReference insideRef2Original = insideRef2;
					insideRef2.count += insideRefDelta;
					if (!XchgInsideReference(pInsideRef, &insideRef2Original, &insideRef2)) continue;
					break;
				}
			}
			return false;
        }

		int64_t amountAdded = outsideRef.count + insideRefDelta;
        insideRef.count += amountAdded;
        if(insideRef.count > (1ull << (64 - BITS_IN_ADDRESS_SPACE - 3))) {
            // Getting close to running out of references...
            OnError(1);
        }
		
        if(!XchgInsideReference(pInsideRef, &insideRefOriginal, &insideRef)) continue;

        if((void*)PACKED_POINTER_GET_POINTER(outsideRef) != pInsideRef) {
            // We just went this far to undo our inside reference changes, but we have nothing more to do now.
            return false;
        }

        // Now try to remove from outside ref. If we fail... we have to go fix insideRef before we retry
        OutsideReference outsideRefOriginal = outsideRef;
        outsideRef.count = 0;
        outsideRef.pointerClipped = nullptr;

		if (!XchgOutsideReference(pOutsideRef, &outsideRefOriginal, &outsideRef)) {
			// Subtract outsideRefOriginal.count from insideRef, but don't worry about it destructing the inside ref, the caller has a reference to it anyway
			insideRefDelta -= amountAdded;
			continue;
		}

        // Now that the outside ref is atomically gone, we can remove the outside's own ref to the inside ref
        //  (this is NOT the reference the caller has, this is the outside reference's own ref, completely different)
        releaseInsideReference(pInsideRef, pointerToFree);
        return true;
    }
    return false;
}

bool Reference_SetOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef, InsideReference** pointerToFree) {
    while(true) {
        OutsideReference outsideRef = *pOutsideRef;
        InsideReference insideRef = *pInsideRef;

        if(IsOutsideRefCorrupt(outsideRef)) return false;
        if(IsInsideRefCorrupt(insideRef, pInsideRef)) return false;

        if((void*)PACKED_POINTER_GET_POINTER(outsideRef) != nullptr) return false;
        if(outsideRef.count != 0) return false;

        // We must add the inside reference first, because as soon as we make the outside reference it is open to being destructed,
        //  which requires having an inside reference, or else the destruct will decrement our own inside reference, ruining everything!
        InsideReference insideRefOriginal = insideRef;
        insideRef.count++;
        if(!XchgInsideReference(pInsideRef, &insideRefOriginal, &insideRef)) continue;


        OutsideReference outsideRefOriginal = outsideRef;
        outsideRef.count = 0;
        outsideRef.pointerClipped = (uint64_t)pInsideRef;
        // So, going from {0, 0} to {0, pInsideRef}
        if(!XchgOutsideReference(pOutsideRef, &outsideRefOriginal, &outsideRef)) {
            // Failure means it is already being used. So, undo our inside ref increment, and abort
            releaseInsideReference(pInsideRef, pointerToFree);
            return false;
        }
        return true;
    }
    return false;
}