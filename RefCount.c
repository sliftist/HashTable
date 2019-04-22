#include "RefCount.h"
#include "AtomicHelpers.h"




bool XchgOutsideReference(
	OutsideReference* structAddress,
	OutsideReference* structOriginal,
	OutsideReference* structNew
) {
    return InterlockedCompareExchange64((LONG64*)structAddress, *(LONG64*)structNew, *(LONG64*)structOriginal) == *(LONG64*)structOriginal;
}

#if !defined(NDEBUG) && defined(_DEBUG)
#define DEBUG
#endif

#ifdef DEBUG
#define IsInsideRefCorrupt(ref, pRef) IsInsideRefCorruptInner(ref, pRef)
#define IsOutsideRefCorrupt(ref) IsOutsideRefCorruptInner(ref)
#else
#define IsInsideRefCorrupt(ref, pRef) false
#define IsOutsideRefCorrupt(ref) false
#endif

bool IsInsideRefCorruptInner(InsideReference ref, InsideReference* pRef) {
	if (!pRef) {
		// nullptr passed as InsideReference...
		OnError(3);
		return true;
	}
    if (ref.pointer == (void*)0xdddddddddddddddd) {
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
	if ((uint64_t)ref.pointer != ((uint64_t)pRef + sizeof(InsideReference))) {
		// The inside ref isn't pointing to itself, so it isn't an inside ref, and this is totally invalid!
		OnError(11);
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
	if (ref.pointerClipped) {
		InsideReference* insideRef = (void*)PACKED_POINTER_GET_POINTER(ref);
        if(IsInsideRefCorrupt(*insideRef, insideRef)) {
            return true;
        }
	}
    return false;
}


void Reference_AllocateInner(uint64_t size, OutsideReference* outRef, void** outPointer, const char* file, int line) {
    InsideReference* insideRef = malloc(size + sizeof(InsideReference));
    if(!insideRef) {
        OnError(2);
        *outPointer = nullptr;
        return;
    }

    file;
    line;
    //printf("Allocate %p, %s:%d\n", insideRef, file, line);
    
    Reference_RecycleAllocate(insideRef, size, outRef, outPointer);
}

void Reference_RecycleAllocate(void* emptyAllocation, uint64_t size, OutsideReference* outRef, void** outPointer) {
    InsideReference* insideRef = emptyAllocation;

    memset(insideRef, 0, size + sizeof(InsideReference));
    insideRef->pointer = (void*)((byte*)insideRef + sizeof(InsideReference));

    // 1, for the OutsideReference
    insideRef->count = 1;

    OutsideReference outsideRef = { 0 };
    outsideRef.pointerClipped = (uint64_t)insideRef;
    // 0, outsid
    outsideRef.count = 0;

    *outRef = outsideRef;
    *outPointer = insideRef->pointer;
}

InsideReference* Reference_Acquire(OutsideReference* pRef) {
    while(true) {
        if(!pRef->pointerClipped) {
            return nullptr;
        }
        OutsideReference ref;
        ref.value = InterlockedAdd64((LONG64*)pRef, 1ll << BITS_IN_ADDRESS_SPACE);

        #ifdef DEBUG
        // Getting a reference to a nullptr can happen, but it is fine, we can just ignore it. It won't happen
        //  enough to overrun the ref count, as only threads that have seen it have a value one will try this,
        //  so it won't continue to build up over time.
        if(!ref.pointerClipped) {
            return nullptr;
        }

        if(IsOutsideRefCorrupt(ref)) return nullptr;

        #ifdef DEBUG
        if(ref.count > (1ull << (64 - BITS_IN_ADDRESS_SPACE - 3))) {
            // Getting close to running out of references...
            OnError(1);
            return nullptr;
        }
        #endif

        // We have a reference to the outside reference, which we verified has a real inside reference, and so
        //  even if the outside reference is destructed immediately, that will just move our reference to the inside
        //  reference, and if it isn't destructed its own reference will keep the inside reference alive. So... this works...

        InsideReference* insideRef = (void*)PACKED_POINTER_GET_POINTER(ref);
        if(IsInsideRefCorrupt(*insideRef, insideRef)) return nullptr;

        return insideRef;
        #else
        return (void*)PACKED_POINTER_GET_POINTER(ref);
        #endif
    }
}

bool releaseInsideReference(InsideReference* insideRef, bool dontFree, const char* file, int line, OutsideReference* outsideRef) {
    if(IsInsideRefCorrupt(*insideRef, insideRef)) return false;

    LONG64 decrementResult = InterlockedDecrement64((LONG64*)&insideRef->count);

    //printf("release outside %p, inside %p, %s:%d\n", outsideRef, insideRef, file, line);

    if(decrementResult == 0) {
        // This means our OutsideReference must be gone, and in fact all ourside references are gone... and
        //  we were the last reference, so... free it.
        // (oh, and this is fine, we aren't going inside insideRef, we are using the actual pointer, where
        //  is an argument, so it is thread safe, and the actual pointer we want to free)
        if(!dontFree) {
            free(insideRef);
            file;
            line;
            //printf("free %p, %s:%d\n", insideRef, file, line);
        }
        return true;
    }
    return false;
}

bool Reference_ReleaseInner(OutsideReference* outsideRef, InsideReference* insideRef, bool dontFree, const char* file, int line) {
    if(!insideRef) {
        // This saves our cleanup code constantly null checking insideRef. Instead it can just unconditional
        //  release it, we we can null check it.
        return false;
    }
    // If our reference still exists in OutsideReference this is easy, just decrement the count.
    //  Of course, we may not be able to, even if OutsideReference is valid and correlates to our insideRef...
    //  because of stuff... but that is fine, if our reference isn't in the outside reference for any reason,
    //  it will have been moved to inside ref, and we can decrement it from there.

    if(outsideRef) {
        while(true) {
            OutsideReference ref = *outsideRef;
            OutsideReference refOriginal = ref;

            if(IsOutsideRefCorrupt(ref)) return false;

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

            // So... OutsideReference still exists. If/when it is destroyed, the inside reference might be freed,
            //  but until then the inside reference has to stay alive (as the outside reference is still exposing it),
            //  so... just return, nothing more to do.
            return false;
        }
    }

    // The OutsideReference is dead. Our reference MUST be in the inside reference. References (counts) only
    //  flow from the outside to the inside.
    //  (while new references may be added to the outside reference, and it may be reused with the same pointer,
    //      meaning it is possible to retry to use the outside reference, we should always be able to dereference
    //      from the inside reference, as our reference had to go somewhere if it wasn't in the outside ref...)
    return releaseInsideReference(insideRef, dontFree, file, line, outsideRef);
}


bool Reference_DestroyOutsideInner(OutsideReference* pOutsideRef, InsideReference* pInsideRef, const char* file, int line) {
    // First move the references from outside ref to inside ref (causing them to be duplicated for a bit)
    //  (and which if removing from the outside ref fails may require removing from the inside ref)
    // Then try to make the outside ref wiped out

	// As we make changes in two parts, we have to keep track of the changes we made to the inner count, so our retry loop can
	//	combine the new changes with undoing the previous changes, which makes this more efficient (for retries).
	int64_t insideRefDelta = 0;

    int loops = 0;

    while(true) {
        loops++;
		OutsideReference outsideRef = *pOutsideRef;

        if(IsOutsideRefCorrupt(outsideRef)) return false;
        if(IsInsideRefCorrupt(*pInsideRef, pInsideRef)) return false;
        
        if((void*)PACKED_POINTER_GET_POINTER(outsideRef) != pInsideRef) {
			if (insideRefDelta != 0) {
                InterlockedAdd64((LONG64*)&pInsideRef->count, (LONG64)insideRefDelta);
			}
			return false;
        }

		int64_t amountAdded = outsideRef.count + insideRefDelta;
        InterlockedAdd64((LONG64*)&pInsideRef->count, (LONG64)amountAdded);
        if(pInsideRef->count > (1ull << (64 - BITS_IN_ADDRESS_SPACE - 3))) {
            // Getting close to running out of references...
            OnError(1);
        }


        // Now try to remove from outside ref. If we fail... we have to go fix insideRef before we retry again
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
        if(releaseInsideReference(pInsideRef, nullptr, __FILE__, __LINE__, pOutsideRef)) {
            // If our reference's own reference was the last reference, that means the caller lied about
            //  having a reference, and bad stuff is going to happen...
            OnError(3);
        }
        if(loops > 1) {
            //printf("MULTIPLE LOOPS!!!! loops %d\n", loops);
        }
        //printf("moved %lld to inside from %p to %p, %s:%d\n", amountAdded, pOutsideRef, pInsideRef, file, line);
        if(loops == 1 && amountAdded == 0) {
            // (loops == 1 because retries will mess up amountAdded. We can check if loops > 1, it just takes more code,
            //      and this case should cover it anyway...)
            // This is actually invalid. You have to gain a reference via the outside reference to release it.
            //  We checked to make sure your reference was for the outside reference, but if it has no references,
            //  you must have obtained your reference via a different outside reference, which is bad.
            OnError(3);
        }
        return true;
    }
    return false;
}

// TODO: Before we expose this, we need to fix our retry loop to acknowledge replacing existing references
//  prevRef is optional, if passed outsideRef may be still in existence (and will be compared against prevRef) to verify
//      we are replacing the correct reference.
//  insideRef is optional, if not passed we just call Reference_DestroyOutside
bool Reference_SetOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef) {
    while(true) {
        OutsideReference outsideRef = *pOutsideRef;

        if(IsOutsideRefCorrupt(outsideRef)) return false;
        if(IsInsideRefCorrupt(*pInsideRef, pInsideRef)) return false;

        if((void*)PACKED_POINTER_GET_POINTER(outsideRef) != 0) return false;
        // The ref count on a nullptr can be positive. We will never release this ref, so it doesn't matter,
        //  just wipe it out
        //if(outsideRef.count != 0) return false;

        InterlockedIncrement64((LONG64*)&pInsideRef->count);

        OutsideReference outsideRefOriginal = outsideRef;
        outsideRef.count = 0;
        outsideRef.pointerClipped = (uint64_t)pInsideRef;
        // So, going from {0, 0} to {0, pInsideRef}
        if(!XchgOutsideReference(pOutsideRef, &outsideRefOriginal, &outsideRef)) {
            // Failure means it is already being used. So, undo our inside ref increment, and abort
            if(releaseInsideReference(pInsideRef, nullptr, __FILE__, __LINE__, pOutsideRef)) {
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