#include "RefCount.h"
#include "AtomicHelpers.h"
#include "MemPool.h"

#define IS_NULL_POINTER IS_FROZEN_POINTER


// ATTTENTION! We can't call PACKED_POINTER_GET_POINTER on any threaded values, because there is a race between access of p.
//  (which is one of the reasons we don't wrap p with parentheses)

// If the highest bit in the address space is set, then we have to set all the high bits. Otherwise bit fields does the rest for us.
//  (and all other fields can just be get/set via the bitfield)
#if defined(KERNEL) && defined(_MSC_VER)
// Windows kernel space may have kernel addresses
//  (the highest bit must be extended, so if the highest bit is set, there must be all 1s)
#define PACKED_POINTER_GET_POINTER_VAL(p) (GET_NULL(p) ? 0 : ((GET_POINTER_CLIPPED(p) & (1ull << (BITS_IN_ADDRESS_SPACE - 1))) ? (GET_POINTER_CLIPPED(p) | 0xFFFF000000000000ull) : GET_POINTER_CLIPPED(p)))
#else
#define PACKED_POINTER_GET_POINTER_VAL(p) (GET_NULL(p) ? 0 : GET_POINTER_CLIPPED(p))
#endif

#define PACKED_POINTER_GET_POINTER(p) (PACKED_POINTER_GET_POINTER_VAL(p.valueForSet))

#define UNSET_NULL(ref) ((ref).valueForSet & ~(1ull << 63))

#define GET_NULL_POINTER_VALUE(ref) (!IS_NULL_POINTER(ref) ? 0 : PACKED_POINTER_GET_POINTER_VAL( (UNSET_NULL(ref)) ) )

#define SET_NON_POINTER_PATTERN(value) ((uint64_t)(value) & ~(1ull << 62) | (1ull << 63))
#define HAS_NON_POINTER_PATTERN(value) ((((uint64_t)(value) & ((1ull << 63) | (1ull << 62))) >> 62) == 0x2)


const OutsideReference emptyReference = { 0 };

// Anyone with InsideReference*, should either have a count incremented in the reference count,
//  or an OutsideReference that points to that InsideReference*.


#ifdef DEBUG_INSIDE_REFERENCES
#define DebugLog(operation, ref) DebugLog2(operation, ref, __FILE__, __LINE__)
void DebugLog2(const char* operation, InsideReference* ref, const char* file, uint64_t line) {
	if (!ref) return;
    DebugLineInfo info = { 0 };
    info.file = file;
    info.line = line;
    info.operation = operation;
    info.state.valueForSet = ref->valueForSet;
    uint64_t eventIndex = InterlockedIncrement64((LONG64*)&ref->nextEventIndex) - 1;
    ref->events[eventIndex % EVENT_ID_COUNT] = info;
}
#else
#define DebugLog(file, line) false
#define DebugLog2(file, line, operation, ref) false
#endif

// I guess it can be more than 4 64 bit ints, but... the larger it is, the worse memory performance we get for hash tables
//  with small items...
//CASSERT(sizeof(InsideReference) == 4 * sizeof(uint64_t));
CASSERT(sizeof(InsideReference) == InsideReferenceSize);

void BitFieldSubtract(uint64_t* pValue, uint64_t valueToSubtract, uint64_t bitIndex) {
    while(true) {
        uint64_t value = *pValue;
        uint64_t newValue = value - (valueToSubtract << bitIndex);
        if(InterlockedCompareExchange64(
            (LONG64*)pValue,
            newValue,
            value
        ) == value) {
            break;
        }
    }
}

bool IsInsideRefCorruptInner(InsideReference* pRef, bool allowFreed) {
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
    if(ref.outsideCount > ref.count && ref.outsideCount < 1024) {
        // This means there are outstanding outside references with references that have been orphaned. This is bad...
        OnError(3);
        return true;
    }
	if (!allowFreed && ref.count <= 0) {
		//todonext
		// Hmm... based on the other error... it IS like someone else is freeing our reference. So... we should probably
		//	starting tracking the destructor of references via passing FILE and LINE, and then storing them in the redirect memory
		//	slots... And maybe... we could add tags to the lower bits of InsideReferences we return, so we can correlate acquires
		//	with releases? Ugh... that is a lot of work though...
		//todonext
		// Oh, so... our value exists, that is fine, but it does seem to be relatively close to the end of the allocation... somehow?
		//	At least there are 0xcd values close to it (not THAT close, but visible in the memory window).

		// Something decremented our ref count more times than it held it... this is invalid...
		//  Our outside references should store at least once reference, and if not the caller should have
		//  a reference, or else accessing it is invalid.
		OnError(3);
		return true;
	}
	if (ref.count > 100) {
		//todonext
		// We have a leak somewhere, only with multiple threads, so it is likely in a contention retry case.
		//	Probably in how we follow redirects?
		if (pRef->pool != (void*)&memPoolSystem) {
			breakpoint();
		}
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
	if (ref.count > 100) {
		breakpoint();
	}
	/* Wait... we can't check the inside ref like this before we get an actual reference! We are dereferences the outside reference without
			getting a reference to it!
    InsideReference* insideRef = (void*)PACKED_POINTER_GET_POINTER(ref);
    if(insideRef) {
        if(IsInsideRefCorrupt(insideRef)) return true;
    }
	*/
    return false;
}


bool Reference_HasBeenMoved(InsideReference* ref) {
    return !!ref->isCommittedToMove;
}

uint64_t Reference_GetHash(InsideReference* ref) {
    return ref->hash;
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

InsideReference* Reference_AcquireInside(OutsideReference* pRef) {
    InsideReference* ref = Reference_Acquire(pRef);
	if (!ref) return nullptr;
    //printf("acquiring inside reference for %p have %llu\n", ref, ref->count);
	InterlockedIncrement64((LONG64*)&ref->valueForSet);
	Reference_Release(pRef, ref);
    //printf("acquired inside reference for %p have %llu\n", ref, ref->count);
	return ref;
}

bool releaseInsideReferenceX(InsideReference* insideRef, const char* file, uint64_t line);
#define releaseInsideReference(insideRef) releaseInsideReferenceX(insideRef, __FILE__, __LINE__)

InsideReference* Reference_AcquireCheckIsMovedProof(OutsideReference* pRef, bool* outIsMoved, bool* outIsFrozen, OutsideReference* refSeen) {
    while(true) {
        OutsideReference ref = *pRef;
        if(refSeen) {
            *refSeen = ref;
        }
        if(!ref.pointerAndCount) {
            // So 0, or BASE_NULL
            return nullptr;
        }
        if(ref.isNull) {
            *outIsMoved = true;
            if(ref.pointerClipped) {
                *outIsFrozen = true;
            }
            return nullptr;
        }
        OutsideReference newRef = ref;
        newRef.count++;
        if(InterlockedCompareExchange64(
            (LONG64*)pRef,
            newRef.valueForSet,
            ref.valueForSet
        ) == ref.valueForSet) {
            InsideReference* insideRef = (void*)PACKED_POINTER_GET_POINTER(ref);
            DebugLog("Reference_AcquireCheckIsMoved (ACQUIRED)", insideRef);
            return insideRef;
        }
    }
}

#define Reference_AcquireIfNull(ref) Reference_AcquireIfNullX(ref, __FILE__, __LINE__)
InsideReference* Reference_AcquireIfNullX(OutsideReference* pRef, const char* file, uint64_t line) {
    // Get outside reference
    InsideReference* ref = nullptr;
    while(true) {
        OutsideReference curRef = *pRef;
        if(IsOutsideRefCorrupt(curRef)) return nullptr;
        if(!IS_NULL_POINTER(curRef)) {
            return nullptr;
        }
        OutsideReference newRef = curRef;
        newRef.count++;
        if(InterlockedCompareExchange64(
            (LONG64*)pRef,
            newRef.valueForSet,
            curRef.valueForSet
        ) == curRef.valueForSet) {
            newRef.isNull = 0;
            ref = (void*)PACKED_POINTER_GET_POINTER(newRef);
            if(IsInsideRefCorrupt(ref)) return nullptr;
            DebugLog2("Reference_AcquireIfNull (ACQUIRED)", ref, file, line);
            break;
        }
    }

    // Safe, as we just added an outside ref
    InterlockedIncrement64(&ref->valueForSet);

    DebugLog("AcquireIfNull (MIDDLE)", ref);

    // Now remove the outside ref we added
	while (true) {
		OutsideReference curRef = *pRef;
		if (!curRef.isNull) break;
		curRef.isNull = 0;
		if(!FAST_CHECK_POINTER(curRef, ref)) break;
        curRef.isNull = 1;
        if(curRef.count == 0) break;
        OutsideReference newRef = curRef;
		newRef.count--;
        if(InterlockedCompareExchange64(
            (LONG64*)pRef,
            newRef.valueForSet,
            curRef.valueForSet
        ) == curRef.valueForSet) {
            IsInsideRefCorrupt(ref);
            return ref;
        }
	}

	// If we get here, it means our ref got moved to the inside, so we have to remove it from the inside
    releaseInsideReference(ref);

    IsInsideRefCorrupt(ref);

    return ref;
}

void setCommittedToMove(InsideReference* ref) {
    while(true) {
        InsideReferenceCount count;
        count.valueForSet = ref->valueForSet;
        InsideReferenceCount newCount = count;
        newCount.isCommittedToMove = 1;
        if(InterlockedCompareExchange64(
            (LONG64*)&ref->valueForSet,
            newCount.valueForSet,
            count.valueForSet
        ) == count.valueForSet) {
            break;
        }
    }
}

int Reference_AcquireStartMove(OutsideReference* pRef, MemPool* newPool, uint64_t size, InsideReference** outNewRef) {
    //todonext
    // Wait, if it is zero here... shouldn't we just immediately convert it to BASE_NULL2, and then skip the rest of the move code?
    //  I think this would fix a bug we have, and simplify the code considerably...

    OutsideReference outRef = *pRef;
    if(outRef.valueForSet == 0) {
        if(InterlockedCompareExchange64(
            (LONG64*)pRef,
            BASE_NULL2.valueForSet,
            0
        ) != outRef.valueForSet) {
            // Changed since we first read it
            return Reference_AcquireStartMove(pRef, newPool, size, outNewRef);
        }
        return 0;
    }
    if(outRef.valueForSet == BASE_NULL.valueForSet) {
        // We can't make BASE_NULL into a block end, because it still isn't a block end, just because it moved...
        if(InterlockedCompareExchange64(
            (LONG64*)pRef,
            BASE_NULL1.valueForSet,
            BASE_NULL.valueForSet
        ) != outRef.valueForSet) {
            // Changed since we first read it
            return Reference_AcquireStartMove(pRef, newPool, size, outNewRef);
        }
        return 0;
    }

    if(outRef.isNull && outRef.pointerClipped) {
        // Already frozen and copied, so acquire the reference, and then return
        *outNewRef = Reference_AcquireIfNull(pRef);
        DebugLog("Reference_AcquireStartMove already moved", *outNewRef);
        return 0;
    }

    if(outRef.valueForSet == BASE_NULL2.valueForSet || outRef.valueForSet == BASE_NULL1.valueForSet) {
        DebugLog("Reference_AcquireStartMove already finished moving", *outNewRef);
        return 0;
    }

	
	bool isMoved = false;
	bool isFrozen = false;
    InsideReference* oldRef = Reference_AcquireCheckIsMoved(pRef, &isMoved, &isFrozen);
    if(!oldRef) {
        // Changed since we first read it
        return Reference_AcquireStartMove(pRef, newPool, size, outNewRef);
    }




    OutsideReference newOutRef;
    byte* newValue = nullptr;
    Reference_Allocate(newPool, &newOutRef, &newValue, size, oldRef->hash);
    if(!newValue) {
        Reference_Release(pRef, oldRef);
        return 3;
    }
    byte* oldValue = Reference_GetValueFast(oldRef);
    memcpy(newValue, oldValue, size);



    InsideReference* newRef = (void*)PACKED_POINTER_GET_POINTER(newOutRef);

    
    #ifdef DEBUG_INSIDE_REFERENCES
    newRef->unsafeSource = oldRef;
    #endif

    // The ref is still uniquely held, so this is fine!
    // (get the ref for our outNewRef parameter)
    newRef->count++;
    newOutRef.isNull = 1;

    DebugLog("Reference_AcquireStartMove, ref still private", newRef);

    // Must be set before we have the changes of duplicates
    setCommittedToMove(oldRef);

    if(!Reference_ReplaceOutside(
        pRef,
        oldRef,
        newOutRef
    )) {
        newOutRef.isNull = 0;
		Reference_Release(pRef, oldRef);
        if(!Reference_DestroyOutside(&newOutRef, newRef)) {
            // Impossible
            OnError(3);
        }
        //MemLog_Add((void*)0, newRef, "not using alloc anyway", newRef->hash);

        Reference_Release(&emptyReference, newRef);
        // Retry, we should get the new allocatin right away this time.
        return Reference_AcquireStartMove(pRef, newPool, size, outNewRef);
    }

    DebugLog("Reference_AcquireStartMove moving from", oldRef);
	Reference_Release(pRef, oldRef);

    *outNewRef = newRef;

    DebugLog("Reference_AcquireStartMove did move", *outNewRef);
    return 0;
}

bool Reference_CopyIntoZero(OutsideReference* target, InsideReference* newRef) {
    OutsideReference newOutsideRef = Reference_CreateOutsideReference(newRef);

    OutsideReference prevRef;
    prevRef.valueForSet = InterlockedCompareExchange64(
        (LONG64*)target,
        newOutsideRef.valueForSet,
        0
    );

    if(prevRef.valueForSet == 0) {
        return true;
    }

    DestroyUniqueOutsideRef(&newOutsideRef);

    return FAST_CHECK_POINTER(prevRef, newRef);
}

bool Reference_FinishMove(OutsideReference* pRef, InsideReference* insideRef) {
    // TODO: Maybe consider only using isCommittedToMove, and not even setting isNull. Of course
    //  Although... it would mean accessed would need accesses would need to check hash and isCommittedToMove...
    //  but we could probably move isCommittedToMove to be beside hash, which might help.
    //  - And we would still need to use a BASE_NULL value for empty slots, and fully moved slots.
    //  - And also, putting the new value in the outside reference slot... wouldn't work? Idk... so nevermind... maybe don't.


    while(true) {
        OutsideReference ref = *pRef;
        if(ref.valueForSet == BASE_NULL2.valueForSet || !ref.pointerClipped) {
            // Impossible, how did they get a ref from it, if it has been marked as previous empty (or has no value)?
            //  Even if it is finished it should become BASE_NULL1...
            OnError(3);
            return false;
        }

        if(ref.valueForSet == BASE_NULL1.valueForSet) {
            return false;
        }


        DebugLog("FinishMove", insideRef);
        if(ref.pointerClipped && !ref.isNull) {
            // Move never started?
            OnError(3);
        }
        OutsideReference refFortest = ref;
        refFortest.isNull = 0;
        if(!FAST_CHECK_POINTER(refFortest, insideRef)) {
            DebugLog("FinishMove, but the ref is different (probably because it was already finished)", insideRef);
            continue;
        }

        // Move the ref count to the inside
        InterlockedExchangeAdd64((LONG64*)&insideRef->valueForSet, ref.count);

        if(InterlockedCompareExchange64(
            (LONG64*)pRef,
            BASE_NULL1.valueForSet,
            ref.valueForSet
        ) == ref.valueForSet) {
            ref.isNull = 0;
            // Release once for ourself, and once to destroy the outside ref
            BitFieldSubtract(&insideRef->valueForSet, 1, OUTSIDE_COUNT_BIT_INDEX);
            BitFieldSubtract(&insideRef->valueForSet, 1, 0);
            DebugLog("FinishMove, success", insideRef);
            return true;
        } else {
            // Undo the move of the ref count and try again
            BitFieldSubtract(&insideRef->valueForSet, ref.count, 0);
            DebugLog("FinishMove, value changed (probably already moved)", insideRef);
        }
    }
    
    
}



void DestroyUniqueOutsideRefInner(OutsideReference* ref, OutsideReference nullValue) {
	InsideReference* temp = Reference_Acquire(ref);
	if (!temp) return;
    if (HAS_NON_POINTER_PATTERN(temp->pool)) {
        // Tried to double free value...
        breakpoint();
    }
    DebugLog("DestroyUniqueOutsideRefInner (ACQUIRED)", temp);
    Reference_ReplaceOutside(ref, temp, nullValue);
    DebugLog("DestroyUniqueOutsideRefInner (REPLACED)", temp);
	Reference_Release(&emptyReference, temp);
}
void DestroyUniqueOutsideRef(OutsideReference* ref) {
    DestroyUniqueOutsideRefInner(ref, emptyReference);
}

bool releaseInsideReferenceX(InsideReference* insideRef, const char* file, uint64_t line) {
    if(IsInsideRefCorrupt(insideRef)) return false;

    DebugLog2("releaseInsideReference (BEFORE)", insideRef, file, line);

    LONG64 decrementResult = InterlockedDecrement64((LONG64*)&insideRef->valueForSet);

    //printf("released inside reference %p, has %llu refs\n", insideRef, insideRef->count);
    // This is dangerous, but... I really want to know who is releasing the outside reference prematurely...
    // decrementResult
    InsideReferenceCount count;
    count.valueForSet = decrementResult;
    if(count.outsideCount > count.count && count.outsideCount < 1024) {
        // A big problem, you released a reference you obtained for an outside reference without that outside reference,
        //  which leaks a reference and will cause leaks or double frees.
        OnError(3);
    }
    
    // 0 means count is 0, outsideCount is 0 and destructStarted is not set.
	if (count.count == 0) {
        //printf("freeing reference memory %p\n", insideRef);
        if(IsInsideRefCorruptInner(insideRef, true)) {
            return true;
        }
		if (HAS_NON_POINTER_PATTERN(insideRef->pool)) {
			// Double free
			breakpoint();
		}

        //MemLog_Add((void*)0, insideRef, "freeing inside ref", insideRef->hash);

        //printf("freed reference memory redirects %p\n", insideRef);
		// No more references, and no more outside references so there will never be more references, so we can exclusively free now.
        MemPool* pool = insideRef->pool;
        bool workedBefore = HAS_NON_POINTER_PATTERN(insideRef->pool);
        insideRef->pool = (void*)SET_NON_POINTER_PATTERN(insideRef->pool);
        bool worked = HAS_NON_POINTER_PATTERN(insideRef->pool);
		pool->Free(pool, insideRef);
        
		return true;
	}
	return false;
}

InsideReference* Reference_Acquire(OutsideReference* pRef) {
    while(true) {
        OutsideReference curRef = *pRef;
        if(IsOutsideRefCorrupt(curRef)) return nullptr;
        if(curRef.isNull && curRef.pointerClipped) {
            // Reference_AcquireCheckIsMoved should be called if it is possible Reference_AcquireStartMove will be called on these references...
            OnError(4);
        }
        if(!curRef.pointerClipped) {
            return nullptr;
        }
        OutsideReference newRef = curRef;
        newRef.count++;
        if(InterlockedCompareExchange64(
            (LONG64*)pRef,
            newRef.valueForSet,
            curRef.valueForSet
        ) == curRef.valueForSet) {
            InsideReference* ref = (void*)PACKED_POINTER_GET_POINTER(newRef);
            if(IsInsideRefCorrupt(ref)) return nullptr;
            DebugLog("Reference_Acquire (ACQUIRED)", ref);
            return ref;
        }
    }

    // Unreachable
    OnError(9);
    return nullptr;
}



void Reference_Allocate(MemPool* pool, OutsideReference* outRef, void** outPointer, uint64_t size, uint64_t hash) {
    InsideReference* ref = pool->Allocate(pool, size + InsideReferenceSize, hash);
    if(!ref) {
        outRef->valueForSet = 0;
        *outPointer = nullptr;
        return;
    }
    memset(ref, 0, sizeof(InsideReference));
    //printf("allocated ref %p\n", ref);

    //MemLog_Add(size, ref, "alloc inside ref", hash);

	outRef->valueForSet = 0;
    outRef->pointerClipped = (uint64_t)ref;

    // Count of 1, for the OutsideReference
    ref->valueForSet = 1;
    ref->outsideCount = 1;
    ref->pool = pool;
    ref->hash = hash;

    *outPointer = (void*)((byte*)ref + InsideReferenceSize);
}

#ifdef DEBUG
bool IsSingleThreadedTest = false;
#endif

void Reference_ReleaseX(OutsideReference* outsideRef, InsideReference* insideRef, const char* file, uint64_t line) {
    //printf("releasing reference %p\n", insideRef);

    if(!insideRef) {
        // This saves our cleanup code constantly null checking insideRef. Instead it can just unconditional
        //  release it, we we can null check it.
        return;
    }
    DebugLog2("Reference_Release (BEFORE)", insideRef, file, line);

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

            DebugLog2("Reference_Release (releasing outside)", insideRef, file, line);
            
            // Unfortunately, we can't do a InterlockedDecrement64, because... the outside reference might
            //  move, which would cause our decrement to wrap around the number, messing up the pointer field.
            ref.count--;
            if(!XchgOutsideReference(outsideRef, &refOriginal, &ref)) {
                DebugLog2("Reference_Release (releasing outside failedd)", insideRef, file, line);
                continue;
            }

            // If we exchange, then we decremented the outside count, so the reference is freed.
            return;
        }
    }

    // The OutsideReference is dead. Our reference MUST be in the inside reference. References (counts) only
    //  flow from the outside to the inside.
    //  (while new references may be added to the outside reference, and it may be reused with the same pointer,
    //      meaning it is possible to retry to use the outside reference, we should always be able to dereference
    //      from the inside reference, as our reference had to go somewhere if it wasn't in the outside ref...)
    releaseInsideReferenceX(insideRef, file, line);
}

bool Reference_ReplaceOutsideX(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef, const char* file, uint64_t line) {
    OutsideReference testRef = *pOutsideRef;
    if(IsOutsideRefCorrupt(testRef)) return false;
    if(IsInsideRefCorrupt(pInsideRef)) return false;
    if(!newOutsideRef.isNull && newOutsideRef.count != 0) {
        // You can't start with references to the new outside reference
        OnError(3);
        return false;
    }

    DebugLog2("ReplaceOutsideInner", pInsideRef, file, line);

    // First move the references from outside ref to inside ref (causing them to be duplicated for a bit)
    //  (and which if removing from the outside ref fails may require removing from the inside ref)
    // Then try to make the outside ref wiped out

    while(true) {
		OutsideReference outsideRef = *pOutsideRef;
        
        if((void*)PACKED_POINTER_GET_POINTER(outsideRef) != pInsideRef) {
            DebugLog("ReplaceOutsideInner failed, ref different", pInsideRef);
			return false;
        }

        InterlockedExchangeAdd64((LONG64*)&pInsideRef->valueForSet, (LONG64)outsideRef.count);
        IsInsideRefCorrupt(pInsideRef);
        // Now try to remove from outside ref. If we fail... we have to go fix insideRef before we retry again
		if (!XchgOutsideReference(pOutsideRef, &outsideRef, &newOutsideRef)) {           
			// Subtract outsideRefOriginal.count from insideRef, but don't worry about it destructing the inside ref, the caller has a reference to it anyway
            BitFieldSubtract(&pInsideRef->valueForSet, outsideRef.count, 0);

            IsInsideRefCorrupt(pInsideRef);

			continue;
		}

        BitFieldSubtract(&pInsideRef->valueForSet, 1, OUTSIDE_COUNT_BIT_INDEX);

        if (IsInsideRefCorrupt(pInsideRef)) return false;
        //printf("destroying outside reference for %p\n", pInsideRef);
        if (IsInsideRefCorrupt(pInsideRef)) return false;
        
        if(releaseInsideReference(pInsideRef)) {
            // If our reference's own reference was the last reference, that means the caller lied about
            //  having a reference, and bad stuff is going to happen...
            OnError(3);
        }
        return true;
    }
    return false;
}

bool Reference_DestroyOutsideX(OutsideReference* pOutsideRef, InsideReference* pInsideRef, const char* file, uint64_t line) {
    return Reference_ReplaceOutsideX(pOutsideRef, pInsideRef, emptyReference, file, line);
}
bool Reference_DestroyOutsideMakeNull(OutsideReference* pOutsideRef, InsideReference* pInsideRef) {
    return Reference_ReplaceOutside(pOutsideRef, pInsideRef, BASE_NULL);
}
bool Reference_DestroyOutsideMakeNull1(OutsideReference* pOutsideRef, InsideReference* pInsideRef) {
    return Reference_ReplaceOutside(pOutsideRef, pInsideRef, BASE_NULL1);
}

OutsideReference Reference_CreateOutsideReferenceX(InsideReference* pInsideRef, const char* file, uint64_t line) {
    if(IsInsideRefCorrupt(pInsideRef)) return emptyReference;

    DebugLog2("CreateOutsideReference (BEFORE)", pInsideRef, file, line);

    InterlockedIncrement64((LONG64*)&pInsideRef->valueForSet);
    //printf("creating outside reference for %p\n", pInsideRef);
    InterlockedExchangeAdd64((LONG64*)&pInsideRef->valueForSet, OUTSIDE_COUNT_1);
	if (IsInsideRefCorrupt(pInsideRef)) return emptyReference;

    OutsideReference newOutsideRef = { 0 };
    newOutsideRef.count = 0;
    newOutsideRef.pointerClipped = (uint64_t)pInsideRef;

    #ifdef DEBUG_INSIDE_REFERENCES
    pInsideRef->file = file;
    pInsideRef->line = line;
    #endif

    return newOutsideRef;
}