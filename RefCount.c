#include "RefCount.h"
#include "AtomicHelpers.h"
#include "MemPool.h"
#include "MemLog.h"

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

uint64_t first = false;
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
		if (InterlockedCompareExchange64(
			(LONG64*)&first,
			true,
			false
		) == false) {
			MemLog_SaveValuesIndex("./logs.txt", (uint64_t)pRef);
			breakpoint();
		}
        // This means there are outstanding outside references with references that have been orphaned. This is bad...
        //OnError(3);
        //return true;
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
    if(ref.outsideCount > 200) {
        // Probably corruption
        if (pRef->pool != (void*)&memPoolSystem) {
			MemLog_SaveValuesIndex("./logs2.txt", (uint64_t)pRef);
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

void Reference_Mark(InsideReference* ref) {
    IsInsideRefCorrupt(ref);
    while(true) {
        InsideReferenceCount count;
        count.valueForSet = ref->valueForSet;
        InsideReferenceCount newCount = count;
        newCount.isMarked = 1;
        if(InterlockedCompareExchange64(
            (LONG64*)&ref->valueForSet,
            newCount.valueForSet,
            count.valueForSet
        ) == count.valueForSet) {
            break;
        }
    }
}
bool Reference_IsMarked(InsideReference* ref) {
    IsInsideRefCorruptInner(ref, true);
    return !!ref->isMarked;
}

bool Reference_MarkIfUniqueIdEqual(InsideReference* ref, uint64_t uniqueId) {
    //*
    //todonext
    // TODO: Wait, we can definitely get rid of the 128 bit state. If we add a reference (which we can definitely do),
    //  then moveId won't change, so we can read it first. And if we mess up destruction by adding the ref,
    //  it will just trigger again when we free the reference, so that is fine too...
    //  - And because the caller knows the ref won't move again, as it calls this after a remove was successfull...
    //      we don't even have to worry about moveId being oldRef, and then changing to something else (and having that
    //      match, but have us miss it, or whatever)...

    // Test moveId before we try to get a reference, as an optimization
    MoveId firstTest = ref->moveId;
    bool isMatch = firstTest.isNull && firstTest.hasUniqueValueId && firstTest.uniqueValueId == uniqueId;
    if(!isMatch) {
        return false;
    }

    // Get a reference, returning false if it has a count of 0
    while(true) {
        InsideReferenceCount count;
        count.valueForSet = ref->valueForSet;
        if(count.count == 0) {
            return false;
        }
        InsideReferenceCount newCount = count;
        newCount.count++;
        // Make it an outside reference, as we use outsideCount as a flag to indicate we won't dereference any pointers inside of it.
        newCount.outsideCount++;
        if(InterlockedCompareExchange64(
            (LONG64*)&ref->valueForSet,
            newCount.valueForSet,
            count.valueForSet
        ) == count.valueForSet) {
            MemLog_Add((void*)0, ref, "acquired for MarkIfUniqueIdEqual", ref->valueForSet);
            break;
        }
    }

    MoveId test = ref->moveId;
    isMatch = test.isNull && test.hasUniqueValueId && test.uniqueValueId == uniqueId;
    if(isMatch) {
        // Set it as marked
        while(true) {
            InsideReferenceCount count;
            count.valueForSet = ref->valueForSet;
            if(count.count == 0) {
                // Impossible, we added a reference
                OnError(3);
                return false;
            }
            InsideReferenceCount newCount = count;
            newCount.isMarked = 1;
            if(InterlockedCompareExchange64(
                (LONG64*)&ref->valueForSet,
                newCount.valueForSet,
                count.valueForSet
            ) == count.valueForSet) {
                break;
            }
        }
    }

    Reference_ReleaseOutsidesInside(ref);

    return isMatch;
    //*/


    // Now that we have a reference, moveId will be frozen

    /*
    InsideReferenceStart* pRef = (void*)refFull;
    while(true) {
        InsideReferenceStart ref = *pRef;
        if(!EqualsStruct128(&ref, pRef)) continue;
        if(ref.count.count == 0 || !(ref.moveId.isNull && ref.moveId.hasUniqueValueId) || ref.moveId.uniqueValueId != uniqueId) {
            return false;
        }
        if(ref.count.isMarked) {
            // Tries to move to self?
            OnError(9);
        }
        InsideReferenceStart newRef = ref;
        newRef.count.isMarked = 1;
        if(InterlockedCompareExchangeStruct128(
            pRef,
            &ref,
            &newRef
        )) {
            return true;
        }
    }
    //*/
}
uint64_t Reference_GetUniqueValueId(InsideReference* ref) {
    MoveId moveId = ref->moveId;
    return moveId.isNull && moveId.hasUniqueValueId ? moveId.uniqueValueId : 0;
}

InsideReference* Reference_unsafeClone(InsideReference* ref) {
    InterlockedIncrement64(&ref->valueForSet);
    return ref;
}

bool Reference_HasBeenMoved(InsideReference* ref) {
    MoveId id = ref->moveId;
    return id.isNull && id.isCommittedToMove || !id.isNull && id.valueForSet;
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

void referenceAddOutsideRef(InsideReference* ref) {
    while(true) {
        InsideReferenceCount count;
        count.valueForSet = ref->valueForSet;
        InsideReferenceCount newCount = count;
        newCount.count++;
        newCount.outsideCount++;
        if(InterlockedCompareExchange64(
            (LONG64*)&ref->valueForSet,
            newCount.valueForSet,
            count.valueForSet
        ) == count.valueForSet) {
            break;
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
    referenceAddOutsideRef(ref);

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

bool referenceCheckAndRunDtorAfterDecrement(InsideReferenceCount count, InsideReference* insideRef) {
    if (count.count == 0) {
        //printf("freeing reference memory %p\n", insideRef);
        if(IsInsideRefCorruptInner(insideRef, true)) {
            return false;
        }
		if (HAS_NON_POINTER_PATTERN(insideRef->pool)) {
			// Double free
			breakpoint();
		}

        MemLog_Add((void*)0, insideRef, "freeing inside ref", insideRef->moveId.uniqueValueId);

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

bool Reference_ReleaseOutsidesInside(InsideReference* ref) {
    while(true) {
        IsInsideRefCorrupt(ref);
        InsideReferenceCount count;
        count.valueForSet = ref->valueForSet;
        InsideReferenceCount newCount = count;

        if(newCount.valueForSet != count.valueForSet) {
            breakpoint();
        }

        if(newCount.outsideCount == 0) {
			MemLog_SaveValuesIndex("./logs2.txt", (uint64_t)ref);
            // Not enough outside references to remove
            OnError(3);
            return false;
        }

        newCount.count--;
        newCount.outsideCount--;
        
        if(InterlockedCompareExchange64(
            (LONG64*)&ref->valueForSet,
            newCount.valueForSet,
            count.valueForSet
        ) == count.valueForSet) {
            return referenceCheckAndRunDtorAfterDecrement(newCount, ref);
        }
    }
    // Unreachable
    OnError(9);
    return false;
}



uint64_t nextUniqueValueId = 1;

// Cleans up newRef's moveId, checking if oldRef has outstanding references, as at this point it will have been frozen,
//  so it won't be able to gain any more outstanding references (also, all references will be moved to the inside,
//  so we will be able to compare count and outsideCount).
// - Frees oldRef, also oldRef can be null, and it will just acquire a new one
void afterConfirmedMove(InsideReference* newRef,
    InsideReference* oldRef
    #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
    ,uint64_t* pOutstandingRefsDuringMove
    ,uint64_t* pNotOutstandingRefsDuringMove
    #endif
) {
    // If we previous had a unique id, or have already finished the move, isNull will be set
    if(newRef->moveId.isNull) {
        if(oldRef) {
            Reference_ReleaseOutsidesInside(oldRef);
        }
        return;
    }

    if(!oldRef) {
        bool nothing1;
        bool nothing2;
        oldRef = Reference_AcquireCheckIsMoved(&newRef->moveId.tempOldRef, &nothing1, &nothing2);
        if(!oldRef) {
            // Must have already been destroyed
            return;
        }
        // Make it both inside, and appear as an outside reference
        referenceAddOutsideRef(oldRef);
        Reference_Release(&newRef->moveId.tempOldRef, oldRef);
    }


    // We have no previous unique id at this point, so if we have no outstanding count here, we just leave uniqueValueId to 0...
    MoveId newMoveId = { 0 };
    newMoveId.isNull = 1;
    
    InsideReferenceCount count;
    count.valueForSet = oldRef->valueForSet;
    if(count.count > count.outsideCount) {
        //todonext
        // Okay, starting from here, we need to trace exactly where the unique value goes.
        //  So... add the uniqueId to this log, and then using our inside reference trace
        //  when our value moves, and verify the uniqueId is still there... and if it is, then it will probably
        //  continue to be there.
        // And we should be able to trace it with the uniqueId through all the different inside references until
        //  the place it frees, and then call free on the pItem->item we first complained about. Once we can
        //  connect this all the way to that, the bug will be obvious...

        #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
        InterlockedIncrement64((LONG64*)pOutstandingRefsDuringMove);
        #endif

         
        
        // TODO: Oh... we can initialize the uniqueValueId in Reference_Allocate, as the initial see will never have to use it
        //  to store the "oldRef", so... there's that. And it is really simpler if we initialize it there, and it also gives us a nice unique id we
        //  can use to trace a value through movement through multiple inside references.
        //  - Actually... only do this if we encounter more issues and want to simplify things to debug it. For now, this is a lot faster for certain use cases
        //      (it doing it here gets rid of an InterlockedIncrement64 that would be needed in every allocate).

        // Make sure the old ref has a uniqueValueId.
        //  We can do this this late, as the old value won't be gone until at least we call Reference_ReplaceOutside on tempOldRef.
        while(true) {
            MoveId oldMoveId = oldRef->moveId;
            if(!oldMoveId.isNull) {
                // Should be impossible, if we are moving out of it, hasn't it long been moved into?
                OnError(3);
            }
            if(oldMoveId.hasUniqueValueId) break;
            MoveId oldMoveIdNew = oldMoveId;
            oldMoveIdNew.hasUniqueValueId = 1;
            oldMoveIdNew.uniqueValueId = InterlockedIncrement64(&nextUniqueValueId);
            InterlockedCompareExchange64(
                (LONG64*)&oldRef->moveId.valueForSet,
                oldMoveIdNew.valueForSet,
                oldMoveId.valueForSet
            );
        }

        newMoveId.hasUniqueValueId = 1;
        newMoveId.uniqueValueId = oldRef->moveId.uniqueValueId;

        MemLog_Add((void*)0, (uint64_t)oldRef, "trailing ref", oldRef->hash);
        MemLog_Add2((void*)0, (uint64_t)oldRef, "trailing ref unique id new ref + uniqueValueId", newRef, newMoveId.uniqueValueId);
        MemLog_Add((void*)0, (uint64_t)oldRef, "trailing ref unique id pool", oldRef->pool);

    } else {
		MemLog_Add((void*)0, (uint64_t)oldRef, "no trail ref", newMoveId.uniqueValueId);

        #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
        InterlockedIncrement64((LONG64*)pNotOutstandingRefsDuringMove);
        #endif
    }


    if(!Reference_ReplaceOutside(
        &newRef->moveId.tempOldRef,
        oldRef,
        newMoveId.tempOldRef
    )) {
        MemLog_Add((void*)0, (uint64_t)oldRef, "was not first unique id replace", newMoveId.uniqueValueId);
    } else {
        MemLog_Add((void*)0, (uint64_t)oldRef, "was first unique id replace", newMoveId.uniqueValueId);
    }


    Reference_ReleaseOutsidesInside(oldRef);
}

int Reference_AcquireStartMove(
    OutsideReference* pRef,
    MemPool* newPool,
    uint64_t size,
    InsideReference** outNewRef
#ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
,uint64_t* pOutstandingRefsDuringMove
,uint64_t* pNotOutstandingRefsDuringMove
#endif
) {
    OutsideReference outRef = *pRef;
    if(outRef.valueForSet == 0) {
        if(InterlockedCompareExchange64(
            (LONG64*)pRef,
            BASE_NULL2.valueForSet,
            0
        ) != outRef.valueForSet) {
            // Changed since we first read it
            return Reference_AcquireStartMove(pRef, newPool, size, outNewRef
            #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
            , pOutstandingRefsDuringMove
            , pNotOutstandingRefsDuringMove
            #endif
            );
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
            return Reference_AcquireStartMove(pRef, newPool, size, outNewRef
            #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
            , pOutstandingRefsDuringMove
            , pNotOutstandingRefsDuringMove
            #endif
            );
        }
        return 0;
    }

    if(outRef.isNull && outRef.pointerClipped) {
        // Already frozen and copied, so acquire the reference, and then return
        InsideReference* newRef = Reference_AcquireIfNull(pRef);
		// (and of course, it might have completely finished moving, in which case just return null).
		if(newRef) {
			afterConfirmedMove(newRef,
				nullptr
				#ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
				, pOutstandingRefsDuringMove
				, pNotOutstandingRefsDuringMove
				#endif
			);
		}
        *outNewRef = newRef;
        DebugLog("Reference_AcquireStartMove already moved", *outNewRef);
        return 0;
    }

    if(outRef.valueForSet == BASE_NULL2.valueForSet || outRef.valueForSet == BASE_NULL1.valueForSet) {
        DebugLog("Reference_AcquireStartMove already finished moving", *outNewRef);
        return 0;
    }

	
	bool isMoved = false;
	bool isFrozen = false;
    // oldRef comes out as an inside reference
    InsideReference* oldRef = Reference_AcquireCheckIsMoved(pRef, &isMoved, &isFrozen);
    if(!oldRef) {
        // Changed since we first read it
        return Reference_AcquireStartMove(pRef, newPool, size, outNewRef
        #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
        , pOutstandingRefsDuringMove
        , pNotOutstandingRefsDuringMove
        #endif
        );
    }
    // Make oldRef an outside ref
    referenceAddOutsideRef(oldRef);
    Reference_Release(pRef, oldRef);


    OutsideReference newOutRef;
    byte* newValue = nullptr;
    Reference_Allocate(newPool, &newOutRef, &newValue, size, oldRef->hash);
    if(!newValue) {
        Reference_ReleaseOutsidesInside(oldRef);
        return 3;
    }
    byte* oldValue = Reference_GetValueFast(oldRef);
    memcpy(newValue, oldValue, size);


    // Makes an outside reference for it, kind of cheating because we know newRef is still private
    InsideReference* newRef = (void*)PACKED_POINTER_GET_POINTER(newOutRef);
    referenceAddOutsideRef(newRef);

    
    #ifdef DEBUG_INSIDE_REFERENCES
    newRef->unsafeSource = oldRef;
    #endif

    

    // At this point oldRef->moveId will definitely no longer be a tempOldref, as afterConfirmedMove will have been called on every ref that has been moved.
    if(oldRef->moveId.hasUniqueValueId) {
        newRef->moveId = oldRef->moveId;
        // Don't inherit isCommittedToMove...
        newRef->moveId.isCommittedToMove = 0;
        MemLog_Add((void*)0, oldRef, "gave inherited trailing ref", newRef->moveId.uniqueValueId);
        MemLog_Add((void*)0, newRef, "inherited trailing ref", newRef->moveId.uniqueValueId);
    } else {
        newRef->moveId.tempOldRef = Reference_CreateOutsideReference(oldRef);
        MemLog_Add((void*)0, oldRef, "gave did not inherit trailing ref", newRef->moveId.uniqueValueId);
        MemLog_Add((void*)0, newRef, "did not inherit trailing ref", newRef->moveId.uniqueValueId);
    }
    while(true) {
        MoveId id = oldRef->moveId;
        MoveId newId = id;
        newId.isCommittedToMove = 1;
        // In case it was never moved before, its moveId would be 0.
        newId.isNull = 1;
        if(InterlockedCompareExchange64(
            (LONG64*)&oldRef->moveId,
            newId.valueForSet,
            id.valueForSet
        ) == id.valueForSet) {
            break;
        }
    }

    newOutRef.isNull = 1;
    if(!Reference_ReplaceOutside(
        pRef,
        oldRef,
        newOutRef
    )) {

        newOutRef.isNull = 0;
        
        Reference_DestroyOutside(&newRef->moveId.tempOldRef, oldRef);
        Reference_ReleaseOutsidesInside(oldRef);

        if(!Reference_DestroyOutside(&newOutRef, newRef)) {
            // Impossible
            OnError(3);
        }

		Reference_ReleaseOutsidesInside(newRef);

        // Retry, we should get the new allocation right away this time.
        return Reference_AcquireStartMove(pRef, newPool, size, outNewRef
        #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
        , pOutstandingRefsDuringMove
        , pNotOutstandingRefsDuringMove
        #endif
        );
    }

    MemLog_Add2((void*)0, (uint64_t)oldRef, "after froze in table", oldRef->moveId.uniqueValueId, newRef);


    afterConfirmedMove(newRef,
        oldRef
        #ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
        , pOutstandingRefsDuringMove
        , pNotOutstandingRefsDuringMove
        #endif
    );

    *outNewRef = newRef;
    return 0;
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
            Reference_ReleaseOutsidesInside(insideRef);
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

    MemLog_Add((void*)0, insideRef, "releaseInsideReference (BEFORE)", insideRef->valueForSet);

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
    
    return referenceCheckAndRunDtorAfterDecrement(count, insideRef);
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
    if((uint64_t)ref % 8 != 0) {
        // Pool must 8 byte aligned
        OnError(3);
    }
    if(!ref) {
        outRef->valueForSet = 0;
        *outPointer = nullptr;
        return;
    }
    memset(ref, 0, sizeof(InsideReference));

    MemLog_Add((void*)0, ref, "alloc inside ref", hash);

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

            IsInsideRefCorrupt(insideRef);

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

            //MemLog_Add((void*)0, pInsideRef, "oldRefBefore5 failed", pInsideRef->valueForSet);

			// Subtract outsideRefOriginal.count from insideRef, but don't worry about it destructing the inside ref, the caller has a reference to it anyway
            BitFieldSubtract(&pInsideRef->valueForSet, outsideRef.count, 0);

            IsInsideRefCorrupt(pInsideRef);

			continue;
		}

        if (IsInsideRefCorrupt(pInsideRef)) return false;
        //printf("destroying outside reference for %p\n", pInsideRef);
        if (IsInsideRefCorrupt(pInsideRef)) return false;

        //MemLog_Add((void*)0, pInsideRef, "oldRefBefore5 success", pInsideRef->valueForSet);
        
        if(Reference_ReleaseOutsidesInside(pInsideRef)) {
            // If our reference's own reference was the last reference, that means the caller lied about
            //  having a reference, and bad stuff is going to happen...
            OnError(3);
        }

        //MemLog_Add((void*)0, pInsideRef, "oldRefBefore5 done", pInsideRef->valueForSet);
        // We can't use pInsideRef after releaseInsideReference, as... sometimes the caller doesn't own pInsideRef, in certain cases... so...
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

    referenceAddOutsideRef(pInsideRef);

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