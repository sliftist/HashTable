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

bool Reference_ReplaceOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef);
bool Reference_ReplaceOutsideInnerX(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef, bool freeOriginalOutsideRef, bool onNulls, const char* file, uint64_t line);
#define Reference_ReplaceOutsideInner(pOutsideRef, pInsideRef, newOutsideRef, freeOriginalOutsideRef, onNulls) Reference_ReplaceOutsideInnerX(pOutsideRef, pInsideRef, newOutsideRef, freeOriginalOutsideRef, onNulls, __FILE__, __LINE__)

InsideReference* Reference_AcquireInternal(OutsideReference* pRef, bool redirect, const char* file, uint64_t line);

const OutsideReference emptyReference = { 0 };

static OutsideReference PREV_NULL = BASE_NULL_LAST_CONST;
OutsideReference GetNextNull() {
    OutsideReference value;
    value.valueForSet = InterlockedIncrement64((LONG64*)&PREV_NULL.valueForSet);
    return value;
}


// Anyone with InsideReference*, should either have a count incremented in the reference count,
//  or an OutsideReference that points to that InsideReference*.

typedef struct { union {
    struct {
        uint64_t count: 43;
        #define OUTSIDE_COUNT_BIT_INDEX (43)
        #define OUTSIDE_COUNT_1 (1ull << OUTSIDE_COUNT_BIT_INDEX)
        uint64_t outsideCount: 20;
        uint64_t destructStarted: 1;
    };
    uint64_t countFullValue;
}; } InsideReferenceCountDebug;

typedef struct {
    InsideReferenceCountDebug state;
    const char* operation;
    uint64_t line;
    const char* file;
} DebugLineInfo;




// When InsideReference reaches a count of 0, it should be freed (as this means nothing knows about it,
//  and so if we don't free it now it will leak).
#pragma pack(push, 1)
struct InsideReference {
    union {
        struct {
            uint64_t count: 43;
            #define OUTSIDE_COUNT_BIT_INDEX (43)
            #define OUTSIDE_COUNT_1 (1ull << OUTSIDE_COUNT_BIT_INDEX)
            uint64_t outsideCount: 20;
            uint64_t destructStarted: 1;
        };
        uint64_t countFullValue;
    };

    MemPool* pool;

    // Means, the underlying value should be read from nextRedirectValue
    OutsideReference nextRedirectValue;

    // Used to update nextRedirectValue of all of our nodes, as we can't free any nodes
    //  if a previous node has a nextRedirectValue pointing to them.
    OutsideReference prevRedirectValue;

    uint64_t nextEventIdIndex;
    DebugLineInfo eventIds[EVENT_ID_COUNT];
};
#pragma pack(pop)

void DebugLog(const char* file, uint64_t line, const char* operation, InsideReference* ref) {
    DebugLineInfo info = { 0 };
    info.file = file;
    info.line = line;
    info.operation = operation;
    info.state.countFullValue = ref->countFullValue;
    uint64_t eventIndex = InterlockedIncrement64((LONG64*)&ref->nextEventIdIndex) - 1;
    ref->eventIds[eventIndex % EVENT_ID_COUNT] = info;
}

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

InsideReference* Reference_AcquireInsideNoRedirect(OutsideReference* pRef, const char* file, uint64_t line) {
	InsideReference* ref = Reference_AcquireInternal(pRef, false, file, line);
	if (!ref) return nullptr;
    //printf("acquiring inside reference for %p have %llu\n", ref, ref->count);
	InterlockedIncrement64((LONG64*)&ref->countFullValue);
	Reference_Release(pRef, ref);
    //printf("acquired inside reference for %p have %llu\n", ref, ref->count);
	return ref;
}

InsideReference* Reference_AcquireInside(OutsideReference* pRef) {
    InsideReference* ref = Reference_AcquireInternal(pRef, true, __FILE__, __LINE__);
	if (!ref) return nullptr;
    //printf("acquiring inside reference for %p have %llu\n", ref, ref->count);
	InterlockedIncrement64((LONG64*)&ref->countFullValue);
	Reference_Release(pRef, ref);
    //printf("acquired inside reference for %p have %llu\n", ref, ref->count);
	return ref;
}

#define ASSERT_DECREMENT_SAFE(expr) (expr) != UINT64_MAX ? 0 : breakpoint()

bool releaseInsideReferenceX(InsideReference* insideRef, const char* file, uint64_t line);
#define releaseInsideReference(ref) releaseInsideReferenceX(ref, __FILE__, __LINE__)

void Reference_RedirectReferenceX(
    // Must be acquired first
    // If already redirects, fails and returns false.
    InsideReference* oldRef,
    // Must be allocated normally, and have the value set as desired
    InsideReference* newRef,
    const char* file,
    uint64_t line
) {
    if(IsInsideRefCorrupt(oldRef)) return;
    if(IsInsideRefCorrupt(newRef)) return;


    DebugLog(file, line, "Redirect TO", newRef);
    DebugLog(file, line, "Redirect FROM", oldRef);

    
    //printf("before redirect of %p(%llu) to %p(%llu)\n", oldRef, oldRef->count, newRef, newRef->count);

	#ifdef DEBUG
	if (newRef->prevRedirectValue.valueForSet != 0 && !FAST_CHECK_POINTER(newRef->prevRedirectValue, oldRef)) {
		// Redirect was called with the same of newRef, but different oldRefs... This should never happen.
		OnError(4);
		return;
	}
	#endif

    OutsideReference refToOldRef = Reference_CreateOutsideReferenceX(oldRef, file, line);

    OutsideReference newRefPrevRedirectValue;
    newRefPrevRedirectValue.valueForSet = InterlockedCompareExchange64(
        (LONG64*)&newRef->prevRedirectValue,
        (LONG64)refToOldRef.valueForSet,
        (LONG64)0
    );
    #ifdef DEBUG
    if(newRefPrevRedirectValue.valueForSet != 0 && !FAST_CHECK_POINTER(newRefPrevRedirectValue, oldRef)) {
        //InterlockedIncrement64((LONG64*)&oldRef->countFullValue);
        // Redirect was called with the same of newRef, but different oldRefs... This should never happen.
        OnError(4);
    }
    #endif

    if(newRefPrevRedirectValue.valueForSet != 0) {
        DestroyUniqueOutsideRef(&refToOldRef);
    }
    
    
    
    OutsideReference refToNewRef = Reference_CreateOutsideReferenceX(newRef, file, line);
    
    //printf("redirect added new ref %p(%llu) to %p(%llu)\n", oldRef, oldRef->count, newRef, newRef->count);
    OutsideReference oldRefNextRedirectValue;
    oldRefNextRedirectValue.valueForSet = InterlockedCompareExchange64(
        (LONG64*)&oldRef->nextRedirectValue,
        (LONG64)refToNewRef.valueForSet,
        (LONG64)0
    );

    if(oldRefNextRedirectValue.valueForSet != 0) {
		// The newRef->prevRedirectValue will be freed when our newRef is freed.
		// But we do need to free the ref we added for the failed outside ref set
        DestroyUniqueOutsideRef(&refToNewRef);

		if (IsInsideRefCorrupt(oldRef)) return;
		if (IsInsideRefCorrupt(newRef)) return;

        DebugLog(file, line, "failed redirect FROM", oldRef);

        //printf("failed redirect of %p(%llu) to %p(%llu)\n", oldRef, oldRef->count, newRef, newRef->count);

		return;
    }

	// Update all previous values to point to the new head
    InsideReference* prevToRedirect = Reference_AcquireInsideNoRedirect(&oldRef->prevRedirectValue, file, line);
	while (prevToRedirect) {
		#ifdef DEBUG
		if (IsSingleThreadedTest) {
			// Redirecting old values is only required when there are hanging references on other threads. If we need this with a single thread
			//	it means we are leaking references, which is bad...
			OnError(13);
		}
		#endif

		// Only redirect from oldRef to newRef
        
        OutsideReference refToNewRef = Reference_CreateOutsideReferenceX(newRef, file, line);
        if(!Reference_ReplaceOutside(
            &prevToRedirect->nextRedirectValue,
            newRef,
            refToNewRef
        )) {
            Reference_DestroyOutside(&refToNewRef, newRef);
            releaseInsideReference(prevToRedirect);
            break;
        }

        //printf("update redirect of %p(%llu) to %p(%llu)\n", prevToRedirect, prevToRedirect->count, newRef, newRef->count);        

		InsideReference* prevToRedirectPrev = prevToRedirect;
		prevToRedirect = Reference_AcquireInsideNoRedirect(&prevToRedirectPrev->prevRedirectValue, file, line);
        // Eventually prevToRedirect will be nullptr, and this will release the final prevToRedirect
		releaseInsideReference(prevToRedirectPrev);
	}

    //printf("after redirect of %p(%llu) to %p(%llu)\n", oldRef, oldRef->count, newRef, newRef->count);
}

InsideReference* Reference_FollowRedirectsX(InsideReference* pRef, const char* file, uint64_t line) {
    if(IsInsideRefCorrupt(pRef)) return pRef;
    while(pRef->nextRedirectValue.valueForSet) {
        InsideReference* next = Reference_AcquireInsideNoRedirect(&pRef->nextRedirectValue, file, line);
        if(!next) {
            break;
        }
        if(IsInsideRefCorrupt(pRef)) return pRef;
        if(IsInsideRefCorrupt(next)) return pRef;
        LONG64 decrementResult = InterlockedDecrement64((LONG64*)&pRef->countFullValue);
        ASSERT_DECREMENT_SAFE(decrementResult);
        pRef = next;
    }
    return pRef;
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
bool unsetDestructValue(InsideReference* ref) {
    while(true) {
        uint64_t count = ref->countFullValue;
        if(!(count & (1ull << 63))) {
            return false;
        }
        if(InterlockedCompareExchange64(
            (LONG64*)&ref->countFullValue,
            count & ~(1ull << 63),
            count
        ) == count) {
            return true;
        }
    }
    return false;
}

void DestroyUniqueOutsideRefInner(OutsideReference* ref, OutsideReference nullValue) {
	InsideReference* temp = Reference_AcquireInternal(ref, false, __FILE__, __LINE__);
	if (!temp) return;
    if (HAS_NON_POINTER_PATTERN(temp->pool)) {
        // Tried to double free value...
        breakpoint();
    }
    DebugLog(__FILE__, __LINE__, "DestroyUniqueOutsideRefInner (ACQUIRED)", temp);
    Reference_ReplaceOutside(ref, temp, nullValue);
    DebugLog(__FILE__, __LINE__, "DestroyUniqueOutsideRefInner (REPLACED)", temp);
	Reference_Release(&emptyReference, temp);
}
void DestroyUniqueOutsideRef(OutsideReference* ref) {
    DestroyUniqueOutsideRefInner(ref, emptyReference);
}
bool releaseInsideReferenceX(InsideReference* insideRef, const char* file, uint64_t line) {
    if(IsInsideRefCorrupt(insideRef)) return false;

    //printf("releasing inside reference %p, has %llu refs\n", insideRef, insideRef->count);

    DebugLog(file, line, "releaseInsideReference (BEFORE)", insideRef);

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
            prevNode = Reference_AcquireInsideNoRedirect(&insideRef->prevRedirectValue, file, line);
            if(prevNode) {
                if(!setDestructValue(prevNode)) {
                    unsetDestructValue(prevNode);
                    break;
                }
            }
        }

        InsideReference* nextNode = nullptr;
        {
            // Goes to the start of the list, as nextRedirectValue is kept pointing to the most recent node, not the opposite of prevRedirectValue.
            InsideReference* searchNode = Reference_AcquireInsideNoRedirect(&insideRef->nextRedirectValue, file, line);
            while(searchNode) {
                if(FAST_CHECK_POINTER(searchNode->prevRedirectValue, insideRef)) {
                    nextNode = searchNode;
                    break;
                }
                
                InsideReference* prevSearchNode = searchNode;
                searchNode = Reference_AcquireInsideNoRedirect(&searchNode->prevRedirectValue, file, line);
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
                newPrevRef = Reference_CreateOutsideReference(prevNode);
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
            unsetDestructValue(nextNode);
            releaseInsideReference(nextNode);
        }
        if(prevNode) {
            unsetDestructValue(prevNode);
            releaseInsideReference(prevNode);
        }
        unsetDestructValue(insideRef);
        if(retry) {
            continue;
        }
        break;
    }

    IsInsideRefCorrupt(insideRef);

    DebugLog(file, line, "releaseInsideReference (BEFORE, but after redirect stuff)", insideRef);

	LONG64 decrementResult = InterlockedDecrement64((LONG64*)&insideRef->countFullValue);
    ASSERT_DECREMENT_SAFE(decrementResult);

    // UNSAFE, but I kind of need it for debugging...
    DebugLog(file, line, "releaseInsideReference (AFTER)", insideRef);

    //printf("released inside reference %p, has %llu refs\n", insideRef, insideRef->count);
    // This is dangerous, but... I really want to know who is releasing the outside reference prematurely...
    // decrementResult
    InsideReferenceCountDebug countDebug;
    countDebug.countFullValue = decrementResult;
    if(countDebug.outsideCount > countDebug.count && countDebug.outsideCount < 1024) {
        // A big problem, you released a reference you obtained for an outside reference without that outside reference,
        //  which leaks a reference and will cause leaks or double frees.
        OnError(3);
    }
    
    // 0 means count is 0, outsideCount is 0 and destructStarted is not set.
	if (decrementResult == 0) {
        //printf("freeing reference memory %p\n", insideRef);
        if(IsInsideRefCorruptInner(insideRef, true)) {
            return true;
        }
		if (HAS_NON_POINTER_PATTERN(insideRef->pool)) {
			// Double free
			breakpoint();
		}
		DestroyUniqueOutsideRefInner(&insideRef->nextRedirectValue, BASE_NULL);
        DestroyUniqueOutsideRefInner(&insideRef->prevRedirectValue, BASE_NULL);
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

InsideReference* referenceAcquire(OutsideReference* pRef, const char* file, uint64_t line) {
    while(true) {
        OutsideReference curRef = *pRef;
        if(IsOutsideRefCorrupt(curRef)) return nullptr;
        if(curRef.isNull || curRef.valueForSet == 0) {
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
            DebugLog(file, line, "referenceAcquire (ACQUIRED)", ref);
            return ref;
        }
    }

    // Unreachable
    OnError(9);
    return nullptr;
}

InsideReference* Reference_AcquireInternal(OutsideReference* pRef, bool redirect, const char* file, uint64_t line) {
    // TODO: Adding unsafely doesn't work anymore, because it might mess up null pointers. But there is still
    //  enough order to get this to work, so... we should...

    InsideReference* ref = referenceAcquire(pRef, file, line);
    if(!ref) {
        return nullptr;
    }

    DebugLog(file, line, "Reference_Acquire (ACQUIRED)", ref);

    //todonext
    // Actually... because Reference_ReduceToZeroRefsAndSetIsNull, we don't need to follow redirects.
    //  So redirects are really just links to prevent moved references from being released, AND extra work
    //  to make sure just because an old reference doesn't get freed, doesn't mean every reference in that
    //  chain leaks, so we can free all references except the original leaked one.
    // And, change Reference_HasBeenRedirected to just check for links instead, as any reference
    //  that has a link is either moved from the original location, or in the process of moving.
    //todonext
    // Oh, and really it is always just linking a new reference to not destruct until an old reference
    //  destructs. So... it should be something like:
    // Reference_WaitForOldToDestruct
    // Reference_SharedWithOld
    // Reference_DependsOnOld
    // Reference_PointersMovedToNewRef
    // Reference_ExternalLinkPointers

    /*
    if(redirect && ref->nextRedirectValue.valueForSet) {
        // This shouldn't be called anymore?
        //OnError(3);
        InsideReference* newRef = referenceAcquire(&ref->nextRedirectValue, file, line);
        OutsideReference newOutsideRef = Reference_CreateOutsideReferenceX(newRef, file, line);
        // Replacing pRef is required, otherwise they can't acquire the reference and destroy the outside source,
        //  as once they acquire it the returned inside reference will no longer match up with the outside reference!
        if(!Reference_ReplaceOutside(pRef, ref, newOutsideRef)) {
            // Must mean pRef has changed what it is pointing to, so try again with whatever it is pointing to now
            Reference_DestroyOutside(&newOutsideRef, newRef);
            Reference_Release(&ref->nextRedirectValue, newRef);
			// So pRef is valid or moved
			Reference_Release(pRef, ref);
		}
		else {
			Reference_Release(&ref->nextRedirectValue, newRef);
			// We know pRef is moved, so we have to force an inside release.
			Reference_Release(&emptyReference, ref);
		}
        // And now that we updated pRef, try again, with the new deeper value.
        return referenceAcquire(pRef, file, line);
    }
    */

    //printf("acquired reference A %p, for %s:%llu, have %llu\n", ref, file, line, ref->count);
    return ref;
}

InsideReference* Reference_AcquireX(OutsideReference* pRef, const char* file, uint64_t line) {
    // TODO: Actually, fast ways to acquire a reference don't work anymore, because they might mess up special null pointers...
    return Reference_AcquireInternal(pRef, true, file, line);
    /*
    if(IsOutsideRefCorrupt(*pRef)) return nullptr;

    OutsideReference refForSet;
    
	//InterlockedAdd64 is just InterlockedExchangeAdd64 anyway, so it is faster to use InterlockedExchangeAdd64
	//	and then do the add only if we have to (which shouldn't be on the hot path anyway).
    refForSet.valueForSet = InterlockedExchangeAdd64((LONG64*)pRef, 1ll << COUNT_OFFSET_BITS);

    InsideReference* ref = (void*)PACKED_POINTER_GET_POINTER(refForSet);
    if(ref) {
		if (IsInsideRefCorrupt(ref)) return nullptr;
        if(!ref->nextRedirectValue.valueForSet) {
            return ref;
        }
        Reference_Release(pRef, ref);
        return Reference_AcquireInternal(pRef, true);
    }

    // Having zero refs doesn't mean being 0, because of nulls, so... we set the ref count to zero explicitly...
    OutsideReference zeroRefs = refForSet;
    zeroRefs.count = 0;

    // If this fails either we were wiped out and gained a value, or another acquire run, in which case it will
    //  perform this anyway, and we will eventually be reset by some thread (probably).
    InterlockedCompareExchange64(
        (LONG64*)pRef,
        zeroRefs.valueForSet,
        refForSet.valueForSet + (1ll << COUNT_OFFSET_BITS)
    );

    return nullptr;
    */
}


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
            break;
        }
    }

    DebugLog(file, line, "Reference_AcquireIfNull (ACQUIRED OUTSIDE)", ref);


    // Safe, as we just added an outside ref
    InterlockedIncrement64(&ref->countFullValue);

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
            DebugLog(file, line, "Reference_AcquireIfNull (PUT INSIDE)", ref);
            return ref;
        }
	}

	// If we get here, it means our ref got moved to the inside, so we have to remove it from the inside
    ASSERT_DECREMENT_SAFE(InterlockedDecrement64((LONG64*)&ref->countFullValue));

    DebugLog(file, line, "Reference_AcquireIfNull interrupted (AFTER)", ref);

    IsInsideRefCorrupt(ref);

    return ref;
}
void Reference_DestroyThroughNull(OutsideReference* outsideRef, OutsideReference newNullValue) {
    OutsideReference ref = *outsideRef;
    if(IsOutsideRefCorrupt(ref)) return;
    if(ref.valueForSet == newNullValue.valueForSet) {
        return;
    }
    if(!ref.isNull) {
        return;
    }

    InsideReference* insideRef = Reference_AcquireIfNull(outsideRef);
    if(!insideRef) return;
    DebugLog(__FILE__, __LINE__, "DestroyThroughNull (BEFORE)", insideRef);
    Reference_ReplaceOutsideInner(outsideRef, insideRef, newNullValue, true, true);
    IsInsideRefCorrupt(insideRef);
	Reference_Release(&emptyReference, insideRef);
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

    outRef->valueForSet = 0;
    outRef->pointerClipped = (uint64_t)ref;

    // Count of 1, for the OutsideReference
    ref->countFullValue = 1;
    ref->outsideCount = 1;
    ref->pool = pool;
    ref->nextRedirectValue.valueForSet = 0;
    ref->prevRedirectValue.valueForSet = 0;

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
    DebugLog(file, line, "Reference_Release (BEFORE)", insideRef);

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

            DebugLog(file, line, "Reference_Release (releasing outside)", insideRef);
            
            // Unfortunately, we can't do a InterlockedDecrement64, because... the outside reference might
            //  move, which would cause our decrement to wrap around the number, messing up the pointer field.
            ref.count--;
            if(!XchgOutsideReference(outsideRef, &refOriginal, &ref)) {
                DebugLog(file, line, "Reference_Release (releasing outside failedd)", insideRef);
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

bool Reference_ReplaceOutsideInnerX(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef, bool notSetIsNullAndReduceToZeroRefs, bool onNulls, const char* file, uint64_t line) {
	InsideReference testCopy = *pInsideRef;
    OutsideReference testRef = *pOutsideRef;
    if(onNulls) {
        if(!testRef.isNull) {
            return false;
        }
        testRef.isNull = 0;
    }
    if(IsOutsideRefCorrupt(testRef)) return false;
    if(IsInsideRefCorrupt(pInsideRef)) return false;
    if(!newOutsideRef.isNull && newOutsideRef.count != 0) {
        // You can't start with references to the new outside reference
        OnError(3);
        return false;
    }

    DebugLog(file, line, "ReplaceOutsideInner", pInsideRef);

    // First move the references from outside ref to inside ref (causing them to be duplicated for a bit)
    //  (and which if removing from the outside ref fails may require removing from the inside ref)
    // Then try to make the outside ref wiped out

    while(true) {
		OutsideReference outsideRef = *pOutsideRef;
        if(onNulls) {
            if(!outsideRef.isNull) {
                return false;
            }
            outsideRef.isNull = 0;
        }
        
        if((void*)PACKED_POINTER_GET_POINTER(outsideRef) != pInsideRef) {
            DebugLog(file, line, "ReplaceOutsideInner failed, ref different", pInsideRef);
			return false;
        }
        if(!onNulls && outsideRef.isNull) {
            // Impossible, PACKED_POINTER_GET_POINTER must be broken...
            breakpoint();
        }

        //printf("moving count to reference %p, have %llu\n", pInsideRef, pInsideRef->count);
        InterlockedAdd64((LONG64*)&pInsideRef->countFullValue, (LONG64)outsideRef.count);
        if(pInsideRef->count > (1ull << (64 - BITS_IN_ADDRESS_SPACE - 4))) {
            // Getting close to running out of references...
            OnError(1);
        }

        if(onNulls) {
            outsideRef.isNull = 1;
        }
        if(!notSetIsNullAndReduceToZeroRefs) {
            newOutsideRef = outsideRef;
            newOutsideRef.isNull = 1;
            newOutsideRef.count = 0;
        }

        uint64_t eventIndex = 0;

        if(notSetIsNullAndReduceToZeroRefs) {
            DebugLog(file, line, "Premature remove outside ref count", pInsideRef);

            // We have to remove outside count first, or else it will look like the outside ref is alive when it isn't
            //  This leaves a race where it could appear our outside ref is gone when it isn't, but the count is just for debugging anyway...
            BitFieldSubtract(&pInsideRef->countFullValue, 1, OUTSIDE_COUNT_BIT_INDEX);
			IsInsideRefCorrupt(pInsideRef);
        }

        // Now try to remove from outside ref. If we fail... we have to go fix insideRef before we retry again
		if (!XchgOutsideReference(pOutsideRef, &outsideRef, &newOutsideRef)) {
			if (notSetIsNullAndReduceToZeroRefs) {
                DebugLog(file, line, "Undo premature remove outside ref count", pInsideRef);

                InterlockedExchangeAdd64((LONG64*)&pInsideRef->countFullValue, OUTSIDE_COUNT_1);
			}
            
			// Subtract outsideRefOriginal.count from insideRef, but don't worry about it destructing the inside ref, the caller has a reference to it anyway
            BitFieldSubtract(&pInsideRef->countFullValue, outsideRef.count, 0);
            //printf("undid move to count to reference %p, have %llu\n", pInsideRef, pInsideRef->count);

            IsInsideRefCorrupt(pInsideRef);

			continue;
		}

        //printf("done move to count to reference %p, have %llu\n", pInsideRef, pInsideRef->count);

        // Now that the outside ref is atomically gone, we can remove the outside's own ref to the inside ref
        //  (this is NOT the reference the caller has, this is the outside reference's own ref, completely different)
        if(notSetIsNullAndReduceToZeroRefs) {
            DebugLog(file, line, "Destroying outside reference", pInsideRef);

			if (IsInsideRefCorrupt(pInsideRef)) return true;
            //printf("destroying outside reference for %p\n", pInsideRef);
			if (IsInsideRefCorrupt(pInsideRef)) return true;
            
            if(releaseInsideReferenceX(pInsideRef, file, line)) {
                // If our reference's own reference was the last reference, that means the caller lied about
                //  having a reference, and bad stuff is going to happen...
                OnError(3);
            }
        } else {
            DebugLog(file, line, "Freezing outside reference", pInsideRef);
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

bool Reference_ReduceToZeroRefsAndSetIsNull(OutsideReference* pOutsideRef, InsideReference* pInsideRef) {
    return Reference_ReplaceOutsideInner(pOutsideRef, pInsideRef, emptyReference, false, false);
}

bool Reference_DestroyOutside(OutsideReference* pOutsideRef, InsideReference* pInsideRef) {
    //return Reference_ReplaceOutside(pOutsideRef, pInsideRef, emptyReference);
    return Reference_ReplaceOutsideInner(pOutsideRef, pInsideRef, emptyReference, true, false);
}
bool Reference_DestroyOutsideMakeNull(OutsideReference* pOutsideRef, InsideReference* pInsideRef) {
    return Reference_ReplaceOutside(pOutsideRef, pInsideRef, BASE_NULL);
}

OutsideReference Reference_CreateOutsideReferenceX(InsideReference* pInsideRef, const char* file, uint64_t line) {
    if(IsInsideRefCorrupt(pInsideRef)) return emptyReference;

    DebugLog(file, line, "CreateOutsideReference (BEFORE)", pInsideRef);

    InterlockedIncrement64((LONG64*)&pInsideRef->countFullValue);
    //printf("creating outside reference for %p\n", pInsideRef);
    InterlockedExchangeAdd64((LONG64*)&pInsideRef->countFullValue, OUTSIDE_COUNT_1);
	if (IsInsideRefCorrupt(pInsideRef)) return emptyReference;

    OutsideReference newOutsideRef = { 0 };
    newOutsideRef.count = 0;
    newOutsideRef.pointerClipped = (uint64_t)pInsideRef;

    return newOutsideRef;
}