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
bool Reference_ReplaceOutsideInner(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef, bool freeOriginalOutsideRef, bool onNulls);

const OutsideReference emptyReference = { 0 };



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




#ifdef EVENT_ID_COUNT
void DebugLog(const char* file, uint64_t line, const char* operation, InsideReference* ref) {
    DebugLineInfo info = { 0 };
    info.file = file;
    info.line = line;
    info.operation = operation;
    info.state.countFullValue = ref->countFullValue;
    uint64_t eventIndex = InterlockedIncrement64((LONG64*)&ref->nextEventIdIndex) - 1;
    ref->eventIds[eventIndex % EVENT_ID_COUNT] = info;
}
#else
#define DebugLog(file, line, operation, ref) false
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

InsideReference* Reference_AcquireInside(OutsideReference* pRef) {
    InsideReference* ref = Reference_Acquire(pRef);
	if (!ref) return nullptr;
    //printf("acquiring inside reference for %p have %llu\n", ref, ref->count);
	InterlockedIncrement64((LONG64*)&ref->countFullValue);
	Reference_Release(pRef, ref);
    //printf("acquired inside reference for %p have %llu\n", ref, ref->count);
	return ref;
}

bool releaseInsideReference(InsideReference* insideRef);

bool Reference_RedirectReference(
    // Must be acquired first
    // If already redirects, fails and returns false.
    InsideReference* oldRef,
    // Must be allocated normally, and have the value set as desired
    InsideReference* newRef
) {
    if(IsInsideRefCorrupt(oldRef)) return false;
    if(IsInsideRefCorrupt(newRef)) return false;


    DebugLog(file, line, "Redirect TO", newRef);
    DebugLog(file, line, "Redirect FROM", oldRef);

    
    //printf("before redirect of %p(%llu) to %p(%llu)\n", oldRef, oldRef->count, newRef, newRef->count);

	#ifdef DEBUG
	if (newRef->prevRedirectValue.valueForSet != 0 && !FAST_CHECK_POINTER(newRef->prevRedirectValue, oldRef)) {
		// Redirect was called with the same of newRef, but different oldRefs... This should never happen.
		OnError(4);
		return false;
	}
	#endif

    //todonext
    // We're leaking this? But... HOW! And we're leaking it in the case the redirection fails!?
    OutsideReference refToOldRef = Reference_CreateOutsideReference(oldRef);

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
        // NewRef should be uniquely held by the caller...
        OnError(3);
    }
    
    
    
    OutsideReference refToNewRef = Reference_CreateOutsideReference(newRef);
    
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
        // newRef is unique to this thread, so this is safe
        newRef->prevRedirectValue.valueForSet = 0;
        DestroyUniqueOutsideRef(&refToOldRef);

        DestroyUniqueOutsideRef(&refToNewRef);

		if (IsInsideRefCorrupt(oldRef)) return false;
		if (IsInsideRefCorrupt(newRef)) return false;

        DebugLog(file, line, "failed redirect FROM", oldRef);

        //printf("failed redirect of %p(%llu) to %p(%llu)\n", oldRef, oldRef->count, newRef, newRef->count);

		return false;
    }

	// Update all previous values to point to the new head
    InsideReference* prevToRedirect = Reference_AcquireInside(&oldRef->prevRedirectValue);
	while (prevToRedirect) {
		#ifdef DEBUG
		if (IsSingleThreadedTest) {
			// Redirecting old values is only required when there are hanging references on other threads. If we need this with a single thread
			//	it means we are leaking references, which is bad...
			OnError(13);
		}
		#endif

		// Only redirect from oldRef to newRef
        
        OutsideReference refToNewRef = Reference_CreateOutsideReference(newRef);
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
		prevToRedirect = Reference_AcquireInside(&prevToRedirect->prevRedirectValue);
        // Eventually prevToRedirect will be nullptr, and this will release the final prevToRedirect
		releaseInsideReference(prevToRedirectPrev);
	}

    //printf("after redirect of %p(%llu) to %p(%llu)\n", oldRef, oldRef->count, newRef, newRef->count);

    return true;
}

InsideReference* Reference_FollowRedirects(InsideReference* pRef) {
    if(!pRef) return nullptr;
    if(IsInsideRefCorrupt(pRef)) return pRef;
    while(pRef->nextRedirectValue.valueForSet) {
        InsideReference* next = Reference_AcquireInside(&pRef->nextRedirectValue);
        if(!next) {
            break;
        }
        if(IsInsideRefCorrupt(pRef)) return pRef;
        if(IsInsideRefCorrupt(next)) return pRef;
        releaseInsideReference(pRef);
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
	InsideReference* temp = Reference_Acquire(ref);
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

// On success leaves destructStarted
void detachFromLinkList(InsideReference* insideRef) {
    //todonext
    // The goal is to make the prevRedirectValue no longer point to us.
    //  A nextRedirectValue MAY point to us. If we have our own nextRedirectValue it may not, but it is possible,
    //  because redirect may hang after setting up the redirect, but before setting all nextRedirectValue values...
    //  (new redirects will fix this though, so it isn't that big a deal)


    if(!setDestructValue(insideRef)) {
        printf("abort detach of %p\n", insideRef);
        return;
    }
    
    // We find our next node and swap it from pointing to us, to pointing to our previous node.
    //  If the next removed itself we have nothing to point to, so that can't happen.
    //  And, what if the previous node removed itself, then we need to change the new prev node.

    InsideReference* prevNode = nullptr;
    {
        prevNode = Reference_AcquireInside(&insideRef->prevRedirectValue);
        if(prevNode) {
            if(!setDestructValue(prevNode)) {
                printf("abort detach of %p due to prev already destructing\n", insideRef);
                unsetDestructValue(insideRef);
                return;
            }
        }
    }

    InsideReference* nextNode = nullptr;
    {
        // Get the actual next most node, which may require following a few nodes, as we always want to point to the next most node
        InsideReference* searchNode = Reference_FollowRedirects(Reference_AcquireInside(&insideRef->nextRedirectValue));
        while(searchNode) {
            if(FAST_CHECK_POINTER(searchNode->prevRedirectValue, insideRef)) {
                nextNode = searchNode;
                if(!setDestructValue(nextNode)) {
                    printf("abort detach of %p due to next already destructing\n", insideRef);
                    unsetDestructValue(insideRef);
                    if(prevNode) unsetDestructValue(prevNode);
                    return;
                }
                break;
            }
            
            InsideReference* prevSearchNode = searchNode;
            searchNode = Reference_AcquireInside(&searchNode->prevRedirectValue);
            releaseInsideReference(prevSearchNode);
        }
    }

    if(!FAST_CHECK_POINTER(nextNode->prevRedirectValue, insideRef)
    || !FAST_CHECK_POINTER(insideRef->prevRedirectValue, prevNode)) {
        printf("abort detach of %p due to state change \n", insideRef);
        unsetDestructValue(insideRef);
        if(prevNode) unsetDestructValue(prevNode);
        unsetDestructValue(nextNode);
        return;
    }

    OutsideReference newPrevRef = { 0 };
    if(prevNode) {
        newPrevRef = Reference_CreateOutsideReference(prevNode);
    }
    if(!Reference_ReplaceOutside(
        &nextNode->prevRedirectValue,
        insideRef,
        newPrevRef
    )) {
        printf("abort detach of %p due to nextNode->prevRedirectValue change \n", insideRef);
        if(prevNode) {
            Reference_DestroyOutside(&newPrevRef, prevNode);
        }
    }
    // leave the destruct value on success
    unsetDestructValue(insideRef);
    if(prevNode) unsetDestructValue(prevNode);
    unsetDestructValue(nextNode);
    return;
}

bool releaseInsideReference(InsideReference* insideRef) {
    if(IsInsideRefCorrupt(insideRef)) return false;

    //printf("releasing inside reference %p, has %llu refs\n", insideRef, insideRef->count);

    DebugLog(file, line, "releaseInsideReference (BEFORE)", insideRef);

    // Will we ever race to go from 1 to 0 while something else is trying to redirect us?
    //  - No, because if we have 1, then we are already unique, and won't be redirected
    // So, if we see we have been redirected, we will always be redirected, and so
    //  we just have to look for the 3 to 2 (which means if we get the destruct state,
    //  we are then unique).

    
    while(true) {
        InsideReferenceCountDebug count;
        count.countFullValue = insideRef->countFullValue;
        InsideReferenceCountDebug newCount = count;
        // If ever nextRedirectValue, and we see count == 2 and !count.destructStarted,
        //  it means there is definitely a prevRedirectedValue, and as insideRef is 1 ref,
        //  prevRedirectedValue is the other, so we can remove the predRedirectedValue,
        //  and know it won't come back.
        if(count.count == 2 && insideRef->nextRedirectValue.valueForSet) {
            // In this case, while count might increase because of our siblings moving around redirect values,
            //  we won't get any real new references, and we only need to worry about detachFromLinkList
            //  suffering from the classic linked list remove problem (A->B->C, remove B, but C was removed, so setting A.next = C is invalid)
            // Set destructStarted first
            detachFromLinkList(insideRef);
        }

        newCount.count--;
        if(newCount.outsideCount > newCount.count && newCount.outsideCount < 1024) {
            // A big problem, you released a reference you obtained for an outside reference without that outside reference,
            //  which leaks a reference and will cause leaks or double frees.
            OnError(3);
        }

        IsInsideRefCorrupt(insideRef);
        if(InterlockedCompareExchange64(
            (LONG64*)&insideRef->countFullValue,
            newCount.countFullValue,
            count.countFullValue
        ) != count.countFullValue) {
            continue;
        }

        if(newCount.count == 0) {
            //printf("freeing reference memory %p\n", insideRef);
            if(IsInsideRefCorruptInner(insideRef, true)) {
                return true;
            }
            if (HAS_NON_POINTER_PATTERN(insideRef->pool)) {
                // Double free
                breakpoint();
            }
            DestroyUniqueOutsideRef(&insideRef->nextRedirectValue);
            DestroyUniqueOutsideRef(&insideRef->prevRedirectValue);
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
}

InsideReference* Reference_Acquire(OutsideReference* pRef) {
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
            DebugLog(file, line, "Reference_Acquire (ACQUIRED)", ref);
            return ref;
        }
    }

    // Unreachable
    OnError(9);
    return nullptr;
}


InsideReference* Reference_AcquireIfNull(OutsideReference* pRef) {
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
    releaseInsideReference(ref);

    DebugLog(file, line, "Reference_AcquireIfNull interrupted (AFTER)", ref);

    IsInsideRefCorrupt(ref);

    return ref;
}
bool Reference_DestroyThroughNull(OutsideReference* outsideRef, OutsideReference newNullValue) {
    OutsideReference ref = *outsideRef;
    if(IsOutsideRefCorrupt(ref)) return false;
    if(ref.valueForSet == newNullValue.valueForSet) {
        return false;
    }
    if(!ref.isNull) {
        return false;
    }

    InsideReference* insideRef = Reference_AcquireIfNull(outsideRef);
    if(!insideRef) return false;
    DebugLog(__FILE__, __LINE__, "DestroyThroughNull (BEFORE)", insideRef);
    bool destroyed = Reference_ReplaceOutsideInner(outsideRef, insideRef, newNullValue, true, true);
    IsInsideRefCorrupt(insideRef);
    if(destroyed) {
        //insideRef->file = "replaced";
    } else {
        //insideRef->file = "failed to replace";
    }
	Reference_Release(&emptyReference, insideRef);
    return destroyed;
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

    *outPointer = (void*)((byte*)ref + InsideReferenceSize);
}

#ifdef DEBUG
bool IsSingleThreadedTest = false;
#endif

void Reference_Release(OutsideReference* outsideRef, InsideReference* insideRef) {
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
    releaseInsideReference(insideRef);
}

bool Reference_ReplaceOutsideInner(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef, bool notSetIsNullAndReduceToZeroRefs, bool onNulls) {
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
            
            if(releaseInsideReference(pInsideRef)) {
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
bool Reference_DestroyOutsideMakeNull1(OutsideReference* pOutsideRef, InsideReference* pInsideRef) {
    return Reference_ReplaceOutside(pOutsideRef, pInsideRef, BASE_NULL1);
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

    pInsideRef->file = file;
    pInsideRef->line = line;

    return newOutsideRef;
}