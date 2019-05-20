#pragma once

#include "environment.h"
#include "MemPool.h"

#ifdef __cplusplus
extern "C" {
#endif

// Basically atomic_shared_ptr, and the implementation is basically the best implementation of
//  atomic_shared_ptr (the only reasonable one). Although some 
// https://github.com/facebook/folly/blob/master/folly/concurrency/AtomicSharedPtr.h

// Outside and inside references. The inside reference is in the actual memory we are keeping track of.
//  The outside reference itself holds a reference to the inside reference, and to gain a reference to the inside
//  reference you just have to add to the outside reference count. Then when an outside reference is removed
//  from it's original storage location we move its references to the inside reference.


// We use the fact that 64 processors only use some bits in the 64 bits to store the reference counts
//  in the extra data. This allows us to do 64 bit compare and swaps, which eliminates all need for
//  128 bit (16 byte) alignment, and should be a lot faster, as on my machine
//  - Furthermore, using half the bytes allows us to store double the data before blowing a cache,
//      which should give us a huge performance advantage.
//  - There is a cost to having to modify a pointer before using it though, but I don't think the extra
//      few operations should matter compared to the cost of a 128 CAS vs a 64 CAS (which surely is at
//      least one operation more).
//      (My benchmarks showed 7ns for a 64 bit CAS, and 10ns for a 128 bit CAS, while messing with the pointer
//          took a dereference from 0.9ns to 1.8ns.)


// TODO:
//  - Change the OuterReference to move its references to the InsideReference inside of rolling over.
//      - ALTHOUGH! Trying to move the bottom bits from the outside reference to the inside reference is actually quite dangerous.
//          While we are doing it we don't have any references to the inside reference, but we have to go into its memory to
//          increment inside references, and during this time, we could be freed, and then crash... So... maybe don't use the bottom bits...


#define BITS_IN_ADDRESS_SPACE 48


typedef struct InsideReference InsideReference;
//#define InsideReferenceSize 32
//#define EVENT_ID_COUNT 1

#ifdef DEBUG_INSIDE_REFERENCES
#define InsideReferenceSize (sizeof(InsideReference))
#else
#define InsideReferenceSize (32)
#endif

#ifdef DEBUG
extern bool IsSingleThreadedTest;
#endif


// Creates an expression to fast check a reference and a pointer. May fail due to redirect though,
//  so this should only be used in retry loops that call Reference_Acquire and Reference_GetValue
//  on the value anyway.
// Also, because it uses an expression, outsideRef obviously can't be a shared value...
// Also, doesn't compare against nullptr InsideReferences...

#define COUNT_OFFSET_BITS (BITS_IN_ADDRESS_SPACE)

#if defined(WINDOWS) && defined(KERNEL)
#define FAST_CHECK_POINTER(outsideRef, insideRef) (!(outsideRef).isNull ? (outsideRef).pointerClipped == ((uint64_t)(insideRef) & ((1ll << BITS_IN_ADDRESS_SPACE) - 1)) : (uint64_t)(insideRef) == nullptr)
#else
#define FAST_CHECK_POINTER(outsideRef, insideRef) (!(outsideRef).isNull ? (outsideRef).pointerClipped == ((uint64_t)(insideRef)) : (uint64_t)(insideRef) == nullptr)
#endif

#define GET_POINTER_CLIPPED(val) (val & ((1ull << BITS_IN_ADDRESS_SPACE) - 1))
#define GET_NULL(val) (val & (1ull << 63))
#define IS_FROZEN_POINTER(ref) (ref.isNull && ref.pointerClipped)

#define IS_VALUE_MOVED(value) (value.isNull && value.pointerAndCount)

#define IS_BLOCK_END(value) (value.valueForSet == 0 || value.valueForSet == BASE_NULL2.valueForSet)

// OutsideReferences can have a count of 0, the count simply refers to the number of references not yet moved
//  to the InsideReference. We have to sort of shuffle references to the InsideReference, erroring on the side
//  of having a count in both the outside and inside reference, for fear of having no count and being
//  accessing the inside memory, which is dangerous!
// Every OutsideReference has a reference inside of InsideReference. OutsideReferences are shared... however... if you atomically
//  remove an OutsideReference, then you can claim it's reference to InsideReference as itself, and then
//  go decrement 
#pragma pack(push, 1)
typedef struct {
    union {
        struct {
            uint64_t pointerClipped : BITS_IN_ADDRESS_SPACE;
            uint64_t count : 64 - BITS_IN_ADDRESS_SPACE - 1;
            uint64_t isNull : 1;
        };
        struct {
            uint64_t spacerForFastCount : BITS_IN_ADDRESS_SPACE;
            // Presumably it is faster to set fastCount than count, in the very rare cases we are constructing an OutsideReference
            //  an explicitly setting the count (faster because setting count has to wipe out the high bit to not intersect with null,
            //  but most of the time we KNOW the count won't be that high, so we should be able to just set fastCount and not
            //  worry about the highest bit setting isNull to 1).
            uint64_t fastCount : 64 - BITS_IN_ADDRESS_SPACE;
        };
        struct {
            uint64_t pointerAndCount: 63;
        };
        uint64_t valueForSet;
    };
} OutsideReference;
#pragma pack(pop)
CASSERT(sizeof(OutsideReference) == sizeof(uint64_t));

#define BASE_NULL ((OutsideReference){ 0, 0, 1 })
#define BASE_NULL1 ((OutsideReference){ 0, 1, 1 })
#define BASE_NULL2 ((OutsideReference){ 0, 2, 1 })
#define BASE_NULL3 ((OutsideReference){ 0, 3, 1 })
#define BASE_NULL4 ((OutsideReference){ 0, 4, 1 })
#define BASE_NULL5 ((OutsideReference){ 0, 5, 1 })
#define BASE_NULL6 ((OutsideReference){ 0, 6, 1 })
#define BASE_NULL7 ((OutsideReference){ 0, 7, 1 })
#define BASE_NULL8 ((OutsideReference){ 0, 8, 1 })
#define BASE_NULL9 ((OutsideReference){ 0, 9, 1 })


void* Reference_GetValue(InsideReference* ref);

// Assumes ref isn't NULL
#define Reference_GetValueFast(ref) ((void*)((byte*)(ref) + InsideReferenceSize))

// True if it is moving, or has been moved (when it is moving it may be duplicated), or sometimes if it has
//  been deleted while moving (so this is only good enough to detect duplicates with some false positives, not to handle memory management)
bool Reference_HasBeenMoved(InsideReference* ref);

void Reference_Mark(InsideReference* ref);
bool Reference_IsMarked(InsideReference* ref);

// Marks it, if it is still allocated, and the unique id is equal to uniqueId
//  Does this atomically, and returns true if it did mark it.
//  Requires the ref to be never moved again after this is called (or to not match uniqueId)
bool Reference_MarkIfUniqueIdEqual(InsideReference* ref, uint64_t uniqueId);
uint64_t Reference_GetUniqueValueId(InsideReference* ref);

// Ugh... has to be done somewhere, only to be done if ref is non-shared, which begs the question of why it is
//  being cloned, but it happens once...
InsideReference* Reference_unsafeClone(InsideReference* ref);

// Of course the memory OutsideReference is stored in must be guaranteed to exist, and the reuse of the memory for other
//  OutsideReferences is allowed, but you better not store arbitrary other memory there.

// (May set outPointer to nullptr if the allocation fails)
//void Reference_Allocate(uint64_t size, OutsideReference* outRef, void** outPointer);
void Reference_Allocate(MemPool* pool, OutsideReference* outRef, void** outPointer, uint64_t size, uint64_t hash);

uint64_t Reference_GetHash(InsideReference* ref);


//InsideReference* Reference_AcquireCheckIsMoved(OutsideReference* ref, bool* outIsMoved, bool* outIsFrozen);
#define Reference_AcquireCheckIsMoved(ref, outIsMoved, outIsFrozen)  Reference_AcquireCheckIsMovedProof(ref, outIsMoved, outIsFrozen, nullptr)
// Returns a reference that must be freed with Reference_ReleaseOutsidesInside
InsideReference* Reference_AcquireCheckIsMovedProof(OutsideReference* ref, bool* outIsMoved, bool* outIsFrozen, OutsideReference* refSeen);

// Freezes ref, so future Reference_AcquireCheckIsMoved calls will return nullptr, but true for outIsMoved
//  (and so future Reference_Acquire calls will throw, as anything that is moved shouldn't be called with Reference_Acquire),
//  and returns a value that is allocated in newPool, and suitable for Reference_CopyIntoZero.
// (and of course, returns nullptr if ref didn't have a value to begin with, but still freezes it).
// - Sets isCommittedToMove inside the newRef, as this is the only way a frozen outside
//      reference can be destroyed (or should be), and so isCommittedToMove should matter before
//      this, but after this it may be destructed, so knowing if it moved is important.
// Reference must be freed with Reference_ReleaseOutsidesInside
int Reference_AcquireStartMove(
    OutsideReference* ref,
    MemPool* newPool,
    uint64_t size,
    InsideReference** outNewRef
#ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
,uint64_t* pOutstandingRefsDuringMove
,uint64_t* pNotOutstandingRefsDuringMove
#endif
);

// Releases the inside ref an outside ref was holding (basically Reference_Release(&emptyReference, ref), but with
//  some extra changes that make debugging easier, and that are now actually used for how we handle the delete callback)
//  (returns true if it was the final reference)
bool Reference_ReleaseOutsidesInside(InsideReference* ref);

// Called after Reference_AcquireStartMove and Reference_CopyIntoZero on the original ref,
//  destroying it (returning the InsideReference* of the reference that was destroyed, or nullptr
//  if there was no reference to destroy).
// Returns true if it destroyed an outside reference.
bool Reference_FinishMove(OutsideReference* outsideRef, InsideReference* ref);



// pointer should be directly used inside of InsideReference. The only reason a raw pointer isn't returned is
//  to ensure that random pointers aren't passed back.

// MAY change ref to be for a redirected value, as is required to allow proper destruction of an outside reference
//  otherwise when you tried to acquire and then destroy an outside reference it wouldn't work, as the pointers wouldn't match...
// If the reference has been freed (or was never initialized), we return nullptr (but if we return an InsideReference,
//  its pointer is always valid, so does not need to be null checked).
//__forceinline InsideReference* Reference_Acquire(OutsideReference* ref);

InsideReference* Reference_Acquire(OutsideReference* pRef);

// pRef must be guaranteed to have a value, as we might mess up null values, or create bad pointers
__forceinline InsideReference* Reference_AcquireFast(OutsideReference* pRef) {
    // Assume there were no refs before
    OutsideReference prevRef = *pRef;
    prevRef.fastCount = 0;
    OutsideReference newRef = prevRef;
    newRef.fastCount = 1;
    
    if(InterlockedCompareExchange64(
        (LONG64*)pRef,
        newRef.valueForSet,
        prevRef.valueForSet
    ) == prevRef.valueForSet) {
        return (InsideReference*)newRef.pointerClipped;
    }

    return Reference_Acquire(pRef);
}

InsideReference* Reference_AcquireInside(OutsideReference* pRef);




// Must be passed if a Reference_Release call is freeing a reference count it knows has been moved to the inside reference.
extern const OutsideReference emptyReference;

// Outside reference may be knowingly wiped out, we will just ignore it and then release the inside reference.
//  Always pass an outsideRef, even if you know it has been wiped out.
#define Reference_Release(outsideRef, insideRef) Reference_ReleaseX(outsideRef, insideRef, __FILE__, __LINE__)
void Reference_ReleaseX(OutsideReference* outsideRef, InsideReference* insideRef, const char* file, uint64_t line);

// InsideRef must not be null
__forceinline void Reference_ReleaseFast(OutsideReference* outsideRef, InsideReference* insideRef) {
    // Assume we had 1 outside ref. If not, just bail to Reference_Release (but most of the time
    //  we have 1 outside ref, so this is good)
    OutsideReference prevRef;
    prevRef.pointerClipped = (uint64_t)insideRef;
    prevRef.fastCount = 1;
    OutsideReference newRef;
	newRef.valueForSet = 0;
	newRef.pointerClipped = (uint64_t)insideRef;
    
    if(InterlockedCompareExchange64(
        (LONG64*)outsideRef,
        newRef.valueForSet,
        prevRef.valueForSet
    ) == prevRef.valueForSet) {
        return;
    }

    Reference_Release(outsideRef, insideRef);
}


// Destroys this outside ref to the inside ref (which if it is the last outside ref, and there are no more inside references, will result
//  in the inside ref being freed).
// Must have an inside reference to be called, but does not free inside reference
//  returns true on success
bool Reference_DestroyOutsideX(OutsideReference* outsideRef, InsideReference* insideRef, const char* file, uint64_t line);
#define Reference_DestroyOutside(outsideRef, insideRef) Reference_DestroyOutsideX(outsideRef, insideRef, __FILE__, __LINE__)

// Makes the outsideRef BASE_NULL after it destroys it, instead of 0
//  returns true on success
bool Reference_DestroyOutsideMakeNull(OutsideReference* outsideRef, InsideReference* insideRef);

bool Reference_DestroyOutsideMakeNull1(OutsideReference* pOutsideRef, InsideReference* pInsideRef);

//  returns true on success
bool Reference_ReplaceOutsideX(OutsideReference* pOutsideRef, InsideReference* pInsideRef, OutsideReference newOutsideRef, const char* file, uint64_t line);
#define Reference_ReplaceOutside(pOutsideRef, pInsideRef, newOutsideRef) Reference_ReplaceOutsideX(pOutsideRef, pInsideRef, newOutsideRef, __FILE__, __LINE__)

// We need this in one specific place... It is used when moving to basically freeze the reference,
//  and then access it by calling special functions which allow us to treat isNull values like regular references.
bool Reference_ReduceToZeroRefsAndSetIsNull(OutsideReference* pOutsideRef, InsideReference* pInsideRef);

#define Reference_CreateOutsideReference(insideRef) Reference_CreateOutsideReferenceX(insideRef, __FILE__, __LINE__)
OutsideReference Reference_CreateOutsideReferenceX(InsideReference* insideRef, const char* file, uint64_t line);

void DestroyUniqueOutsideRef(OutsideReference* ref);

// Must be passed a reference obtained as an inside reference
// Frees the argument reference, and always returns a reference (so you only need to free the result).
InsideReference* Reference_FollowRedirects(InsideReference* ref);

#ifdef DEBUG
#define IsInsideRefCorrupt(pRef) IsInsideRefCorruptInner(pRef, false)
#define IsOutsideRefCorrupt(ref) IsOutsideRefCorruptInner(ref)
#else
#define IsInsideRefCorrupt(pRef) false
#define IsOutsideRefCorrupt(ref) false
#endif

bool IsInsideRefCorruptInner(InsideReference* pRef, bool allowFreed);
bool IsOutsideRefCorruptInner(OutsideReference ref);


#ifdef DEBUG_INSIDE_REFERENCES
void DebugLog2(const char* operation, InsideReference* ref, const char* file, uint64_t line);
#else
#define DebugLog2(operation, ref, file, line) false
#endif

#pragma pack(push, 1)
typedef struct {
    union {
        struct {
            // Persists across
            uint64_t uniqueValueId: 61;
            uint64_t hasUniqueValueId: 1;
            uint64_t isCommittedToMove: 1;
            // Must be set to true, allowing outside references and unique values to be distinguished...
            uint64_t isNull: 1;
        };
        OutsideReference tempOldRef;
        uint64_t valueForSet;
    };
} MoveId;
#pragma pack(pop)
CASSERT(sizeof(MoveId) == 8);

#pragma pack(push, 1)
typedef struct { union {
    struct {
        uint64_t count: 40;
        uint64_t outsideCount: 20;
        uint64_t isMarked: 1;
    };
    uint64_t valueForSet;
}; } InsideReferenceCount;
#pragma pack(pop)
CASSERT(sizeof(InsideReferenceCount) == 8);
#pragma pack(push, 1)
typedef struct {
	InsideReferenceCount state;
	const char* operation;
	uint64_t line;
	const char* file;
} DebugLineInfo;
#pragma pack(pop)


// struct InsideReference really be inside RefCount.c, so it is hidden, but for debugging it was a lot easier to bring it out...

// When InsideReference reaches a count of 0, it should be freed (as this means nothing knows about it,
//  and so if we don't free it now it will leak).
#pragma pack(push, 1)
typedef struct InsideReference {
    union {
        struct {
            uint64_t count: 40;
            uint64_t outsideCount: 20;
            uint64_t isMarked: 1;
        };
        uint64_t valueForSet;
    };
    
    // Starts at 0, is used to store the old ref as an outside reference, then gains a unique value, possibly with hasUniqueValueId
    //  set to true, but definitely with isNull set to true.
    MoveId moveId;

    MemPool* pool;

    uint64_t hash;


    #ifdef DEBUG_INSIDE_REFERENCES
    const char* file;
    uint64_t line;

    #define EVENT_ID_COUNT 100
    uint64_t nextEventIndex;
    DebugLineInfo events[EVENT_ID_COUNT];

    InsideReference* unsafeSource;
    #endif

} InsideReference;
#pragma pack(pop)


#ifdef __cplusplus
}
#endif