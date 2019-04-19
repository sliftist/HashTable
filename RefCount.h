#pragma once

#include "environment.h"

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

#pragma pack(push, 1)
typedef struct {
    uint64_t pointerClipped : BITS_IN_ADDRESS_SPACE;
	uint64_t value : 64 - BITS_IN_ADDRESS_SPACE;
} PackedPointer;
#pragma pack(pop)

// If the highest bit in the address space is set, then we have to set all the high bits. Otherwise bit fields does the rest for us.
//  (and all other fields can just be get/set via the bitfield)
#define PACKED_POINTER_GET_POINTER(p) (((p).pointerClipped & (1ull << (BITS_IN_ADDRESS_SPACE - 1))) ? ((p).pointerClipped | 0xFFFF000000000000ull) : (p).pointerClipped)


// When InsideReference reaches a count of 0, it should be freed (as this means nothing knows about it,
//  and so if we don't free it now it will leak).
#pragma pack(push, 1)
typedef struct {
    void* pointer;
	uint64_t count;
} InsideReference;
#pragma pack(pop)


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
            uint64_t count : 64 - BITS_IN_ADDRESS_SPACE;
        };
        uint64_t value;
    };
} OutsideReference;
#pragma pack(pop)
CASSERT(sizeof(OutsideReference) == sizeof(uint64_t));


// Of course the memory OutsideReference is stored in must be guaranteed to exist, however reuse of the memory for other
//  OutsideReferences is allowed (however you better not store arbitrary memory there).

// (May set outPointer to nullptr if the allocation fails)
void Reference_Allocate(uint64_t size, OutsideReference* outRef, void** outPointer);

// The emptyAllocation must be size + sizeof(InsideReference)
void Reference_RecycleAllocate(void* emptyAllocation, uint64_t size, OutsideReference* outRef, void** outPointer);


// pointer should be directly used inside of InsideReference. The only reason a raw pointer isn't returned is
//  to ensure that random pointers aren't passed back.


// If the reference has been freed (or was never initialized), we return nullptr (but if we return an InsideReference,
//  its pointer is always valid, so does not need to be null checked).
InsideReference* Reference_Acquire(OutsideReference* ref);

// Outside reference may be knowingly wiped out, we will just ignore it and then
//  release the inside reference.
// If dontFree, and this is the last reference, then we don't free it.
// Returns true if this was the last reference (so if dontFree is passed, it is up to the caller
//  to call free on insideRef).
bool Reference_Release(OutsideReference* outsideRef, InsideReference* insideRef, bool dontFree);



// Destroys this outside ref to the inside ref (which if it is the last outside ref, and there are no more inside references, will result
//  in the inside ref being freed).
// Must have an inside reference to be called, but does not free inside reference
//  returns true on success
bool Reference_DestroyOutside(OutsideReference* outsideRef, InsideReference* insideRef);


// dest must be zeroed out, OR previously an outside ref destroyed by DestroyReference
// Must have an inside reference to be called, and does not free inside reference
//  returns true on success
bool Reference_SetOutside(OutsideReference* outsideRef, InsideReference* insideRef);


#ifdef __cplusplus
}
#endif